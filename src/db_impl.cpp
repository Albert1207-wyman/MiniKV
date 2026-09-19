#include <iostream>
#include <thread>
#include <chrono>
#include <algorithm>
#include <cstdio>
#include <cassert>
#include <filesystem>
#include <set>
#include "db_impl.h"
#include "write_batch.h"
#include "coding.h"
#include "table_builder.h"
#include "table_cache.h"
#include "memtable.h"
#include "comparator.h"
#include "version_set.h"
#include "version_edit.h"
#include "log_reader.h"
#include "merging_iterator.h"

namespace minikv {

DBImpl::DBImpl(
    const Options& options,
    const std::string& dbname)
    : options_(options),
      dbname_(dbname),
      shutting_down_(false),
      bg_compaction_scheduled_(false),
      last_sequence_(0),
      imm_(nullptr),
      logfile_(nullptr),
      log_(nullptr),
      internal_comparator_(options.comparator) {

    // InternalKeyComparator 封装了用户比较器，
    // 额外处理序列号降序。
    //
    // 所有内部组件统一使用 internal_options，
    // 避免用户比较器泄漏到磁盘格式层。
    Options internal_options = options_;
    internal_options.comparator =
        &internal_comparator_;

    table_cache_ =
        new TableCache(
            dbname_,
            internal_options,
            1000);

    versions_ =
        new VersionSet(
            dbname_,
            &internal_options,
            table_cache_,
            &internal_comparator_);

    // 从 MANIFEST 恢复版本集合。
    // 首次建库时 CURRENT 文件不存在，
    // NotFound 属于正常情况。
    bool save_manifest = false;

    Status s =
        versions_->Recover(
            &save_manifest);

    if (s.IsNotFound()) {
        s = Status::OK();
    } else if (!s.ok()) {
        std::cerr
            << "[FATAL] Failed to recover VersionSet: "
            << s.ToString()
            << "\n";
        abort();
    }

    mem_ =
        new MemTable(
            &internal_comparator_);

    // WAL 回放：
    // 找出所有编号 >= versions_->log_number()
    // 的遗留 .log 文件并按序重放。
    if (s.ok()) {
        std::vector<uint64_t> logs_to_replay;

        for (const auto& entry :
             std::filesystem::directory_iterator(dbname_)) {

            if (entry.path().extension() == ".log") {
                uint64_t log_num =
                    std::stoull(
                        entry.path().stem().string());

                if (log_num >=
                    versions_->log_number()) {

                    logs_to_replay.push_back(
                        log_num);
                }
            }
        }

        std::sort(
            logs_to_replay.begin(),
            logs_to_replay.end());

        for (uint64_t log_num :
             logs_to_replay) {

            char buf[100];

            std::snprintf(
                buf,
                sizeof(buf),
                "%s/%06llu.log",
                dbname_.c_str(),
                (unsigned long long)log_num);

            SequentialFile* log_file = nullptr;

            s = NewSequentialFile(
                buf,
                &log_file);

            if (!s.ok()) {
                continue;
            }

            log::Reader reader(
                log_file,
                nullptr,
                /*checksum=*/true,
                0);

            Slice record;
            std::string scratch;

            // 每条 WAL record：
            // [seq: 8B][count: 4B][record...]
            while (reader.ReadRecord(
                &record,
                &scratch)) {

                if (record.size() < 12) {
                    continue;
                }

                uint64_t seq =
                    DecodeFixed64(
                        record.data());

                uint32_t count =
                    DecodeFixed32(
                        record.data() + 8);

                const char* p =
                    record.data() + 12;

                const char* limit =
                    record.data() + record.size();

                for (uint32_t i = 0;
                     i < count;
                     i++) {

                    if (p >= limit) {
                        break;
                    }

                    char type = *p++;

                    uint32_t key_len;

                    if (!GetVarint32(
                            &p,
                            limit,
                            &key_len) ||
                        p + key_len > limit) {
                        break;
                    }

                    Slice key(
                        p,
                        key_len);

                    p += key_len;

                    if (type == kTypeValue) {
                        uint32_t val_len;

                        if (!GetVarint32(
                                &p,
                                limit,
                                &val_len) ||
                            p + val_len > limit) {
                            break;
                        }

                        Slice value(
                            p,
                            val_len);

                        p += val_len;

                        mem_->Add(
                            seq,
                            type,
                            key,
                            value);
                    } else {
                        // kTypeDeletion 无 value 字段。
                        mem_->Add(
                            seq,
                            type,
                            key,
                            Slice());
                    }

                    seq++;
                }

                // 回放后推进全局序列号，
                // 防止后续写入序列号倒退。
                last_sequence_ =
                    seq - 1;
            }

            delete log_file;
        }
    }

    // 为本次进程分配新的 WAL。
    // 并将编号写入 MANIFEST。
    if (s.ok()) {
        uint64_t new_log_number =
            versions_->NextFileNumber();

        char buf[100];

        std::snprintf(
            buf,
            sizeof(buf),
            "%s/%06llu.log",
            dbname_.c_str(),
            (unsigned long long)new_log_number);

        s = NewWritableFile(
            buf,
            (WritableFile**)&logfile_);

        if (s.ok()) {
            log_ =
                new log::Writer(
                    (WritableFile*)logfile_);

            VersionEdit edit;

            edit.SetLogNumber(
                new_log_number);

            s =
                versions_->LogAndApply(
                    &edit);
        }
    }

    // 后台线程常驻，
    // 负责 immutable MemTable flush
    // 或 level compaction。
    bg_thread_ =
        std::thread(
            &DBImpl::BackgroundCall,
            this);

    // 恢复后如果目录已经满足 compaction 条件，
    // 主动触发后台处理。
    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        MaybeScheduleCompaction();
    }
}

DBImpl::~DBImpl() {

    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        shutting_down_ = true;
        bg_cv_.notify_all();
    }

    if (bg_thread_.joinable()) {
        bg_thread_.join();
    }

    delete versions_;
    delete table_cache_;
    delete mem_;

    if (imm_ != nullptr) {
        delete imm_;
    }

    delete log_;
    delete logfile_;

    for (Snapshot* s :
         snapshots_) {
        delete s;
    }
}

Status DBImpl::Put(
    const WriteOptions& options,
    const Slice& key,
    const Slice& value) {

    WriteBatch batch;

    batch.Put(
        key,
        value);

    return Write(
        options,
        &batch);
}

Status DBImpl::Delete(
    const WriteOptions& options,
    const Slice& key) {

    WriteBatch batch;

    batch.Delete(
        key);

    return Write(
        options,
        &batch);
}

// Write 是所有写操作的统一入口。
// 整批 WriteBatch 序列化为单条 WAL record 后原子写入。
Status DBImpl::Write(
    const WriteOptions& options,
    WriteBatch* updates) {

    std::unique_lock<std::mutex> lock(
        mutex_);

    Status s =
        MakeRoomForWrite(lock);

    if (!s.ok()) {
        return s;
    }

    uint64_t seq =
        last_sequence_ + 1;

    if (log_ != nullptr) {

        std::string batch_payload;

        batch_payload.reserve(
            updates->ApproximateSize() +
            1024);

        PutFixed64(
            &batch_payload,
            seq);

        PutFixed32(
            &batch_payload,
            static_cast<uint32_t>(
                updates->Records().size()));

        for (const auto& record :
             updates->Records()) {

            batch_payload.push_back(
                static_cast<char>(
                    record.type ==
                            BatchValueType::kTypeValue
                        ? kTypeValue
                        : kTypeDeletion));

            PutVarint32(
                &batch_payload,
                static_cast<uint32_t>(
                    record.key.size()));

            batch_payload.append(
                record.key.data(),
                record.key.size());

            if (record.type ==
                BatchValueType::kTypeValue) {

                PutVarint32(
                    &batch_payload,
                    static_cast<uint32_t>(
                        record.value.size()));

                batch_payload.append(
                    record.value.data(),
                    record.value.size());
            }
        }

        s =
            log_->AddRecord(
                Slice(batch_payload));

        if (s.ok() &&
            options.sync &&
            logfile_ != nullptr) {

            s =
                logfile_->Sync();
        }
    }

    // WAL 成功之后才写 MemTable。
    if (s.ok()) {

        for (const auto& record :
             updates->Records()) {

            uint32_t type =
                (record.type ==
                         BatchValueType::kTypeValue)
                    ? kTypeValue
                    : kTypeDeletion;

            mem_->Add(
                seq,
                type,
                record.key,
                record.value);

            seq++;
        }

        last_sequence_ =
            seq - 1;
    }

    return s;
}

Status DBImpl::MakeRoomForWrite(
    std::unique_lock<std::mutex>& lock) {

    while (true) {

        if (mem_->ApproximateMemoryUsage() <=
            options_.write_buffer_size) {

            return Status::OK();

        } else if (imm_ != nullptr) {

            // imm_ 尚未 flush，
            // 等待后台线程完成。
            bg_cv_.wait(lock);

        } else {

            // mem_ 已满，
            // imm_ 为空：执行 double-buffer 翻转。
            uint64_t new_log_number =
                versions_->NextFileNumber();

            char buf[100];

            std::snprintf(
                buf,
                sizeof(buf),
                "%s/%06llu.log",
                dbname_.c_str(),
                (unsigned long long)new_log_number);

            WritableFile* new_logfile =
                nullptr;

            Status s =
                NewWritableFile(
                    buf,
                    &new_logfile);

            if (!s.ok()) {
                return s;
            }

            delete log_;
            delete logfile_;

            logfile_ =
                new_logfile;

            log_ =
                new log::Writer(
                    logfile_);

            VersionEdit edit;

            edit.SetLogNumber(
                new_log_number);

            s =
                versions_->LogAndApply(
                    &edit);

            if (!s.ok()) {
                return s;
            }

            imm_ = mem_;

            mem_ =
                new MemTable(
                    &internal_comparator_);

            MaybeScheduleCompaction();
        }
    }
}

void DBImpl::MaybeScheduleCompaction() {

    // 调用方必须持有 mutex_。
    if (bg_compaction_scheduled_ ||
        shutting_down_) {

        return;
    }

    if (imm_ == nullptr) {

        if (options_.disable_auto_compaction ||
            !versions_->NeedsCompaction()) {

            return;
        }
    }

    bg_compaction_scheduled_ = true;

    bg_cv_.notify_all();
}

void DBImpl::BackgroundCall() {

    std::unique_lock<std::mutex> lock(
        mutex_);

    while (!shutting_down_) {

        while (!bg_compaction_scheduled_ &&
               !shutting_down_) {

            bg_cv_.wait(lock);
        }

        if (shutting_down_) {
            break;
        }

        bg_compaction_scheduled_ = false;

        BackgroundCompaction();

        if (imm_ != nullptr) {
            MaybeScheduleCompaction();
        }

        bg_cv_.notify_all();
    }
}

void DBImpl::BackgroundCompaction() {

    // 优先处理 Immutable MemTable flush。
    if (imm_ != nullptr) {

        Status s =
            CompactMemTable();

        if (s.ok()) {

            delete imm_;
            imm_ = nullptr;

            DeleteObsoleteFiles();

        } else {

            std::cerr
                << "[ERROR] MemTable flush failed: "
                << s.ToString()
                << "\n";

            std::this_thread::sleep_for(
                std::chrono::seconds(1));

            return;
        }
    }

    // 自动执行 level compaction。
    while (!options_.disable_auto_compaction &&
           versions_->NeedsCompaction()) {

        Compaction* c =
            versions_->PickCompaction();

        if (c == nullptr) {
            break;
        }

        Status s =
            RunCompaction(c);

        delete c;

        if (!s.ok()) {

            std::cerr
                << "[ERROR] Level compaction failed: "
                << s.ToString()
                << "\n";

            break;
        }

        DeleteObsoleteFiles();
    }
}

Status DBImpl::CompactMemTable() {

    uint64_t file_number =
        versions_->NextFileNumber();

    char buf[100];

    std::snprintf(
        buf,
        sizeof(buf),
        "%s/%06llu.sst",
        dbname_.c_str(),
        (unsigned long long)file_number);

    WritableFile* sst_file = nullptr;

    Status s =
        NewWritableFile(
            std::string(buf),
            &sst_file);

    if (!s.ok()) {
        return s;
    }

    Options builder_options = options_;
    builder_options.comparator =
        &internal_comparator_;

    TableBuilder* builder =
        new TableBuilder(
            builder_options,
            sst_file);

    std::string smallest_key;
    std::string largest_key;
    bool has_data = false;

    // 释放锁允许前台继续写。
    mutex_.unlock();

    Iterator* iter =
        imm_->NewIterator();

    for (iter->SeekToFirst();
         iter->Valid();
         iter->Next()) {

        Slice key =
            iter->key();

        if (!has_data) {

            smallest_key =
                key.ToString();

            has_data = true;
        }

        largest_key =
            key.ToString();

        builder->Add(
            key,
            iter->value());
    }

    delete iter;

    s =
        builder->Finish();

    uint64_t file_size =
        builder->FileSize();

    if (s.ok()) {
        s =
            sst_file->Sync();
    }

    delete builder;
    delete sst_file;

    mutex_.lock();

    if (s.ok() && has_data) {

        VersionEdit edit;

        edit.AddFile(
            0,
            file_number,
            file_size,
            Slice(smallest_key),
            Slice(largest_key));

        s =
            versions_->LogAndApply(
                &edit);
    }

    return s;
}

// --------------------------------------------------------------------------
// RunCompaction
// --------------------------------------------------------------------------

Status DBImpl::RunCompaction(
    Compaction* c) {

    const int src_level =
        c->input_level;

    const int dst_level =
        src_level + 1;

    std::vector<Iterator*> iters;

    ReadOptions ro;
    ro.fill_cache = false;

    for (int which = 0;
         which < 2;
         ++which) {

        for (const auto& f :
             c->inputs[which]) {

            iters.push_back(
                table_cache_->NewIterator(
                    ro,
                    f->number,
                    f->file_size));
        }
    }

    mutex_.unlock();

    const uint64_t oldest_snap =
        OldestSnapshotSequence();

    MergingIterator merge_iter(
        &internal_comparator_,
        std::move(iters));

    merge_iter.SeekToFirst();

    struct OutputFileInfo {
        uint64_t number;
        uint64_t file_size;
        std::string smallest;
        std::string largest;
    };

    std::vector<OutputFileInfo>
        output_files;

    WritableFile* out_file = nullptr;
    TableBuilder* builder = nullptr;

    uint64_t out_number = 0;

    std::string out_smallest;
    std::string out_largest;

    constexpr uint64_t
        kTargetFileSize =
            2 * 1024 * 1024;

    auto FinishOutputFile =
        [&]() -> Status {

        if (builder == nullptr) {
            return Status::OK();
        }

        Status s =
            builder->Finish();

        uint64_t fsize =
            builder->FileSize();

        delete builder;
        builder = nullptr;

        if (s.ok()) {
            s =
                out_file->Sync();
        }

        delete out_file;
        out_file = nullptr;

        if (s.ok()) {

            output_files.push_back(
                {
                    out_number,
                    fsize,
                    out_smallest,
                    out_largest
                });
        }

        return s;
    };

    Options builder_options =
        options_;

    builder_options.comparator =
        &internal_comparator_;

    std::string prev_user_key;
    bool has_prev = false;

    Status s;

    for (; merge_iter.Valid() && s.ok();
         merge_iter.Next()) {

        Slice ikey =
            merge_iter.key();

        if (ikey.size() < 8) {
            continue;
        }

        uint64_t packed =
            DecodeFixed64(
                ikey.data() +
                ikey.size() - 8);

        uint64_t seq =
            packed >> 8;

        uint8_t type =
            static_cast<uint8_t>(
                packed & 0xff);

        Slice ukey(
            ikey.data(),
            ikey.size() - 8);

        bool same_user_key =
            has_prev &&
            (ukey.compare(
                 Slice(prev_user_key)) == 0);

        if (same_user_key) {

            if (seq < oldest_snap) {
                continue;
            }
        }

        prev_user_key =
            ukey.ToString();

        has_prev = true;

        // 当前实现的简化 compaction 策略暂不对
        // deletion tombstone 做额外的最低层过滤。
        (void)type;

        if (builder == nullptr) {

            mutex_.lock();

            out_number =
                versions_->NextFileNumber();

            mutex_.unlock();

            char buf[100];

            std::snprintf(
                buf,
                sizeof(buf),
                "%s/%06llu.sst",
                dbname_.c_str(),
                (unsigned long long)out_number);

            s =
                NewWritableFile(
                    std::string(buf),
                    &out_file);

            if (!s.ok()) {
                break;
            }

            builder =
                new TableBuilder(
                    builder_options,
                    out_file);

            out_smallest =
                ikey.ToString();
        }

        builder->Add(
            ikey,
            merge_iter.value());

        out_largest =
            ikey.ToString();

        if (builder->FileSize() >=
            kTargetFileSize) {

            s =
                FinishOutputFile();
        }
    }

    if (s.ok()) {
        s =
            FinishOutputFile();
    }

    mutex_.lock();

    if (s.ok()) {

        VersionEdit& edit =
            c->edit;

        for (int which = 0;
             which < 2;
             ++which) {

            int level =
                (which == 0)
                    ? src_level
                    : dst_level;

            for (const auto& f :
                 c->inputs[which]) {

                edit.DeleteFile(
                    level,
                    f->number);
            }
        }

        for (const auto& fi :
             output_files) {

            edit.AddFile(
                dst_level,
                fi.number,
                fi.file_size,
                Slice(fi.smallest),
                Slice(fi.largest));
        }

        s =
            versions_->LogAndApply(
                &edit);
    }

    return s;
}

// --------------------------------------------------------------------------
// 辅助方法
// --------------------------------------------------------------------------

void DBImpl::DeleteObsoleteFiles() {

    std::set<uint64_t> live_files;

    std::shared_ptr<Version> cur =
        versions_->current();

    if (cur != nullptr) {

        for (int level = 0;
             level < kNumLevels;
             ++level) {

            for (const auto& f :
                 cur->files(level)) {

                live_files.insert(
                    f->number);
            }
        }
    }

    std::error_code ec;

    for (const auto& entry :
         std::filesystem::directory_iterator(
             dbname_,
             ec)) {

        const std::string ext =
            entry.path()
                .extension()
                .string();

        if (ext == ".sst") {

            uint64_t num =
                std::stoull(
                    entry.path()
                        .stem()
                        .string());

            if (live_files.find(num) ==
                live_files.end()) {

                std::filesystem::remove(
                    entry.path(),
                    ec);

                if (!ec) {

                    std::cout
                        << "[INFO] Removed obsolete SSTable: "
                        << entry.path()
                               .filename()
                               .string()
                        << "\n";
                }
            }

        } else if (ext == ".log") {

            uint64_t log_num =
                std::stoull(
                    entry.path()
                        .stem()
                        .string());

            if (log_num <
                versions_->log_number()) {

                std::filesystem::remove(
                    entry.path(),
                    ec);

                if (!ec) {

                    std::cout
                        << "[INFO] Removed obsolete WAL: "
                        << entry.path()
                               .filename()
                               .string()
                        << "\n";
                }
            }
        }
    }
}

uint64_t DBImpl::OldestSnapshotSequence() const {

    if (snapshots_.empty()) {
        return last_sequence_;
    }

    return snapshots_.front()->sequence();
}

// --------------------------------------------------------------------------
// Get
// --------------------------------------------------------------------------

Status DBImpl::Get(
    const ReadOptions& options,
    const Slice& key,
    std::string* value) {

    std::shared_ptr<Version>
        current_version;

    uint64_t snapshot_seq;

    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        // 先查活跃 MemTable。
        //
        // 三态结果：
        //   kFound    -> 直接返回；
        //   kDeleted  -> 直接 NotFound，
        //                禁止继续查旧版本；
        //   kNotFound -> 继续查 imm。
        MemTableLookupResult result =
            mem_->Get(
                key,
                value);

        if (result ==
            MemTableLookupResult::kFound) {

            return Status::OK();
        }

        if (result ==
            MemTableLookupResult::kDeleted) {

            return Status::NotFound(
                "Key deleted");
        }

        // 再查 Immutable MemTable。
        if (imm_ != nullptr) {

            result =
                imm_->Get(
                    key,
                    value);

            if (result ==
                MemTableLookupResult::kFound) {

                return Status::OK();
            }

            if (result ==
                MemTableLookupResult::kDeleted) {

                return Status::NotFound(
                    "Key deleted");
            }
        }

        current_version =
            versions_->current();

        snapshot_seq =
            (options.snapshot != nullptr)
                ? options.snapshot->sequence()
                : last_sequence_;
    }

    // 构造 LookupKey：
    // UserKey + (snapshot_seq << 8 | kTypeValue)。
    //
    // kTypeValue 作为类型上界，
    // 确保 Seek 命中同 key 的最新版本。
    std::string internal_key;

    internal_key.reserve(
        key.size() + 8);

    internal_key.append(
        key.data(),
        key.size());

    uint64_t packed =
        (snapshot_seq << 8) | 1;

    PutFixed64(
        &internal_key,
        packed);

    // 磁盘查询无需持锁。
    return current_version->Get(
        options,
        Slice(internal_key),
        value);
}

// --------------------------------------------------------------------------
// NewIterator
// --------------------------------------------------------------------------

Iterator* DBImpl::NewIterator(
    const ReadOptions& options) {

    std::vector<Iterator*> iters;

    std::shared_ptr<Version>
        current_version;

    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        iters.push_back(
            mem_->NewIterator());

        if (imm_ != nullptr) {
            iters.push_back(
                imm_->NewIterator());
        }

        current_version =
            versions_->current();
    }

    if (current_version != nullptr) {

        for (int level = 0;
             level < kNumLevels;
             ++level) {

            for (const auto& f :
                 current_version
                     ->files(level)) {

                iters.push_back(
                    table_cache_->NewIterator(
                        options,
                        f->number,
                        f->file_size));
            }
        }
    }

    return new MergingIterator(
        &internal_comparator_,
        std::move(iters));
}

// --------------------------------------------------------------------------
// Snapshot
// --------------------------------------------------------------------------

const Snapshot* DBImpl::GetSnapshot() {

    std::lock_guard<std::mutex> lock(
        mutex_);

    Snapshot* snap =
        new Snapshot(
            last_sequence_);

    auto it =
        snapshots_.begin();

    while (it != snapshots_.end() &&
           (*it)->sequence() <=
               snap->sequence()) {

        ++it;
    }

    snapshots_.insert(
        it,
        snap);

    return snap;
}

void DBImpl::ReleaseSnapshot(
    const Snapshot* snapshot) {

    std::lock_guard<std::mutex> lock(
        mutex_);

    for (auto it =
             snapshots_.begin();
         it != snapshots_.end();
         ++it) {

        if (*it == snapshot) {

            snapshots_.erase(it);
            break;
        }
    }

    delete snapshot;
}

void DBImpl::GetLevelFileStats(
    std::vector<uint64_t>* files_per_level,
    std::vector<uint64_t>* bytes_per_level) {

    if (files_per_level == nullptr ||
        bytes_per_level == nullptr) {

        return;
    }

    std::lock_guard<std::mutex> lock(
        mutex_);

    files_per_level->assign(
        kNumLevels,
        0);

    bytes_per_level->assign(
        kNumLevels,
        0);

    std::shared_ptr<Version> cur =
        versions_->current();

    if (cur == nullptr) {
        return;
    }

    for (int level = 0;
         level < kNumLevels;
         ++level) {

        const auto& files =
            cur->files(level);

        (*files_per_level)[level] =
            static_cast<uint64_t>(
                files.size());

        uint64_t bytes = 0;

        for (const auto& f :
             files) {

            bytes += f->file_size;
        }

        (*bytes_per_level)[level] =
            bytes;
    }
}

// --------------------------------------------------------------------------
// DB::Open
// --------------------------------------------------------------------------

Status DB::Open(
    const Options& options,
    const std::string& name,
    DB** dbptr) {

    *dbptr = nullptr;

    std::error_code ec;

    if (!std::filesystem::exists(name)) {

        if (!options.create_if_missing) {

            return Status::NotFound(
                "DB directory does not exist: " +
                name);
        }

        if (!std::filesystem::create_directories(
                name,
                ec)) {

            return Status::IOError(
                "Failed to create DB directory: " +
                name);
        }
    }

    DBImpl* impl =
        new DBImpl(
            options,
            name);

    *dbptr = impl;

    return Status::OK();
}

} // namespace minikv
