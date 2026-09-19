#include "memtable.h"
#include "db_format.h"
#include "coding.h"
#include "comparator.h"

#include <cassert>
#include <cstring>

namespace minikv {

// 从 Arena 分配的节点首地址读取长度前缀，并返回其后紧跟的数据 Slice。
// 节点内存布局：
// [varint32: internal_key_size]
// [internal_key]
// [varint32: val_size]
// [val]
static const char* GetVarint32Ptr(
    const char* p,
    const char* limit,
    uint32_t* value) {

    [[maybe_unused]] bool ok =
        GetVarint32(&p, limit, value);

    assert(ok);
    return p;
}

static Slice GetLengthPrefixedSlice(const char* data) {
    uint32_t len;

    const char* p =
        GetVarint32Ptr(data, data + 5, &len);

    return Slice(p, len);
}

// MemTableComparator 委托给 InternalKeyComparator。
// InternalKeyComparator 的语义：
// UserKey 升序，序列号降序（新版本排在前面）。
int MemTableComparator::operator()(
    const char* a,
    const char* b) const {

    Slice a_slice =
        GetLengthPrefixedSlice(a);

    Slice b_slice =
        GetLengthPrefixedSlice(b);

    return comparator->Compare(
        a_slice,
        b_slice);
}

// ---------------------------------------------------------------------------
// MemTableIterator
// ---------------------------------------------------------------------------

// 将 SkipList 迭代器适配为通用 Iterator 接口。
class MemTableIterator : public Iterator {
public:
    explicit MemTableIterator(MemTable::Table* table)
        : iter_(table) {
    }

    bool Valid() const override {
        return iter_.Valid();
    }

    void Seek(const Slice& k) override {
        iter_.Seek(k.data());
    }

    void SeekToFirst() override {
        iter_.SeekToFirst();
    }

    void SeekToLast() override {
        iter_.SeekToLast();
    }

    void Next() override {
        iter_.Next();
    }

    void Prev() override {
        iter_.Prev();
    }

    Status status() const override {
        return Status::OK();
    }

    // key() 返回包含序列号的 InternalKey Slice。
    // 零拷贝，直接指向 Arena 内存。
    Slice key() const override {
        return GetLengthPrefixedSlice(iter_.key());
    }

    // value() 紧跟在 InternalKey 之后，
    // 同样通过长度前缀定位，零拷贝。
    Slice value() const override {
        Slice key_slice =
            GetLengthPrefixedSlice(iter_.key());

        const char* value_ptr =
            key_slice.data() + key_slice.size();

        return GetLengthPrefixedSlice(value_ptr);
    }

private:
    MemTable::Table::Iterator iter_;
};

// ---------------------------------------------------------------------------
// MemTable
// ---------------------------------------------------------------------------

MemTable::MemTable(const Comparator* comparator)
    : comparator_(comparator),
      arena_(),
      table_(comparator_, &arena_) {
}

size_t MemTable::ApproximateMemoryUsage() const {
    return arena_.MemoryUsage();
}

// Add 将一条操作编码后插入跳表。
//
// 节点格式：
// [varint32 internal_key_size]
// [UserKey]
// [seq<<8|type: 8B]
// [varint32 val_size]
// [Value]
//
// InternalKey 包含 UserKey 和 8 字节的 (seq, type) 尾缀，
// 是 SkipList 排序的基准。
void MemTable::Add(
    uint64_t seq,
    char type,
    const Slice& key,
    const Slice& value) {

    const size_t key_size = key.size();
    const size_t val_size = value.size();

    const size_t internal_key_size =
        key_size + 8;

    const size_t encoded_len =
        VarintLength(internal_key_size) +
        internal_key_size +
        VarintLength(val_size) +
        val_size;

    char* buf =
        arena_.AllocateAligned(encoded_len);

    char* p = buf;

    // 直接将 varint 编码写入目标 buffer，
    // 避免经由临时 std::string 中转。
    p = EncodeVarint32(
        p,
        static_cast<uint32_t>(internal_key_size));

    memcpy(
        p,
        key.data(),
        key_size);

    p += key_size;

    // 写入：
    // 高 56 位 = sequence
    // 低 8 位 = type
    const uint64_t packed =
        (seq << 8) |
        static_cast<uint8_t>(type);

    EncodeFixed64(
        p,
        packed);

    p += 8;

    p = EncodeVarint32(
        p,
        static_cast<uint32_t>(val_size));

    memcpy(
        p,
        value.data(),
        val_size);

    table_.Insert(buf);
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------
//
// 查找逻辑：
//
// 1. 构造一个 sequence 为最大值的 LookupKey；
// 2. SkipList::Seek() 找到该 UserKey 的最新版本；
// 3. 检查 UserKey；
// 4. 根据 type 判断：
//      Value    -> kFound
//      Deletion -> kDeleted
//      不存在   -> kNotFound
//
MemTableLookupResult MemTable::Get(
    const Slice& key,
    std::string* value) {

    const size_t key_size = key.size();

    const size_t internal_key_size =
        key_size + 8;

    // LookupKey：
    // [varint32 internal_key_size]
    // [UserKey]
    // [sequence/type]
    //
    // 使用最大 sequence，
    // 保证 Seek() 优先命中该 UserKey 最新版本。
    std::string lookup_buf;

    PutVarint32(
        &lookup_buf,
        static_cast<uint32_t>(internal_key_size));

    lookup_buf.append(
        key.data(),
        key_size);

    const uint64_t packed =
        (0xFFFFFFFFFFFFFFull << 8) |
        0x01;

    PutFixed64(
        &lookup_buf,
        packed);

    Table::Iterator iter(&table_);

    iter.Seek(lookup_buf.data());

    if (!iter.Valid()) {
        return MemTableLookupResult::kNotFound;
    }

    const char* entry = iter.key();

    uint32_t key_length = 0;

    const char* key_ptr =
        GetVarint32Ptr(
            entry,
            entry + 5,
            &key_length);

    if (key_length < 8) {
        return MemTableLookupResult::kNotFound;
    }

    // key_ptr 指向完整 InternalKey。
    // 当前只比较 UserKey，所以必须使用真正的 UserKey comparator。
    //
    // comparator_.comparator 的静态类型是 Comparator*，
    // 实际对象由 DBImpl 创建为 InternalKeyComparator。
    const auto* internal_comparator =
        static_cast<const InternalKeyComparator*>(
            comparator_.comparator);

    const Comparator* user_comparator =
        internal_comparator->user_comparator();

    Slice user_key(
        key_ptr,
        key_length - 8);

    if (user_comparator->Compare(
            user_key,
            key) != 0) {

        return MemTableLookupResult::kNotFound;
    }

    // InternalKey 最后 8 字节：
    // [sequence: 56 bits][type: 8 bits]
    const uint64_t tag =
        DecodeFixed64(
            key_ptr + key_length - 8);

    switch (tag & 0xff) {
        case kTypeValue: {
            Slice v =
                GetLengthPrefixedSlice(
                    key_ptr + key_length);

            if (value != nullptr) {
                value->assign(
                    v.data(),
                    v.size());
            }

            return MemTableLookupResult::kFound;
        }

        case kTypeDeletion:
            // 找到最新版本的删除墓碑。
            //
            // 非常重要：
            // 上层不能再继续向 imm_ / SSTable 查询，
            // 否则可能重新读到该 key 的旧版本。
            return MemTableLookupResult::kDeleted;

        default:
            return MemTableLookupResult::kNotFound;
    }
}

Iterator* MemTable::NewIterator() {
    return new MemTableIterator(&table_);
}

} // namespace minikv

