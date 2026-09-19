#pragma once

#include <cstdint>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "slice.h"
#include "status.h"

namespace minikv {

struct FileMetaData {
    int refs;
    int allowed_seeks;

    uint64_t number;
    uint64_t file_size;

    std::string smallest;
    std::string largest;

    FileMetaData()
        : refs(0),
          allowed_seeks(1 << 30),
          number(0),
          file_size(0) {
    }
};

class VersionEdit {
public:
    // VersionEdit 中各种字段在 MANIFEST 中对应的编码 tag。
    enum Tag {
        kLogNumber = 1,
        kPrevLogNumber = 2,
        kNextFileNumber = 3,
        kLastSequence = 4,
        kCompactPointer = 5,
        kDeletedFile = 6,
        kNewFile = 7
    };

    VersionEdit()
        : has_log_number_(false),
          has_prev_log_number_(false),
          has_next_file_number_(false),
          has_last_sequence_(false),
          log_number_(0),
          prev_log_number_(0),
          next_file_number_(0),
          last_sequence_(0) {
    }

    void Clear();

    void SetLogNumber(uint64_t num) {
        has_log_number_ = true;
        log_number_ = num;
    }

    void SetPrevLogNumber(uint64_t num) {
        has_prev_log_number_ = true;
        prev_log_number_ = num;
    }

    void SetNextFile(uint64_t num) {
        has_next_file_number_ = true;
        next_file_number_ = num;
    }

    void SetLastSequence(uint64_t seq) {
        has_last_sequence_ = true;
        last_sequence_ = seq;
    }

    void AddFile(
        int level,
        uint64_t file,
        uint64_t file_size,
        const Slice& smallest,
        const Slice& largest);

    void DeleteFile(
        int level,
        uint64_t file);

    void EncodeTo(std::string* dst) const;

    Status DecodeFrom(const Slice& src);

private:
    friend class VersionSet;
    friend class DBImpl;

    bool has_log_number_;
    bool has_prev_log_number_;
    bool has_next_file_number_;
    bool has_last_sequence_;

    uint64_t log_number_;
    uint64_t prev_log_number_;
    uint64_t next_file_number_;
    uint64_t last_sequence_;

    typedef std::pair<int, uint64_t> DeletedFileSet;

    std::set<DeletedFileSet> deleted_files_;

    std::vector<
        std::pair<int, FileMetaData>>
        new_files_;
};

} // namespace minikv

