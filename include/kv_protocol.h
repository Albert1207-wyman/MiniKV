#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace minikv::kvproto {

enum class Command : uint8_t {
    kPing = 1,
    kGet = 2,
    kPut = 3,
    kDelete = 4,

    // Internal replication commands.
    // These commands are only used between MiniKV nodes.
    kReplicaPut = 5,
    kReplicaDelete = 6
};

enum class StatusCode : uint8_t {
    kOk = 0,
    kNotFound = 1,
    kError = 2
};

constexpr uint32_t kMaxFrameSize =
    64 * 1024 * 1024;

constexpr uint32_t kMaxKeySize =
    1 * 1024 * 1024;

constexpr uint32_t kMaxValueSize =
    64 * 1024 * 1024;

struct Request {
    Command command = Command::kPing;
    std::string key;
    std::string value;
};

struct Response {
    StatusCode status = StatusCode::kOk;
    std::string payload;
};

inline void AppendU32(
    std::string* dst,
    uint32_t value) {

    dst->push_back(
        static_cast<char>(
            (value >> 24) & 0xff));

    dst->push_back(
        static_cast<char>(
            (value >> 16) & 0xff));

    dst->push_back(
        static_cast<char>(
            (value >> 8) & 0xff));

    dst->push_back(
        static_cast<char>(
            value & 0xff));
}

inline bool ReadU32(
    std::string_view data,
    size_t* offset,
    uint32_t* value) {

    if (offset == nullptr ||
        value == nullptr ||
        *offset + 4 > data.size()) {

        return false;
    }

    const unsigned char* p =
        reinterpret_cast<const unsigned char*>(
            data.data() + *offset);

    *value =
        (static_cast<uint32_t>(p[0]) << 24) |
        (static_cast<uint32_t>(p[1]) << 16) |
        (static_cast<uint32_t>(p[2]) << 8) |
        static_cast<uint32_t>(p[3]);

    *offset += 4;

    return true;
}

inline std::string EncodeRequest(
    const Request& request) {

    std::string body;

    body.reserve(
        9 +
        request.key.size() +
        request.value.size());

    body.push_back(
        static_cast<char>(
            request.command));

    AppendU32(
        &body,
        static_cast<uint32_t>(
            request.key.size()));

    AppendU32(
        &body,
        static_cast<uint32_t>(
            request.value.size()));

    body.append(request.key);
    body.append(request.value);

    return body;
}

inline bool DecodeRequest(
    std::string_view body,
    Request* request) {

    if (request == nullptr ||
        body.size() < 9) {

        return false;
    }

    size_t offset = 0;

    request->command =
        static_cast<Command>(
            static_cast<uint8_t>(
                body[offset++]));

    uint32_t key_len = 0;
    uint32_t value_len = 0;

    if (!ReadU32(
            body,
            &offset,
            &key_len) ||
        !ReadU32(
            body,
            &offset,
            &value_len)) {

        return false;
    }

    if (key_len > kMaxKeySize ||
        value_len > kMaxValueSize) {

        return false;
    }

    const size_t payload_size =
        static_cast<size_t>(key_len) +
        static_cast<size_t>(value_len);

    if (offset + payload_size !=
        body.size()) {

        return false;
    }

    request->key.assign(
        body.data() + offset,
        key_len);

    offset += key_len;

    request->value.assign(
        body.data() + offset,
        value_len);

    switch (request->command) {
        case Command::kPing:
            return key_len == 0 &&
                   value_len == 0;

        case Command::kGet:
        case Command::kDelete:
            return key_len > 0 &&
                   value_len == 0;

        case Command::kPut:
            return key_len > 0;

        case Command::kReplicaPut:
            return key_len > 0;

        case Command::kReplicaDelete:
            return key_len > 0 &&
                   value_len == 0;
    }

    return false;
}

inline std::string EncodeResponse(
    const Response& response) {

    std::string body;

    body.reserve(
        5 +
        response.payload.size());

    body.push_back(
        static_cast<char>(
            response.status));

    AppendU32(
        &body,
        static_cast<uint32_t>(
            response.payload.size()));

    body.append(
        response.payload);

    return body;
}

inline bool DecodeResponse(
    std::string_view body,
    Response* response) {

    if (response == nullptr ||
        body.size() < 5) {

        return false;
    }

    size_t offset = 0;

    response->status =
        static_cast<StatusCode>(
            static_cast<uint8_t>(
                body[offset++]));

    uint32_t payload_len = 0;

    if (!ReadU32(
            body,
            &offset,
            &payload_len)) {

        return false;
    }

    if (payload_len > kMaxFrameSize ||
        offset +
            static_cast<size_t>(
                payload_len) !=
        body.size()) {

        return false;
    }

    response->payload.assign(
        body.data() + offset,
        payload_len);

    return true;
}

} // namespace minikv::kvproto