#include <arpa/inet.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <numeric>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "kv_protocol.h"

namespace {

using Clock = std::chrono::steady_clock;

// --------------------------------------------------------------------------
// Benchmark settings
// --------------------------------------------------------------------------

constexpr int kSocketTimeoutMs = 5000;

// 连续失败达到这个次数后，当前 benchmark worker 提前结束。
// 剩余操作统一计入 failed，避免 benchmark 永久等待。
constexpr int kMaxConsecutiveFailures = 3;

// 每完成这么多操作打印一次整体进度。
// 设置为 0 可以关闭进度输出。
constexpr uint64_t kProgressInterval = 1000;

std::atomic<uint64_t> g_completed_ops{0};
std::atomic<uint64_t> g_next_progress{kProgressInterval};

std::mutex g_progress_mutex;

// --------------------------------------------------------------------------
// Data structures
// --------------------------------------------------------------------------

struct BenchConfig {
    std::string host = "127.0.0.1";
    int port = 9000;

    int threads = 4;
    int ops_per_thread = 5000;

    int key_size = 16;
    int value_size = 100;

    std::string mode = "put";
};

struct ThreadResult {
    uint64_t success = 0;
    uint64_t failed = 0;

    std::vector<double> latency_us;
};

struct BenchResult {
    uint64_t success = 0;
    uint64_t failed = 0;

    std::vector<double> latency_us;
};

// --------------------------------------------------------------------------
// Socket helpers
// --------------------------------------------------------------------------

bool SetSocketTimeout(
    int fd,
    int timeout_ms) {

    if (fd < 0 || timeout_ms <= 0) {
        return false;
    }

    timeval timeout{};

    timeout.tv_sec =
        timeout_ms / 1000;

    timeout.tv_usec =
        (timeout_ms % 1000) * 1000;

    if (setsockopt(
            fd,
            SOL_SOCKET,
            SO_RCVTIMEO,
            &timeout,
            sizeof(timeout)) < 0) {

        return false;
    }

    if (setsockopt(
            fd,
            SOL_SOCKET,
            SO_SNDTIMEO,
            &timeout,
            sizeof(timeout)) < 0) {

        return false;
    }

    return true;
}

bool ReadFull(
    int fd,
    void* buffer,
    size_t size) {

    char* p =
        static_cast<char*>(buffer);

    size_t received = 0;

    while (received < size) {

        ssize_t n =
            recv(
                fd,
                p + received,
                size - received,
                0);

        if (n == 0) {
            return false;
        }

        if (n < 0) {

            if (errno == EINTR) {
                continue;
            }

            if (errno == EAGAIN ||
                errno == EWOULDBLOCK ||
                errno == ETIMEDOUT) {

                return false;
            }

            return false;
        }

        received +=
            static_cast<size_t>(n);
    }

    return true;
}

bool WriteFull(
    int fd,
    const void* buffer,
    size_t size) {

    const char* p =
        static_cast<const char*>(buffer);

    size_t sent = 0;

    while (sent < size) {

        ssize_t n =
            send(
                fd,
                p + sent,
                size - sent,
                MSG_NOSIGNAL);

        if (n < 0) {

            if (errno == EINTR) {
                continue;
            }

            if (errno == EAGAIN ||
                errno == EWOULDBLOCK ||
                errno == ETIMEDOUT) {

                return false;
            }

            return false;
        }

        if (n == 0) {
            return false;
        }

        sent +=
            static_cast<size_t>(n);
    }

    return true;
}

int Connect(
    const std::string& host,
    int port) {

    int fd =
        socket(
            AF_INET,
            SOCK_STREAM,
            0);

    if (fd < 0) {
        return -1;
    }

    int nodelay = 1;

    if (setsockopt(
            fd,
            IPPROTO_TCP,
            TCP_NODELAY,
            &nodelay,
            sizeof(nodelay)) < 0) {

        close(fd);
        return -1;
    }

    if (!SetSocketTimeout(
            fd,
            kSocketTimeoutMs)) {

        close(fd);
        return -1;
    }

    sockaddr_in address{};

    address.sin_family =
        AF_INET;

    address.sin_port =
        htons(
            static_cast<uint16_t>(
                port));

    if (inet_pton(
            AF_INET,
            host.c_str(),
            &address.sin_addr) != 1) {

        close(fd);
        return -1;
    }

    if (connect(
            fd,
            reinterpret_cast<sockaddr*>(&address),
            sizeof(address)) < 0) {

        close(fd);
        return -1;
    }

    return fd;
}

// --------------------------------------------------------------------------
// Protocol framing
// --------------------------------------------------------------------------

bool SendFrame(
    int fd,
    const std::string& body) {

    if (body.empty() ||
        body.size() >
            minikv::kvproto::kMaxFrameSize) {

        return false;
    }

    const uint32_t length =
        static_cast<uint32_t>(
            body.size());

    uint8_t header[4] = {
        static_cast<uint8_t>(
            (length >> 24) & 0xff),

        static_cast<uint8_t>(
            (length >> 16) & 0xff),

        static_cast<uint8_t>(
            (length >> 8) & 0xff),

        static_cast<uint8_t>(
            length & 0xff)
    };

    if (!WriteFull(
            fd,
            header,
            sizeof(header))) {

        return false;
    }

    return WriteFull(
        fd,
        body.data(),
        body.size());
}

bool ReadFrame(
    int fd,
    std::string* body) {

    if (body == nullptr) {
        return false;
    }

    uint8_t header[4];

    if (!ReadFull(
            fd,
            header,
            sizeof(header))) {

        return false;
    }

    const uint32_t length =
        (static_cast<uint32_t>(
             header[0]) << 24) |

        (static_cast<uint32_t>(
             header[1]) << 16) |

        (static_cast<uint32_t>(
             header[2]) << 8) |

        static_cast<uint32_t>(
            header[3]);

    if (length == 0 ||
        length >
            minikv::kvproto::kMaxFrameSize) {

        return false;
    }

    body->resize(length);

    return ReadFull(
        fd,
        body->data(),
        body->size());
}

bool SendRequest(
    int fd,
    const minikv::kvproto::Request& request,
    minikv::kvproto::Response* response) {

    if (response == nullptr) {
        return false;
    }

    const std::string request_body =
        minikv::kvproto::EncodeRequest(
            request);

    if (!SendFrame(
            fd,
            request_body)) {

        return false;
    }

    std::string response_body;

    if (!ReadFrame(
            fd,
            &response_body)) {

        return false;
    }

    return minikv::kvproto::DecodeResponse(
        response_body,
        response);
}

// --------------------------------------------------------------------------
// Test data generation
// --------------------------------------------------------------------------

std::string MakeKey(
    int thread_id,
    int op_id,
    int key_size) {

    std::string key =
        "k" +
        std::to_string(thread_id) +
        "_" +
        std::to_string(op_id);

    if (static_cast<int>(
            key.size()) <
        key_size) {

        key.resize(
            static_cast<size_t>(
                key_size),
            'x');

    } else if (
        static_cast<int>(
            key.size()) >
        key_size) {

        key.resize(
            static_cast<size_t>(
                key_size));
    }

    return key;
}

std::string MakeValue(
    int thread_id,
    int op_id,
    int value_size) {

    std::string value =
        "value_" +
        std::to_string(thread_id) +
        "_" +
        std::to_string(op_id);

    if (static_cast<int>(
            value.size()) <
        value_size) {

        value.resize(
            static_cast<size_t>(
                value_size),
            'v');

    } else if (
        static_cast<int>(
            value.size()) >
        value_size) {

        value.resize(
            static_cast<size_t>(
                value_size));
    }

    return value;
}

// --------------------------------------------------------------------------
// Benchmark operation
// --------------------------------------------------------------------------

bool RunOneOperation(
    int fd,
    const BenchConfig& config,
    int thread_id,
    int op_id) {

    minikv::kvproto::Request request;

    if (config.mode == "put") {

        request.command =
            minikv::kvproto::Command::kPut;

        request.key =
            MakeKey(
                thread_id,
                op_id,
                config.key_size);

        request.value =
            MakeValue(
                thread_id,
                op_id,
                config.value_size);

    } else if (
        config.mode == "get") {

        request.command =
            minikv::kvproto::Command::kGet;

        request.key =
            MakeKey(
                thread_id,
                op_id,
                config.key_size);

    } else if (
        config.mode == "delete") {

        request.command =
            minikv::kvproto::Command::kDelete;

        request.key =
            MakeKey(
                thread_id,
                op_id,
                config.key_size);

    } else {

        return false;
    }

    minikv::kvproto::Response response;

    if (!SendRequest(
            fd,
            request,
            &response)) {

        return false;
    }

    if (config.mode == "get") {

        return response.status ==
                   minikv::kvproto::StatusCode::kOk ||

               response.status ==
                   minikv::kvproto::StatusCode::kNotFound;
    }

    return response.status ==
           minikv::kvproto::StatusCode::kOk;
}

// --------------------------------------------------------------------------
// Progress reporting
// --------------------------------------------------------------------------

void ReportProgress(
    uint64_t completed) {

    if (kProgressInterval == 0) {
        return;
    }

    while (true) {

        uint64_t next =
            g_next_progress.load(
                std::memory_order_relaxed);

        if (completed < next) {
            return;
        }

        if (g_next_progress.compare_exchange_weak(
                next,
                next + kProgressInterval,
                std::memory_order_relaxed,
                std::memory_order_relaxed)) {

            std::lock_guard<std::mutex> lock(
                g_progress_mutex);

            std::cout
                << "[PROGRESS] completed="
                << completed
                << "\n";

            return;
        }
    }
}

// --------------------------------------------------------------------------
// Worker
// --------------------------------------------------------------------------

void RunThread(
    const BenchConfig& config,
    int thread_id,
    ThreadResult* result) {

    if (result == nullptr) {
        return;
    }

    result->latency_us.reserve(
        static_cast<size_t>(
            config.ops_per_thread));

    const int fd =
        Connect(
            config.host,
            config.port);

    if (fd < 0) {

        result->failed =
            static_cast<uint64_t>(
                config.ops_per_thread);

        const uint64_t completed =
            g_completed_ops.fetch_add(
                static_cast<uint64_t>(
                    config.ops_per_thread),
                std::memory_order_relaxed) +
            static_cast<uint64_t>(
                config.ops_per_thread);

        ReportProgress(completed);

        std::lock_guard<std::mutex> lock(
            g_progress_mutex);

        std::cerr
            << "[THREAD "
            << thread_id
            << "] connect failed\n";

        return;
    }

    int consecutive_failures = 0;

    for (int i = 0;
         i < config.ops_per_thread;
         ++i) {

        const auto begin =
            Clock::now();

        const bool ok =
            RunOneOperation(
                fd,
                config,
                thread_id,
                i);

        const auto end =
            Clock::now();

        const double latency =
            std::chrono::duration<
                double,
                std::micro>(
                end - begin)
                .count();

        if (ok) {

            ++result->success;

            result->latency_us.push_back(
                latency);

            consecutive_failures = 0;

        } else {

            ++result->failed;

            ++consecutive_failures;
        }

        const uint64_t completed =
            g_completed_ops.fetch_add(
                1,
                std::memory_order_relaxed) +
            1;

        ReportProgress(completed);

        if (consecutive_failures >=
            kMaxConsecutiveFailures) {

            const uint64_t remaining =
                static_cast<uint64_t>(
                    config.ops_per_thread -
                    i -
                    1);

            result->failed +=
                remaining;

            if (remaining > 0) {

                const uint64_t completed_after_abort =
                    g_completed_ops.fetch_add(
                        remaining,
                        std::memory_order_relaxed) +
                    remaining;

                ReportProgress(
                    completed_after_abort);
            }

            std::lock_guard<std::mutex> lock(
                g_progress_mutex);

            std::cerr
                << "[THREAD "
                << thread_id
                << "] stopping after "
                << consecutive_failures
                << " consecutive failures"
                << "\n";

            break;
        }
    }

    shutdown(
        fd,
        SHUT_RDWR);

    close(fd);
}

// --------------------------------------------------------------------------
// Argument parsing
// --------------------------------------------------------------------------

bool ParseInt(
    const char* text,
    int* value) {

    if (text == nullptr ||
        value == nullptr) {

        return false;
    }

    try {

        long long parsed =
            std::stoll(text);

        if (parsed <= 0 ||
            parsed >
                std::numeric_limits<
                    int>::max()) {

            return false;
        }

        *value =
            static_cast<int>(
                parsed);

        return true;

    } catch (...) {

        return false;
    }
}

void PrintUsage(
    const char* program) {

    std::cout
        << "Usage:\n"
        << "  "
        << program
        << " [host] [port] [threads]"
           " [ops_per_thread]"
           " [mode]"
           " [key_size]"
           " [value_size]\n\n"

        << "Options:\n"
        << "  -h, --help       show this help\n\n"

        << "mode:\n"
        << "  put       benchmark PUT\n"
        << "  get       benchmark GET\n"
        << "  delete    benchmark DELETE\n\n"

        << "Defaults:\n"
        << "  host=127.0.0.1\n"
        << "  port=9000\n"
        << "  threads=4\n"
        << "  ops_per_thread=5000\n"
        << "  mode=put\n"
        << "  key_size=16\n"
        << "  value_size=100\n\n"

        << "Example:\n"
        << "  "
        << program
        << " 127.0.0.1 9000 4 5000 put 16 100\n\n"

        << "Runtime safeguards:\n"
        << "  socket_timeout_ms="
        << kSocketTimeoutMs
        << "\n"
        << "  max_consecutive_failures="
        << kMaxConsecutiveFailures
        << "\n";
}

// --------------------------------------------------------------------------
// Result merge
// --------------------------------------------------------------------------

BenchResult MergeResults(
    const std::vector<ThreadResult>& results) {

    BenchResult merged;

    uint64_t total_latency_count = 0;

    for (const ThreadResult& result :
         results) {

        merged.success +=
            result.success;

        merged.failed +=
            result.failed;

        total_latency_count +=
            static_cast<uint64_t>(
                result.latency_us.size());
    }

    merged.latency_us.reserve(
        static_cast<size_t>(
            total_latency_count));

    for (const ThreadResult& result :
         results) {

        merged.latency_us.insert(
            merged.latency_us.end(),
            result.latency_us.begin(),
            result.latency_us.end());
    }

    return merged;
}

// --------------------------------------------------------------------------
// Percentile
// --------------------------------------------------------------------------

double Percentile(
    std::vector<double>& values,
    double percentile) {

    if (values.empty()) {
        return 0.0;
    }

    std::sort(
        values.begin(),
        values.end());

    const double index =
        (percentile / 100.0) *
        static_cast<double>(
            values.size() - 1);

    const size_t lower =
        static_cast<size_t>(index);

    const size_t upper =
        (lower + 1 < values.size())
            ? lower + 1
            : lower;

    const double fraction =
        index -
        static_cast<double>(lower);

    return values[lower] +
           (values[upper] -
            values[lower]) *
               fraction;
}

} // namespace

// --------------------------------------------------------------------------
// main
// --------------------------------------------------------------------------

int main(
    int argc,
    char** argv) {

    if (argc == 2 &&
        (std::string(argv[1]) == "--help" ||
         std::string(argv[1]) == "-h")) {

        PrintUsage(argv[0]);
        return 0;
    }

    if (argc > 8) {

        PrintUsage(argv[0]);
        return 1;
    }

    BenchConfig config;

    if (argc > 1) {
        config.host = argv[1];
    }

    if (argc > 2 &&
        !ParseInt(
            argv[2],
            &config.port)) {

        PrintUsage(argv[0]);
        return 1;
    }

    if (argc > 3 &&
        !ParseInt(
            argv[3],
            &config.threads)) {

        PrintUsage(argv[0]);
        return 1;
    }

    if (argc > 4 &&
        !ParseInt(
            argv[4],
            &config.ops_per_thread)) {

        PrintUsage(argv[0]);
        return 1;
    }

    if (argc > 5) {

        config.mode =
            argv[5];

        if (config.mode != "put" &&
            config.mode != "get" &&
            config.mode != "delete") {

            PrintUsage(argv[0]);
            return 1;
        }
    }

    if (argc > 6 &&
        !ParseInt(
            argv[6],
            &config.key_size)) {

        PrintUsage(argv[0]);
        return 1;
    }

    if (argc > 7 &&
        !ParseInt(
            argv[7],
            &config.value_size)) {

        PrintUsage(argv[0]);
        return 1;
    }

    const uint64_t total_ops =
        static_cast<uint64_t>(
            config.threads) *
        static_cast<uint64_t>(
            config.ops_per_thread);

    g_completed_ops.store(
        0,
        std::memory_order_relaxed);

    g_next_progress.store(
        kProgressInterval,
        std::memory_order_relaxed);

    std::cout
        << "=== MiniKV Benchmark ===\n"
        << "host=" << config.host << "\n"
        << "port=" << config.port << "\n"
        << "threads=" << config.threads << "\n"
        << "ops_per_thread="
        << config.ops_per_thread
        << "\n"
        << "total_ops=" << total_ops << "\n"
        << "mode=" << config.mode << "\n"
        << "key_size=" << config.key_size << "\n"
        << "value_size=" << config.value_size << "\n"
        << "connection_model="
           "1 persistent TCP connection / thread\n"
        << "socket_timeout_ms="
        << kSocketTimeoutMs
        << "\n"
        << "max_consecutive_failures="
        << kMaxConsecutiveFailures
        << "\n"
        << "========================\n";

    std::vector<ThreadResult> results(
        static_cast<size_t>(
            config.threads));

    std::vector<std::thread> workers;

    workers.reserve(
        static_cast<size_t>(
            config.threads));

    const auto begin =
        Clock::now();

    for (int thread_id = 0;
         thread_id < config.threads;
         ++thread_id) {

        workers.emplace_back(
            RunThread,
            std::cref(config),
            thread_id,
            &results[
                static_cast<size_t>(
                    thread_id)]);
    }

    for (std::thread& worker :
         workers) {

        worker.join();
    }

    const auto end =
        Clock::now();

    const double elapsed_s =
        std::chrono::duration<double>(
            end - begin)
            .count();

    BenchResult result =
        MergeResults(results);

    const uint64_t completed_ops =
        result.success +
        result.failed;

    const double qps =
        elapsed_s > 0.0
            ? static_cast<double>(
                  completed_ops) /
                  elapsed_s
            : 0.0;

    const double error_rate =
        completed_ops > 0
            ? static_cast<double>(
                  result.failed) /
                  static_cast<double>(
                      completed_ops) *
                  100.0
            : 0.0;

    std::cout
        << "\n=== Result ===\n"
        << "elapsed_s="
        << std::fixed
        << std::setprecision(6)
        << elapsed_s
        << "\n"

        << "success="
        << result.success
        << "\n"

        << "failed="
        << result.failed
        << "\n"

        << "qps="
        << std::setprecision(1)
        << qps
        << "\n"

        << "error_rate="
        << std::setprecision(3)
        << error_rate
        << "%\n";

    if (!result.latency_us.empty()) {

        const double p50 =
            Percentile(
                result.latency_us,
                50.0);

        const double p95 =
            Percentile(
                result.latency_us,
                95.0);

        const double p99 =
            Percentile(
                result.latency_us,
                99.0);

        const double avg =
            std::accumulate(
                result.latency_us.begin(),
                result.latency_us.end(),
                0.0) /
            static_cast<double>(
                result.latency_us.size());

        std::cout
            << "latency_us: avg="
            << std::setprecision(3)
            << avg
            << ", p50="
            << p50
            << ", p95="
            << p95
            << ", p99="
            << p99
            << "\n";
    }

    std::cout
        << "========================\n";

    return result.failed == 0
               ? 0
               : 2;
}