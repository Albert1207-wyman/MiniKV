#include "kv_router.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <stdexcept>
#include <utility>

#include "kv_protocol.h"

namespace minikv {

namespace {

constexpr int kBacklog = 512;
constexpr int kMaxEvents = 1024;
constexpr int kPollTimeoutMs = 1000;

constexpr size_t kDefaultWorkerCount = 8;

constexpr size_t kMaxInputBuffer =
    static_cast<size_t>(
        kvproto::kMaxFrameSize) *
    2;

// FNV-1a 64-bit.
size_t HashKey(
    const std::string& key) {

    uint64_t hash =
        14695981039346656037ull;

    for (unsigned char c :
         key) {

        hash ^=
            static_cast<uint64_t>(c);

        hash *=
            1099511628211ull;
    }

    return static_cast<size_t>(
        hash);
}

} // namespace

KvRouter::KvRouter(
    std::array<ShardGroup, kShardCount> shards,
    std::string listen_host,
    uint16_t listen_port)
    : shards_(std::move(shards)),
      listen_host_(std::move(listen_host)),
      listen_port_(listen_port),
      listen_fd_(-1),
      epoll_fd_(-1),
      event_fd_(-1),
      stop_requested_(0),
      worker_count_(kDefaultWorkerCount),
      worker_stop_(false) {

    const unsigned int cpu_count =
        std::thread::hardware_concurrency();

    if (cpu_count > 0) {
        worker_count_ =
            static_cast<size_t>(
                cpu_count);

        if (worker_count_ > 16) {
            worker_count_ = 16;
        }
    }
}

KvRouter::~KvRouter() {
    Stop();
}

void KvRouter::RequestStop() noexcept {
    stop_requested_ = 1;
}

void KvRouter::Stop() {
    stop_requested_ = 1;

    {
        std::lock_guard<std::mutex> lock(
            task_mutex_);

        worker_stop_ = true;
    }

    task_cv_.notify_all();
}

// --------------------------------------------------------------------------
// Socket
// --------------------------------------------------------------------------

bool KvRouter::SetSocketOptions(
    int fd) {

    int reuse = 1;

    if (setsockopt(
            fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &reuse,
            sizeof(reuse)) < 0) {

        return false;
    }

    int keepalive = 1;

    if (setsockopt(
            fd,
            SOL_SOCKET,
            SO_KEEPALIVE,
            &keepalive,
            sizeof(keepalive)) < 0) {

        return false;
    }

    int nodelay = 1;

    if (setsockopt(
            fd,
            IPPROTO_TCP,
            TCP_NODELAY,
            &nodelay,
            sizeof(nodelay)) < 0) {

        return false;
    }

    return true;
}

// --------------------------------------------------------------------------
// Hash
// --------------------------------------------------------------------------

size_t KvRouter::SelectShard(
    const std::string& key) const {

    return HashKey(key) %
           kShardCount;
}

// --------------------------------------------------------------------------
// Backend IO
// --------------------------------------------------------------------------

bool KvRouter::ReadFull(
    int fd,
    void* buffer,
    size_t size) {

    char* p =
        static_cast<char*>(buffer);

    size_t received = 0;

    while (received < size) {
        const ssize_t n =
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

            return false;
        }

        received +=
            static_cast<size_t>(n);
    }

    return true;
}

bool KvRouter::WriteFull(
    int fd,
    const void* buffer,
    size_t size) {

    const char* p =
        static_cast<const char*>(
            buffer);

    size_t sent = 0;

    while (sent < size) {
        const ssize_t n =
            send(
                fd,
                p + sent,
                size - sent,
                MSG_NOSIGNAL);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
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

bool KvRouter::SendFrame(
    int fd,
    const std::string& body) {

    if (body.empty() ||
        body.size() >
            kvproto::kMaxFrameSize) {

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

    return
        WriteFull(
            fd,
            header,
            sizeof(header)) &&
        WriteFull(
            fd,
            body.data(),
            body.size());
}

bool KvRouter::ReadFrame(
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
             header[0])
         << 24) |
        (static_cast<uint32_t>(
             header[1])
         << 16) |
        (static_cast<uint32_t>(
             header[2])
         << 8) |
        static_cast<uint32_t>(
            header[3]);

    if (length == 0 ||
        length > kvproto::kMaxFrameSize) {

        return false;
    }

    body->resize(length);

    return ReadFull(
        fd,
        body->data(),
        body->size());
}

// --------------------------------------------------------------------------
// Backend Connection
// --------------------------------------------------------------------------

int KvRouter::ConnectBackend(
    size_t shard,
    size_t replica) {

    if (shard >= kShardCount ||
        replica >= kReplicaCount) {

        return -1;
    }

    const Backend& backend =
        shards_[shard][replica];

    const int fd =
        socket(
            AF_INET,
            SOCK_STREAM,
            0);

    if (fd < 0) {
        return -1;
    }

    if (!SetSocketOptions(fd)) {
        close(fd);
        return -1;
    }

    sockaddr_in address{};

    address.sin_family =
        AF_INET;

    address.sin_port =
        htons(backend.port);

    if (inet_pton(
            AF_INET,
            backend.host.c_str(),
            &address.sin_addr) != 1) {

        close(fd);
        return -1;
    }

    if (connect(
            fd,
            reinterpret_cast<
                sockaddr*>(&address),
            sizeof(address)) < 0) {

        close(fd);
        return -1;
    }

    return fd;
}

// --------------------------------------------------------------------------
// Forward
// --------------------------------------------------------------------------

bool KvRouter::ForwardToBackend(
    const std::shared_ptr<ConnectionState>& connection,
    size_t shard,
    const std::string& request_body,
    std::string* response_body) {

    if (!connection ||
        response_body == nullptr ||
        shard >= kShardCount) {

        return false;
    }

    std::lock_guard<std::mutex> lock(
        connection->backend_mutex);

    const size_t preferred =
        connection->preferred_replica[
            shard];

    std::string last_response;

    for (size_t attempt = 0;
         attempt < kReplicaCount;
         ++attempt) {

        const size_t replica =
            (preferred + attempt) %
            kReplicaCount;

        int& backend_fd =
            connection->backend_fds[
                shard][replica];

        if (backend_fd < 0) {
            backend_fd =
                ConnectBackend(
                    shard,
                    replica);

            if (backend_fd < 0) {
                continue;
            }

            std::cout
                << "[ROUTE] connected shard="
                << shard
                << " replica="
                << replica
                << " -> "
                << shards_[shard][replica].host
                << ":"
                << shards_[shard][replica].port
                << "\n";
        }

        if (!SendFrame(
                backend_fd,
                request_body)) {

            close(backend_fd);

            backend_fd = -1;

            continue;
        }

        std::string response;

        if (!ReadFrame(
                backend_fd,
                &response)) {

            close(backend_fd);

            backend_fd = -1;

            continue;
        }

        kvproto::Response decoded;

        if (!kvproto::DecodeResponse(
                response,
                &decoded)) {

            close(backend_fd);

            backend_fd = -1;

            continue;
        }

        last_response =
            response;

        // Follower told us to find the leader.
        if (decoded.status ==
                kvproto::StatusCode::kError &&
            decoded.payload ==
                "NOT_LEADER") {

            std::cout
                << "[ROUTE] shard="
                << shard
                << " replica="
                << replica
                << " is not leader\n";

            continue;
        }

        connection->preferred_replica[
            shard] =
            replica;

        *response_body =
            std::move(response);

        std::cout
            << "[ROUTE] shard="
            << shard
            << " using replica="
            << replica
            << "\n";

        return true;
    }

    // All candidates might have replied NOT_LEADER.
    if (!last_response.empty()) {
        *response_body =
            std::move(last_response);

        return true;
    }

    return false;
}

// --------------------------------------------------------------------------
// Request
// --------------------------------------------------------------------------

std::string KvRouter::HandleRequest(
    const std::shared_ptr<ConnectionState>& connection,
    const std::string& request_body) {

    kvproto::Request request;

    if (!kvproto::DecodeRequest(
            request_body,
            &request)) {

        return kvproto::EncodeResponse(
            kvproto::Response{
                kvproto::StatusCode::kError,
                "invalid request"
            });
    }

    if (request.command ==
        kvproto::Command::kPing) {

        return kvproto::EncodeResponse(
            kvproto::Response{
                kvproto::StatusCode::kOk,
                "PONG"
            });
    }

    size_t shard = 0;

    switch (request.command) {
        case kvproto::Command::kGet:
        case kvproto::Command::kPut:
        case kvproto::Command::kDelete:
            shard =
                SelectShard(
                    request.key);
            break;

        default:
            return kvproto::EncodeResponse(
                kvproto::Response{
                    kvproto::StatusCode::kError,
                    "unknown command"
                });
    }

    std::string response_body;

    if (!ForwardToBackend(
            connection,
            shard,
            request_body,
            &response_body)) {

        return kvproto::EncodeResponse(
            kvproto::Response{
                kvproto::StatusCode::kError,
                "all backend replicas unavailable"
            });
    }

    return response_body;
}

// --------------------------------------------------------------------------
// Worker
// --------------------------------------------------------------------------

void KvRouter::EnqueueRequest(
    const std::shared_ptr<ConnectionState>& connection,
    std::string request_body) {

    if (!connection) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(
            task_mutex_);

        if (worker_stop_) {
            return;
        }

        tasks_.push(
            Task{
                connection,
                std::move(request_body)
            });
    }

    task_cv_.notify_one();
}

void KvRouter::WorkerLoop() {
    while (true) {
        Task task;

        {
            std::unique_lock<std::mutex> lock(
                task_mutex_);

            task_cv_.wait(
                lock,
                [this] {
                    return worker_stop_ ||
                           !tasks_.empty();
                });

            if (worker_stop_ &&
                tasks_.empty()) {

                return;
            }

            task =
                std::move(
                    tasks_.front());

            tasks_.pop();
        }

        if (!task.connection) {
            continue;
        }

        // Keep requests from one client serialized.
        std::lock_guard<std::mutex>
            request_lock(
                task.connection->request_mutex);

        std::string response =
            HandleRequest(
                task.connection,
                task.request_body);

        PublishResponse(
            task.connection,
            std::move(response));
    }
}

// --------------------------------------------------------------------------
// Response -> Reactor
// --------------------------------------------------------------------------

void KvRouter::NotifyReactor(
    const std::shared_ptr<ConnectionState>& connection) {

    {
        std::lock_guard<std::mutex> lock(
            notify_mutex_);

        notified_connections_.push(
            connection);
    }

    const uint64_t value = 1;

    const ssize_t n =
        write(
            event_fd_,
            &value,
            sizeof(value));

    if (n < 0 &&
        errno != EAGAIN &&
        errno != EINTR) {

        std::cerr
            << "[WARN] eventfd notify failed: "
            << std::strerror(errno)
            << "\n";
    }
}

void KvRouter::PublishResponse(
    const std::shared_ptr<ConnectionState>& connection,
    std::string response_body) {

    if (!connection ||
        response_body.empty() ||
        response_body.size() >
            kvproto::kMaxFrameSize) {

        return;
    }

    const uint32_t length =
        static_cast<uint32_t>(
            response_body.size());

    std::string frame;

    frame.reserve(
        4 +
        response_body.size());

    frame.push_back(
        static_cast<char>(
            (length >> 24) & 0xff));

    frame.push_back(
        static_cast<char>(
            (length >> 16) & 0xff));

    frame.push_back(
        static_cast<char>(
            (length >> 8) & 0xff));

    frame.push_back(
        static_cast<char>(
            length & 0xff));

    frame.append(
        response_body);

    {
        std::lock_guard<std::mutex> lock(
            connection->output_mutex);

        connection->output.push_back(
            ConnectionState::OutputBuffer{
                std::move(frame),
                0
            });
    }

    NotifyReactor(
        connection);
}

void KvRouter::DrainNotifications() {
    std::queue<
        std::shared_ptr<ConnectionState>>
        local_queue;

    {
        std::lock_guard<std::mutex> lock(
            notify_mutex_);

        std::swap(
            local_queue,
            notified_connections_);
    }

    while (!local_queue.empty()) {
        auto connection =
            std::move(
                local_queue.front());

        local_queue.pop();

        if (!IsCurrentConnection(
                connection)) {

            continue;
        }

        UpdateConnectionEvents(
            connection);
    }
}

// --------------------------------------------------------------------------
// Frontend Read
// --------------------------------------------------------------------------

bool KvRouter::ReadFromSocket(
    const std::shared_ptr<ConnectionState>& connection) {

    if (!connection) {
        return false;
    }

    char buffer[
        64 * 1024];

    while (true) {
        const ssize_t n =
            recv(
                connection->fd,
                buffer,
                sizeof(buffer),
                0);

        if (n > 0) {
            connection->input.append(
                buffer,
                static_cast<size_t>(
                    n));

            if (connection->input.size() >
                kMaxInputBuffer) {

                return false;
            }

            if (!ParseFrames(
                    connection)) {

                return false;
            }

            continue;
        }

        if (n == 0) {
            return false;
        }

        if (errno == EINTR) {
            continue;
        }

        if (errno == EAGAIN ||
            errno == EWOULDBLOCK) {

            return true;
        }

        return false;
    }
}

bool KvRouter::ParseFrames(
    const std::shared_ptr<ConnectionState>& connection) {

    if (!connection) {
        return false;
    }

    while (connection->input.size() >= 4) {
        const unsigned char* p =
            reinterpret_cast<
                const unsigned char*>(
                    connection->input.data());

        const uint32_t length =
            (static_cast<uint32_t>(
                 p[0])
             << 24) |
            (static_cast<uint32_t>(
                 p[1])
             << 16) |
            (static_cast<uint32_t>(
                 p[2])
             << 8) |
            static_cast<uint32_t>(
                p[3]);

        if (length == 0 ||
            length > kvproto::kMaxFrameSize) {

            return false;
        }

        const size_t frame_size =
            4 +
            static_cast<size_t>(
                length);

        if (connection->input.size() <
            frame_size) {

            return true;
        }

        std::string request_body =
            connection->input.substr(
                4,
                length);

        connection->input.erase(
            0,
            frame_size);

        EnqueueRequest(
            connection,
            std::move(request_body));
    }

    return true;
}

void KvRouter::HandleClientRead(
    const std::shared_ptr<ConnectionState>& connection) {

    if (!ReadFromSocket(
            connection)) {

        CloseConnection(
            connection);
    }
}

// --------------------------------------------------------------------------
// Frontend Write
// --------------------------------------------------------------------------

bool KvRouter::WriteToSocket(
    const std::shared_ptr<ConnectionState>& connection) {

    if (!connection) {
        return false;
    }

    std::lock_guard<std::mutex> lock(
        connection->output_mutex);

    while (!connection->output.empty()) {
        auto& output =
            connection->output.front();

        const char* data =
            output.data.data() +
            output.offset;

        const size_t remaining =
            output.data.size() -
            output.offset;

        const ssize_t n =
            send(
                connection->fd,
                data,
                remaining,
                MSG_NOSIGNAL);

        if (n > 0) {
            output.offset +=
                static_cast<size_t>(
                    n);

            if (output.offset ==
                output.data.size()) {

                connection->output.pop_front();
            }

            continue;
        }

        if (n < 0 &&
            errno == EINTR) {

            continue;
        }

        if (n < 0 &&
            (errno == EAGAIN ||
             errno == EWOULDBLOCK)) {

            return true;
        }

        return false;
    }

    return true;
}

void KvRouter::HandleClientWrite(
    const std::shared_ptr<ConnectionState>& connection) {

    if (!WriteToSocket(
            connection)) {

        CloseConnection(
            connection);

        return;
    }

    UpdateConnectionEvents(
        connection);
}

// --------------------------------------------------------------------------
// Connection
// --------------------------------------------------------------------------

bool KvRouter::IsCurrentConnection(
    const std::shared_ptr<ConnectionState>& connection) const {

    if (!connection) {
        return false;
    }

    auto it =
        connections_.find(
            connection->fd);

    if (it == connections_.end()) {
        return false;
    }

    return it->second.get() ==
           connection.get();
}

void KvRouter::UpdateConnectionEvents(
    const std::shared_ptr<ConnectionState>& connection) {

    if (!IsCurrentConnection(
            connection)) {

        return;
    }

    uint32_t events =
        EPOLLIN |
        EPOLLRDHUP |
        EPOLLERR |
        EPOLLHUP;

    {
        std::lock_guard<std::mutex> lock(
            connection->output_mutex);

        if (!connection->output.empty()) {
            events |= EPOLLOUT;
        }
    }

    epoll_event event{};

    event.events =
        events;

    event.data.fd =
        connection->fd;

    if (epoll_ctl(
            epoll_fd_,
            EPOLL_CTL_MOD,
            connection->fd,
            &event) < 0) {

        if (errno != EBADF &&
            errno != ENOENT) {

            std::cerr
                << "[WARN] epoll MOD failed fd="
                << connection->fd
                << ": "
                << std::strerror(errno)
                << "\n";
        }
    }
}

void KvRouter::CloseConnection(
    const std::shared_ptr<ConnectionState>& connection) {

    if (!connection) {
        return;
    }

    auto it =
        connections_.find(
            connection->fd);

    if (it == connections_.end() ||
        it->second.get() !=
            connection.get()) {

        return;
    }

    epoll_ctl(
        epoll_fd_,
        EPOLL_CTL_DEL,
        connection->fd,
        nullptr);

    shutdown(
        connection->fd,
        SHUT_RDWR);

    close(
        connection->fd);

    {
        std::lock_guard<std::mutex> lock(
            connection->backend_mutex);

        for (auto& shard :
             connection->backend_fds) {

            for (int& backend_fd :
                 shard) {

                if (backend_fd >= 0) {
                    shutdown(
                        backend_fd,
                        SHUT_RDWR);

                    close(
                        backend_fd);

                    backend_fd = -1;
                }
            }
        }
    }

    connections_.erase(it);

    std::cout
        << "[INFO] Router client disconnected fd="
        << connection->fd
        << "\n";
}

// --------------------------------------------------------------------------
// Start
// --------------------------------------------------------------------------

int KvRouter::Start() {
    listen_fd_ =
        socket(
            AF_INET,
            SOCK_STREAM,
            0);

    if (listen_fd_ < 0) {
        return 1;
    }

    const int flags =
        fcntl(
            listen_fd_,
            F_GETFL,
            0);

    if (flags < 0 ||
        fcntl(
            listen_fd_,
            F_SETFL,
            flags | O_NONBLOCK) < 0) {

        close(listen_fd_);
        listen_fd_ = -1;

        return 1;
    }

    if (!SetSocketOptions(
            listen_fd_)) {

        close(listen_fd_);
        listen_fd_ = -1;

        return 1;
    }

    sockaddr_in address{};

    address.sin_family =
        AF_INET;

    address.sin_port =
        htons(listen_port_);

    if (inet_pton(
            AF_INET,
            listen_host_.c_str(),
            &address.sin_addr) != 1) {

        close(listen_fd_);
        listen_fd_ = -1;

        return 1;
    }

    if (bind(
            listen_fd_,
            reinterpret_cast<
                sockaddr*>(&address),
            sizeof(address)) < 0) {

        std::cerr
            << "[ERROR] bind(): "
            << std::strerror(errno)
            << "\n";

        close(listen_fd_);
        listen_fd_ = -1;

        return 1;
    }

    if (listen(
            listen_fd_,
            kBacklog) < 0) {

        close(listen_fd_);
        listen_fd_ = -1;

        return 1;
    }

    epoll_fd_ =
        epoll_create1(
            EPOLL_CLOEXEC);

    if (epoll_fd_ < 0) {
        close(listen_fd_);
        listen_fd_ = -1;

        return 1;
    }

    event_fd_ =
        eventfd(
            0,
            EFD_NONBLOCK |
            EFD_CLOEXEC);

    if (event_fd_ < 0) {
        close(epoll_fd_);
        close(listen_fd_);

        epoll_fd_ = -1;
        listen_fd_ = -1;

        return 1;
    }

    epoll_event listen_event{};

    listen_event.events =
        EPOLLIN;

    listen_event.data.fd =
        listen_fd_;

    if (epoll_ctl(
            epoll_fd_,
            EPOLL_CTL_ADD,
            listen_fd_,
            &listen_event) < 0) {

        close(event_fd_);
        close(epoll_fd_);
        close(listen_fd_);

        return 1;
    }

    epoll_event eventfd_event{};

    eventfd_event.events =
        EPOLLIN;

    eventfd_event.data.fd =
        event_fd_;

    if (epoll_ctl(
            epoll_fd_,
            EPOLL_CTL_ADD,
            event_fd_,
            &eventfd_event) < 0) {

        close(event_fd_);
        close(epoll_fd_);
        close(listen_fd_);

        return 1;
    }

    {
        std::lock_guard<std::mutex> lock(
            task_mutex_);

        worker_stop_ = false;
    }

    workers_.reserve(
        worker_count_);

    for (size_t i = 0;
         i < worker_count_;
         ++i) {

        workers_.emplace_back(
            &KvRouter::WorkerLoop,
            this);
    }

    stop_requested_ = 0;

    std::cout
        << "========================================\n"
        << " MiniKV Distributed Router V2\n"
        << " Listen  : "
        << listen_host_
        << ":"
        << listen_port_
        << "\n"
        << " Reactor : epoll\n"
        << " Workers : "
        << worker_count_
        << "\n"
        << " Shards  : "
        << kShardCount
        << "\n"
        << " Replicas: "
        << kReplicaCount
        << "\n"
        << "========================================\n";

    for (size_t shard = 0;
         shard < kShardCount;
         ++shard) {

        for (size_t replica = 0;
             replica < kReplicaCount;
             ++replica) {

            std::cout
                << "Shard "
                << shard
                << " Replica "
                << replica
                << " -> "
                << shards_[shard][replica].host
                << ":"
                << shards_[shard][replica].port
                << "\n";
        }
    }

    epoll_event events[
        kMaxEvents];

    while (!stop_requested_) {
        const int event_count =
            epoll_wait(
                epoll_fd_,
                events,
                kMaxEvents,
                kPollTimeoutMs);

        if (event_count < 0) {
            if (errno == EINTR) {
                continue;
            }

            std::cerr
                << "[ERROR] epoll_wait(): "
                << std::strerror(errno)
                << "\n";

            break;
        }

        for (int i = 0;
             i < event_count;
             ++i) {

            const int fd =
                events[i].data.fd;

            const uint32_t event =
                events[i].events;

            if (fd == event_fd_) {
                uint64_t value = 0;

                while (true) {
                    const ssize_t n =
                        read(
                            event_fd_,
                            &value,
                            sizeof(value));

                    if (n ==
                        static_cast<ssize_t>(
                            sizeof(value))) {

                        continue;
                    }

                    if (n < 0 &&
                        (errno == EAGAIN ||
                         errno == EWOULDBLOCK)) {

                        break;
                    }

                    if (n < 0 &&
                        errno == EINTR) {

                        continue;
                    }

                    break;
                }

                DrainNotifications();

                continue;
            }

            if (fd == listen_fd_) {
                while (true) {
                    sockaddr_in client_addr{};

                    socklen_t client_len =
                        sizeof(client_addr);

                    const int client_fd =
                        accept4(
                            listen_fd_,
                            reinterpret_cast<
                                sockaddr*>(
                                &client_addr),
                            &client_len,
                            SOCK_NONBLOCK |
                            SOCK_CLOEXEC);

                    if (client_fd < 0) {
                        if (errno == EINTR) {
                            continue;
                        }

                        if (errno == EAGAIN ||
                            errno == EWOULDBLOCK) {

                            break;
                        }

                        break;
                    }

                    if (!SetSocketOptions(
                            client_fd)) {

                        close(client_fd);
                        continue;
                    }

                    auto connection =
                        std::make_shared<
                            ConnectionState>(
                                client_fd);

                    epoll_event client_event{};

                    client_event.events =
                        EPOLLIN |
                        EPOLLRDHUP |
                        EPOLLERR |
                        EPOLLHUP;

                    client_event.data.fd =
                        client_fd;

                    if (epoll_ctl(
                            epoll_fd_,
                            EPOLL_CTL_ADD,
                            client_fd,
                            &client_event) < 0) {

                        close(client_fd);
                        continue;
                    }

                    connections_[client_fd] =
                        connection;

                    std::cout
                        << "[INFO] Router client connected fd="
                        << client_fd
                        << "\n";
                }

                continue;
            }

            auto it =
                connections_.find(fd);

            if (it == connections_.end()) {
                continue;
            }

            auto connection =
                it->second;

            if (event &
                (EPOLLERR |
                 EPOLLHUP |
                 EPOLLRDHUP)) {

                if (event & EPOLLIN) {
                    HandleClientRead(
                        connection);
                }

                if (IsCurrentConnection(
                        connection)) {

                    CloseConnection(
                        connection);
                }

                continue;
            }

            if (event & EPOLLIN) {
                HandleClientRead(
                    connection);

                if (!IsCurrentConnection(
                        connection)) {

                    continue;
                }
            }

            if (event & EPOLLOUT) {
                HandleClientWrite(
                    connection);
            }
        }
    }

    // --------------------------------------------------
    // Shutdown
    // --------------------------------------------------

    for (auto& item :
         connections_) {

        const auto& connection =
            item.second;

        if (!connection) {
            continue;
        }

        epoll_ctl(
            epoll_fd_,
            EPOLL_CTL_DEL,
            connection->fd,
            nullptr);

        shutdown(
            connection->fd,
            SHUT_RDWR);

        close(
            connection->fd);

        std::lock_guard<std::mutex> lock(
            connection->backend_mutex);

        for (auto& shard :
             connection->backend_fds) {

            for (int& backend_fd :
                 shard) {

                if (backend_fd >= 0) {
                    shutdown(
                        backend_fd,
                        SHUT_RDWR);

                    close(
                        backend_fd);

                    backend_fd = -1;
                }
            }
        }
    }

    connections_.clear();

    {
        std::lock_guard<std::mutex> lock(
            task_mutex_);

        worker_stop_ = true;
    }

    task_cv_.notify_all();

    for (std::thread& worker :
         workers_) {

        if (worker.joinable()) {
            worker.join();
        }
    }

    workers_.clear();

    close(event_fd_);
    close(epoll_fd_);
    close(listen_fd_);

    event_fd_ = -1;
    epoll_fd_ = -1;
    listen_fd_ = -1;

    return 0;
}

} // namespace minikv