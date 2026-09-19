#include "kv_server.h"

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
#include <sstream>
#include <utility>

namespace minikv {

namespace {

constexpr int kBacklog = 512;
constexpr int kMaxEvents = 1024;
constexpr int kPollTimeoutMs = 1000;

constexpr size_t kDefaultWorkerCount = 4;

constexpr size_t kMaxInputBuffer =
    static_cast<size_t>(
        kvproto::kMaxFrameSize) *
    2;

} // namespace

KvServer::KvServer(
    DB* db,
    std::string host,
    uint16_t port,
    int raft_id,
    uint16_t raft_port,
    std::vector<RaftNode::Peer> raft_peers,
    std::string raft_state_file)
    : db_(db),
      host_(std::move(host)),
      port_(port),
      raft_id_(raft_id),
      raft_port_(raft_port),
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

    if (raft_id_ >= 0 &&
        raft_port_ != 0) {

        raft_ =
            std::make_unique<
                RaftNode>(
                    raft_id_,
                    raft_port_,
                    std::move(
                        raft_peers),
                    std::move(
                        raft_state_file));
    }
}

KvServer::~KvServer() {
    Stop();

    if (raft_) {
        raft_->Stop();
    }
}

void KvServer::RequestStop() noexcept {
    stop_requested_ = 1;

    if (raft_) {
        raft_->Stop();
    }
}

void KvServer::Stop() {
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

bool KvServer::SetSocketOptions(int fd) {
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

bool KvServer::IsCurrentConnection(
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

// --------------------------------------------------------------------------
// Worker
// --------------------------------------------------------------------------

void KvServer::WorkerLoop() {
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

        std::string response =
            HandleRequest(
                task.request_body);

        PublishResponse(
            task.connection,
            std::move(response));
    }
}

void KvServer::EnqueueRequest(
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

// --------------------------------------------------------------------------
// Hex
// --------------------------------------------------------------------------

std::string KvServer::HexEncode(
    const std::string& value) const {

    static constexpr char hex[] =
        "0123456789abcdef";

    std::string result;

    result.reserve(
        value.size() * 2);

    for (unsigned char c :
         value) {

        result.push_back(
            hex[(c >> 4) & 0xf]);

        result.push_back(
            hex[c & 0xf]);
    }

    return result;
}

std::string KvServer::HexDecode(
    const std::string& value) const {

    if (value.size() % 2 != 0) {
        return {};
    }

    auto hex_value =
        [](char c) -> int {

        if (c >= '0' &&
            c <= '9') {

            return c - '0';
        }

        if (c >= 'a' &&
            c <= 'f') {

            return c - 'a' + 10;
        }

        if (c >= 'A' &&
            c <= 'F') {

            return c - 'A' + 10;
        }

        return -1;
    };

    std::string result;

    result.reserve(
        value.size() / 2);

    for (size_t i = 0;
         i < value.size();
         i += 2) {

        const int high =
            hex_value(
                value[i]);

        const int low =
            hex_value(
                value[i + 1]);

        if (high < 0 ||
            low < 0) {

            return {};
        }

        result.push_back(
            static_cast<char>(
                (high << 4) |
                low));
    }

    return result;
}

// --------------------------------------------------------------------------
// Raft commands
// --------------------------------------------------------------------------

std::string KvServer::EncodePutCommand(
    const std::string& key,
    const std::string& value) const {

    return
        "P|" +
        HexEncode(key) +
        "|" +
        HexEncode(value);
}

std::string KvServer::EncodeDeleteCommand(
    const std::string& key) const {

    return
        "D|" +
        HexEncode(key);
}

bool KvServer::DecodePutCommand(
    const std::string& command,
    std::string* key,
    std::string* value) const {

    if (key == nullptr ||
        value == nullptr) {

        return false;
    }

    std::stringstream stream(
        command);

    std::vector<std::string>
        parts;

    std::string item;

    while (std::getline(
        stream,
        item,
        '|')) {

        parts.push_back(
            std::move(item));
    }

    if (parts.size() != 3 ||
        parts[0] != "P") {

        return false;
    }

    *key =
        HexDecode(parts[1]);

    *value =
        HexDecode(parts[2]);

    return !key->empty();
}

bool KvServer::DecodeDeleteCommand(
    const std::string& command,
    std::string* key) const {

    if (key == nullptr) {
        return false;
    }

    constexpr const char* prefix =
        "D|";

    if (command.rfind(
            prefix,
            0) != 0) {

        return false;
    }

    *key =
        HexDecode(
            command.substr(2));

    return !key->empty();
}

// --------------------------------------------------------------------------
// Apply
// --------------------------------------------------------------------------

bool KvServer::ApplyRaftCommand(
    const std::string& command) {

    if (command.rfind(
            "P|",
            0) == 0) {

        std::string key;
        std::string value;

        if (!DecodePutCommand(
                command,
                &key,
                &value)) {

            return false;
        }

        WriteOptions options;

        const Status status =
            db_->Put(
                options,
                Slice(key),
                Slice(value));

        if (!status.ok()) {

            std::cerr
                << "[KV] apply PUT failed: "
                << status.ToString()
                << "\n";

            return false;
        }

        std::cout
            << "[KV] applied PUT key="
            << key
            << "\n";

        return true;
    }

    if (command.rfind(
            "D|",
            0) == 0) {

        std::string key;

        if (!DecodeDeleteCommand(
                command,
                &key)) {

            return false;
        }

        WriteOptions options;

        const Status status =
            db_->Delete(
                options,
                Slice(key));

        if (!status.ok()) {

            std::cerr
                << "[KV] apply DELETE failed: "
                << status.ToString()
                << "\n";

            return false;
        }

        std::cout
            << "[KV] applied DELETE key="
            << key
            << "\n";

        return true;
    }

    return false;
}

// --------------------------------------------------------------------------
// Proposal
// --------------------------------------------------------------------------

bool KvServer::ProposePut(
    const std::string& key,
    const std::string& value) {

    if (!raft_) {

        WriteOptions options;

        return db_->Put(
                   options,
                   Slice(key),
                   Slice(value))
            .ok();
    }

    if (raft_->state() !=
        RaftNode::State::Leader) {

        return false;
    }

    return raft_->Propose(
        EncodePutCommand(
            key,
            value));
}

bool KvServer::ProposeDelete(
    const std::string& key) {

    if (!raft_) {

        WriteOptions options;

        return db_->Delete(
                   options,
                   Slice(key))
            .ok();
    }

    if (raft_->state() !=
        RaftNode::State::Leader) {

        return false;
    }

    return raft_->Propose(
        EncodeDeleteCommand(
            key));
}

// --------------------------------------------------------------------------
// Request
// --------------------------------------------------------------------------

std::string KvServer::HandleRequest(
    const std::string& body) {

    kvproto::Request request;

    if (!kvproto::DecodeRequest(
            body,
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

    if (raft_ &&
        raft_->state() !=
            RaftNode::State::Leader) {

        return kvproto::EncodeResponse(
            kvproto::Response{
                kvproto::StatusCode::kError,
                "NOT_LEADER"
            });
    }

    switch (request.command) {
        case kvproto::Command::kPut: {

            if (!ProposePut(
                    request.key,
                    request.value)) {

                if (raft_ &&
                    raft_->state() !=
                        RaftNode::State::Leader) {

                    return kvproto::EncodeResponse(
                        kvproto::Response{
                            kvproto::StatusCode::kError,
                            "NOT_LEADER"
                        });
                }

                return kvproto::EncodeResponse(
                    kvproto::Response{
                        kvproto::StatusCode::kError,
                        "REPLICATION_TIMEOUT"
                    });
            }

            return kvproto::EncodeResponse(
                kvproto::Response{
                    kvproto::StatusCode::kOk,
                    "OK"
                });
        }

        case kvproto::Command::kDelete: {

            if (!ProposeDelete(
                    request.key)) {

                if (raft_ &&
                    raft_->state() !=
                        RaftNode::State::Leader) {

                    return kvproto::EncodeResponse(
                        kvproto::Response{
                            kvproto::StatusCode::kError,
                            "NOT_LEADER"
                        });
                }

                return kvproto::EncodeResponse(
                    kvproto::Response{
                        kvproto::StatusCode::kError,
                        "REPLICATION_TIMEOUT"
                    });
            }

            return kvproto::EncodeResponse(
                kvproto::Response{
                    kvproto::StatusCode::kOk,
                    "OK"
                });
        }

        case kvproto::Command::kGet: {

            ReadOptions options;

            std::string value;

            const Status status =
                db_->Get(
                    options,
                    Slice(request.key),
                    &value);

            if (status.ok()) {

                return kvproto::EncodeResponse(
                    kvproto::Response{
                        kvproto::StatusCode::kOk,
                        value
                    });
            }

            return kvproto::EncodeResponse(
                kvproto::Response{
                    kvproto::StatusCode::kNotFound,
                    status.ToString()
                });
        }

        case kvproto::Command::kReplicaPut:
        case kvproto::Command::kReplicaDelete:

            return kvproto::EncodeResponse(
                kvproto::Response{
                    kvproto::StatusCode::kError,
                    "legacy replication command disabled"
                });

        case kvproto::Command::kPing:
            break;
    }

    return kvproto::EncodeResponse(
        kvproto::Response{
            kvproto::StatusCode::kError,
            "unknown command"
        });
}

// --------------------------------------------------------------------------
// Worker -> Reactor
// --------------------------------------------------------------------------

void KvServer::NotifyReactor(
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

void KvServer::PublishResponse(
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

void KvServer::DrainNotifications() {

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
// Read
// --------------------------------------------------------------------------

bool KvServer::ReadFromSocket(
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

bool KvServer::ParseFrames(
    const std::shared_ptr<ConnectionState>& connection) {

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
            std::move(
                request_body));
    }

    return true;
}

void KvServer::HandleClientRead(
    const std::shared_ptr<ConnectionState>& connection) {

    if (!ReadFromSocket(
            connection)) {

        CloseConnection(
            connection);
    }
}

// --------------------------------------------------------------------------
// Write
// --------------------------------------------------------------------------

bool KvServer::WriteToSocket(
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

void KvServer::HandleClientWrite(
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

void KvServer::UpdateConnectionEvents(
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

void KvServer::CloseConnection(
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

    connections_.erase(it);

    std::cout
        << "[INFO] Client disconnected fd="
        << connection->fd
        << "\n";
}

// --------------------------------------------------------------------------
// Start
// --------------------------------------------------------------------------

int KvServer::Start() {

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
        htons(port_);

    if (inet_pton(
            AF_INET,
            host_.c_str(),
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
            &KvServer::WorkerLoop,
            this);
    }

    if (raft_) {

        raft_->SetApplyCallback(
            [this](
                const std::string& command) {

                return ApplyRaftCommand(
                    command);
            });

        if (!raft_->Start()) {

            std::cerr
                << "[ERROR] Raft start failed\n";

            return 1;
        }
    }

    stop_requested_ = 0;

    std::cout
        << "========================================\n"
        << " MiniKV Server V4\n"
        << " Listen  : "
        << host_
        << ":"
        << port_
        << "\n"
        << " Protocol: binary framed TCP\n"
        << " Reactor : epoll\n"
        << " Workers : "
        << worker_count_
        << "\n";

    if (raft_) {
        std::cout
            << " Raft ID : "
            << raft_id_
            << "\n"
            << " Raft UDP: "
            << raft_port_
            << "\n";
    } else {
        std::cout
            << " Raft    : disabled\n";
    }

    std::cout
        << "========================================\n";

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
                        << "[INFO] Client connected fd="
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

    if (raft_) {
        raft_->Stop();
    }

    close(event_fd_);
    close(epoll_fd_);
    close(listen_fd_);

    event_fd_ = -1;
    epoll_fd_ = -1;
    listen_fd_ = -1;

    return 0;
}

} // namespace minikv