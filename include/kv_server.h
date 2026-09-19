#pragma once

#include <condition_variable>
#include <cstdint>
#include <csignal>
#include <deque>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "db.h"
#include "kv_protocol.h"
#include "raft.h"

namespace minikv {

class KvServer {
public:
    KvServer(
        DB* db,
        std::string host,
        uint16_t port,
        int raft_id = -1,
        uint16_t raft_port = 0,
        std::vector<RaftNode::Peer> raft_peers = {},
        std::string raft_state_file = {});

    ~KvServer();

    KvServer(const KvServer&) = delete;
    KvServer& operator=(const KvServer&) = delete;

    int Start();

    void Stop();

    void RequestStop() noexcept;

private:
    struct ConnectionState {
        explicit ConnectionState(int client_fd)
            : fd(client_fd) {
        }

        const int fd;

        std::string input;

        std::mutex output_mutex;

        struct OutputBuffer {
            std::string data;
            size_t offset = 0;
        };

        std::deque<OutputBuffer> output;
    };

    struct Task {
        std::shared_ptr<ConnectionState> connection;
        std::string request_body;
    };

private:
    void WorkerLoop();

    void HandleClientRead(
        const std::shared_ptr<ConnectionState>& connection);

    void HandleClientWrite(
        const std::shared_ptr<ConnectionState>& connection);

    void CloseConnection(
        const std::shared_ptr<ConnectionState>& connection);

    void EnqueueRequest(
        const std::shared_ptr<ConnectionState>& connection,
        std::string request_body);

    void PublishResponse(
        const std::shared_ptr<ConnectionState>& connection,
        std::string response_body);

    void NotifyReactor(
        const std::shared_ptr<ConnectionState>& connection);

    void DrainNotifications();

    void UpdateConnectionEvents(
        const std::shared_ptr<ConnectionState>& connection);

    std::string HandleRequest(
        const std::string& body);

    bool ApplyRaftCommand(
        const std::string& command);

    bool ProposePut(
        const std::string& key,
        const std::string& value);

    bool ProposeDelete(
        const std::string& key);

    std::string EncodePutCommand(
        const std::string& key,
        const std::string& value) const;

    std::string EncodeDeleteCommand(
        const std::string& key) const;

    bool DecodePutCommand(
        const std::string& command,
        std::string* key,
        std::string* value) const;

    bool DecodeDeleteCommand(
        const std::string& command,
        std::string* key) const;

    std::string HexEncode(
        const std::string& value) const;

    std::string HexDecode(
        const std::string& value) const;

    bool SetSocketOptions(int fd);

    bool ReadFromSocket(
        const std::shared_ptr<ConnectionState>& connection);

    bool ParseFrames(
        const std::shared_ptr<ConnectionState>& connection);

    bool WriteToSocket(
        const std::shared_ptr<ConnectionState>& connection);

    bool IsCurrentConnection(
        const std::shared_ptr<ConnectionState>& connection) const;

private:
    DB* db_;

    std::string host_;
    uint16_t port_;

    int raft_id_;
    uint16_t raft_port_;

    std::unique_ptr<RaftNode> raft_;

    int listen_fd_;
    int epoll_fd_;
    int event_fd_;

    volatile std::sig_atomic_t stop_requested_;

    size_t worker_count_;

    std::vector<std::thread> workers_;

    std::mutex task_mutex_;
    std::condition_variable task_cv_;
    std::queue<Task> tasks_;
    bool worker_stop_;

    std::mutex notify_mutex_;

    std::queue<
        std::shared_ptr<ConnectionState>>
        notified_connections_;

    std::unordered_map<
        int,
        std::shared_ptr<ConnectionState>>
        connections_;
};

} // namespace minikv