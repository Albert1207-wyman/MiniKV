#pragma once

#include <array>
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

namespace minikv {

class KvRouter {
public:
    struct Backend {
        std::string host;
        uint16_t port;
    };

    static constexpr size_t kShardCount = 3;
    static constexpr size_t kReplicaCount = 3;

    using ShardGroup =
        std::array<Backend, kReplicaCount>;

    KvRouter(
        std::array<ShardGroup, kShardCount> shards,
        std::string listen_host,
        uint16_t listen_port);

    ~KvRouter();

    KvRouter(const KvRouter&) = delete;
    KvRouter& operator=(const KvRouter&) = delete;

    int Start();

    void Stop();

    void RequestStop() noexcept;

private:
    struct ConnectionState {
        explicit ConnectionState(int client_fd)
            : fd(client_fd) {

            for (auto& shard :
                 backend_fds) {

                shard.fill(-1);
            }

            preferred_replica.fill(0);
        }

        const int fd;

        std::string input;

        std::mutex request_mutex;

        std::mutex output_mutex;

        struct OutputBuffer {
            std::string data;
            size_t offset = 0;
        };

        std::deque<OutputBuffer> output;

        // One persistent backend connection for each
        // replica candidate of each shard.
        std::array<
            std::array<int, kReplicaCount>,
            kShardCount>
            backend_fds;

        // Last known preferred replica for each shard.
        std::array<size_t, kShardCount>
            preferred_replica;

        std::mutex backend_mutex;
    };

    struct Task {
        std::shared_ptr<ConnectionState> connection;
        std::string request_body;
    };

private:
    void WorkerLoop();

    void EnqueueRequest(
        const std::shared_ptr<ConnectionState>& connection,
        std::string request_body);

    std::string HandleRequest(
        const std::shared_ptr<ConnectionState>& connection,
        const std::string& request_body);

    bool ForwardToBackend(
        const std::shared_ptr<ConnectionState>& connection,
        size_t shard,
        const std::string& request_body,
        std::string* response_body);

    int ConnectBackend(
        size_t shard,
        size_t replica);

    bool ReadFull(
        int fd,
        void* buffer,
        size_t size);

    bool WriteFull(
        int fd,
        const void* buffer,
        size_t size);

    bool SendFrame(
        int fd,
        const std::string& body);

    bool ReadFrame(
        int fd,
        std::string* body);

    size_t SelectShard(
        const std::string& key) const;

    void PublishResponse(
        const std::shared_ptr<ConnectionState>& connection,
        std::string response_body);

    void NotifyReactor(
        const std::shared_ptr<ConnectionState>& connection);

    void DrainNotifications();

    void HandleClientRead(
        const std::shared_ptr<ConnectionState>& connection);

    void HandleClientWrite(
        const std::shared_ptr<ConnectionState>& connection);

    bool ReadFromSocket(
        const std::shared_ptr<ConnectionState>& connection);

    bool ParseFrames(
        const std::shared_ptr<ConnectionState>& connection);

    bool WriteToSocket(
        const std::shared_ptr<ConnectionState>& connection);

    void UpdateConnectionEvents(
        const std::shared_ptr<ConnectionState>& connection);

    void CloseConnection(
        const std::shared_ptr<ConnectionState>& connection);

    bool IsCurrentConnection(
        const std::shared_ptr<ConnectionState>& connection) const;

    bool SetSocketOptions(int fd);

private:
    std::array<
        ShardGroup,
        kShardCount>
        shards_;

    std::string listen_host_;
    uint16_t listen_port_;

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