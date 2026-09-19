#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace minikv {

class RaftNode {
public:
    enum class State {
        Follower,
        Candidate,
        Leader
    };

    struct Peer {
        int id = -1;
        std::string host;
        uint16_t port = 0;
    };

    struct LogEntry {
        uint64_t term = 0;
        uint64_t index = 0;
        std::string command;
    };

    using ApplyCallback =
        std::function<bool(const std::string& command)>;

    RaftNode(
        int id,
        uint16_t port,
        std::vector<Peer> peers,
        std::string state_file);

    ~RaftNode();

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    bool Start();

    void Stop();

    bool Propose(
        const std::string& command);

    void SetApplyCallback(
        ApplyCallback callback);

    int id() const noexcept {
        return id_;
    }

    uint16_t port() const noexcept {
        return port_;
    }

    State state() const;

    uint64_t current_term() const;

    uint64_t commit_index() const;

private:
    void ElectionLoop();

    void HeartbeatLoop();

    void ReceiveLoop();

    void HandlePacket(
        const std::string& packet,
        const std::string& source_ip,
        uint16_t source_port);

    bool SendPacket(
        const Peer& peer,
        const std::string& packet);

    bool SendPacket(
        const std::string& host,
        uint16_t port,
        const std::string& packet);

    void StartElection();

    void BecomeFollower(
        uint64_t term);

    void ResetLeaderState();

    void SendHeartbeats();

    void SendAppendEntries(
        const Peer& peer);

    void RetryAppendEntries(
        const Peer& peer);

    bool HandleRequestVote(
        const std::vector<std::string>& fields,
        std::string* response);

    bool HandleRequestVoteResponse(
        const std::vector<std::string>& fields);

    bool HandleAppendEntries(
        const std::vector<std::string>& fields,
        std::string* response);

    bool HandleAppendEntriesResponse(
        const std::vector<std::string>& fields);

    std::string BuildRequestVote(
        uint64_t term,
        int candidate_id,
        uint64_t last_log_index,
        uint64_t last_log_term) const;

    std::string BuildRequestVoteResponse(
        uint64_t term,
        int voter_id,
        bool granted) const;

    std::string BuildAppendEntries(
        uint64_t term,
        int leader_id,
        uint64_t prev_log_index,
        uint64_t prev_log_term,
        uint64_t leader_commit,
        const std::vector<LogEntry>& entries) const;

    std::string BuildAppendEntriesResponse(
        uint64_t term,
        int follower_id,
        bool success,
        uint64_t match_index) const;

    std::vector<std::string> Split(
        const std::string& value,
        char delimiter) const;

    std::string HexEncode(
        const std::string& value) const;

    std::string HexDecode(
        const std::string& value) const;

    uint64_t RandomElectionTimeoutMs();

    uint64_t LastLogIndex() const;

    uint64_t LastLogTerm() const;

    size_t PeerIndex(
        int peer_id) const;

    void ResetElectionDeadline();

    void TryAdvanceCommitIndex();

    bool ApplyCommittedEntries();

    bool LoadState();

    bool PersistState();

    bool PersistStateLocked();

private:
    int id_;
    uint16_t port_;

    std::vector<Peer> peers_;

    // Persistent Raft state.
    std::string state_file_;

    // UDP control-plane socket.
    int socket_fd_;

    std::atomic<bool> running_;

    mutable std::mutex mutex_;

    State state_;

    // Persistent.
    uint64_t current_term_;

    // Persistent.
    int voted_for_;

    // Persistent.
    std::vector<LogEntry> log_;

    // Volatile.
    uint64_t commit_index_;

    // Volatile.
    uint64_t last_applied_;

    // Volatile leader state.
    std::vector<uint64_t> next_index_;
    std::vector<uint64_t> match_index_;

    int votes_received_;

    std::unordered_set<int>
        voted_peers_;

    std::chrono::steady_clock::time_point
        election_deadline_;

    std::mt19937 rng_;

    ApplyCallback apply_callback_;

    std::condition_variable
        commit_cv_;

    std::thread receive_thread_;

    std::thread election_thread_;

    std::thread heartbeat_thread_;
};

} // namespace minikv