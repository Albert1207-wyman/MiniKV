#include "raft.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <random>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace minikv {

namespace {

constexpr int kElectionTimeoutMinMs = 800;
constexpr int kElectionTimeoutMaxMs = 1400;

constexpr int kHeartbeatIntervalMs = 200;

constexpr int kReceiveBufferSize = 64 * 1024;

constexpr int kProposalTimeoutMs = 3000;

constexpr char kStateHeader[] =
    "MINIKV_RAFT_STATE_V1";

constexpr char kRequestVote[] = "RV";
constexpr char kRequestVoteResponse[] = "RVR";

constexpr char kAppendEntries[] = "AE";
constexpr char kAppendEntriesResponse[] = "AER";

} // namespace

RaftNode::RaftNode(
    int id,
    uint16_t port,
    std::vector<Peer> peers,
    std::string state_file)
    : id_(id),
      port_(port),
      peers_(std::move(peers)),
      state_file_(std::move(state_file)),
      socket_fd_(-1),
      running_(false),
      state_(State::Follower),
      current_term_(0),
      voted_for_(-1),
      commit_index_(0),
      last_applied_(0),
      votes_received_(0),
      rng_(
          std::random_device{}() ^
          static_cast<unsigned int>(
              std::chrono::steady_clock::now()
                  .time_since_epoch()
                  .count()) ^
          static_cast<unsigned int>(
              id * 0x9e3779b9u)) {

    // Dummy entry.
    log_.push_back(
        LogEntry{
            0,
            0,
            ""
        });

    next_index_.resize(
        peers_.size(),
        1);

    match_index_.resize(
        peers_.size(),
        0);

    if (!LoadState()) {
        std::cout
            << "[RAFT "
            << id_
            << "] no valid persisted state, "
               "starting fresh\n";
    }

    ResetElectionDeadline();
}

RaftNode::~RaftNode() {
    Stop();
}

void RaftNode::SetApplyCallback(
    ApplyCallback callback) {

    bool need_apply = false;

    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        apply_callback_ =
            std::move(callback);

        need_apply =
            apply_callback_ &&
            last_applied_ < commit_index_;
    }

    // LoadState() runs in the constructor, before the callback can be
    // installed. Replay persisted committed entries after the KV server
    // provides the state-machine callback.
    if (need_apply) {
        if (!ApplyCommittedEntries()) {
            std::cerr
                << "[RAFT "
                << id_
                << "] failed to replay "
                   "persisted committed entries\n";
        }
    }
}

RaftNode::State RaftNode::state() const {
    std::lock_guard<std::mutex> lock(
        mutex_);

    return state_;
}

uint64_t RaftNode::current_term() const {
    std::lock_guard<std::mutex> lock(
        mutex_);

    return current_term_;
}

uint64_t RaftNode::commit_index() const {
    std::lock_guard<std::mutex> lock(
        mutex_);

    return commit_index_;
}

// --------------------------------------------------------------------------
// Persistence
// --------------------------------------------------------------------------

bool RaftNode::LoadState() {
    if (state_file_.empty()) {
        return false;
    }

    std::ifstream input(
        state_file_);

    if (!input.is_open()) {
        return false;
    }

    std::string header;

    if (!std::getline(
            input,
            header) ||
        header != kStateHeader) {

        return false;
    }

    uint64_t loaded_term = 0;
    int loaded_voted_for = -1;
    uint64_t loaded_commit_index = 0;

    std::vector<LogEntry>
        loaded_log;

    loaded_log.push_back(
        LogEntry{
            0,
            0,
            ""
        });

    std::string line;

    size_t expected_count = 0;

    try {
        if (!std::getline(
                input,
                line)) {

            return false;
        }

        {
            const auto fields =
                Split(line, ' ');

            if (fields.size() != 2 ||
                fields[0] != "term") {

                return false;
            }

            loaded_term =
                std::stoull(fields[1]);
        }

        if (!std::getline(
                input,
                line)) {

            return false;
        }

        {
            const auto fields =
                Split(line, ' ');

            if (fields.size() != 2 ||
                fields[0] != "voted_for") {

                return false;
            }

            loaded_voted_for =
                std::stoi(fields[1]);
        }

        // Newer state files contain commit_index before log_count.
        // Older files do not, so accept log_count directly for compatibility.
        if (!std::getline(
                input,
                line)) {

            return false;
        }

        {
            const auto fields =
                Split(line, ' ');

            if (fields.size() != 2) {
                return false;
            }

            if (fields[0] == "commit_index") {
                loaded_commit_index =
                    std::stoull(fields[1]);

                if (!std::getline(
                        input,
                        line)) {

                    return false;
                }

                const auto log_fields =
                    Split(line, ' ');

                if (log_fields.size() != 2 ||
                    log_fields[0] != "log_count") {

                    return false;
                }

                expected_count =
                    static_cast<size_t>(
                        std::stoull(log_fields[1]));

            } else if (fields[0] == "log_count") {

                expected_count =
                    static_cast<size_t>(
                        std::stoull(fields[1]));

            } else {
                return false;
            }
        }

        for (size_t i = 0;
             i < expected_count;
             ++i) {

            if (!std::getline(
                    input,
                    line)) {

                return false;
            }

            const auto fields =
                Split(line, ' ');

            if (fields.size() != 3) {
                return false;
            }

            const uint64_t index =
                std::stoull(fields[0]);

            const uint64_t term =
                std::stoull(fields[1]);

            const std::string command =
                HexDecode(fields[2]);

            if (index !=
                static_cast<uint64_t>(
                    i + 1)) {

                return false;
            }

            loaded_log.push_back(
                LogEntry{
                    term,
                    index,
                    command
                });
        }
    } catch (const std::exception&) {
        return false;
    }

    // Never restore a commit index beyond the recovered log.
    loaded_commit_index =
        std::min(
            loaded_commit_index,
            loaded_log.back().index);

    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        current_term_ =
            loaded_term;

        voted_for_ =
            loaded_voted_for;

        log_ =
            std::move(loaded_log);

        commit_index_ =
            loaded_commit_index;

        // State-machine replay starts from the beginning after the
        // application callback is installed.
        last_applied_ = 0;
    }

    std::cout
        << "[RAFT "
        << id_
        << "] loaded persistent state: "
        << "term="
        << loaded_term
        << " voted_for="
        << loaded_voted_for
        << " commit_index="
        << loaded_commit_index
        << " log_entries="
        << expected_count
        << "\n";

    return true;
}

bool RaftNode::PersistStateLocked() {
    if (state_file_.empty()) {
        return true;
    }

    try {
        const std::filesystem::path
            path(state_file_);

        const auto parent =
            path.parent_path();

        if (!parent.empty()) {
            std::filesystem::create_directories(
                parent);
        }

        const std::filesystem::path
            temp_path =
                state_file_ + ".tmp";

        std::ofstream output(
            temp_path,
            std::ios::trunc);

        if (!output.is_open()) {
            std::cerr
                << "[RAFT "
                << id_
                << "] failed to open state temp file\n";

            return false;
        }

        output
            << kStateHeader
            << "\n";

        output
            << "term "
            << current_term_
            << "\n";

        output
            << "voted_for "
            << voted_for_
            << "\n";

        output
            << "commit_index "
            << commit_index_
            << "\n";

        // Dummy entry index 0 is not persisted.
        const size_t real_entries =
            log_.size() > 0
                ? log_.size() - 1
                : 0;

        output
            << "log_count "
            << real_entries
            << "\n";

        for (size_t i = 1;
             i < log_.size();
             ++i) {

            const LogEntry& entry =
                log_[i];

            output
                << entry.index
                << " "
                << entry.term
                << " "
                << HexEncode(
                       entry.command)
                << "\n";
        }

        output.flush();

        if (!output.good()) {
            output.close();

            std::error_code ec;
            std::filesystem::remove(
                temp_path,
                ec);

            return false;
        }

        output.close();

        std::error_code ec;

        std::filesystem::rename(
            temp_path,
            path,
            ec);

        if (ec) {
            // Some filesystems do not replace an existing target
            // atomically through rename().
            std::filesystem::remove(
                path,
                ec);

            ec.clear();

            std::filesystem::rename(
                temp_path,
                path,
                ec);
        }

        if (ec) {
            std::cerr
                << "[RAFT "
                << id_
                << "] failed to install "
                   "persistent state: "
                << ec.message()
                << "\n";

            std::filesystem::remove(
                temp_path,
                ec);

            return false;
        }

        return true;

    } catch (const std::exception& ex) {
        std::cerr
            << "[RAFT "
            << id_
            << "] persist exception: "
            << ex.what()
            << "\n";

        return false;
    }
}

bool RaftNode::PersistState() {
    std::lock_guard<std::mutex> lock(
        mutex_);

    return PersistStateLocked();
}

// --------------------------------------------------------------------------
// Timers
// --------------------------------------------------------------------------

uint64_t RaftNode::RandomElectionTimeoutMs() {
    std::uniform_int_distribution<int>
        distribution(
            kElectionTimeoutMinMs,
            kElectionTimeoutMaxMs);

    return static_cast<uint64_t>(
        distribution(rng_));
}

void RaftNode::ResetElectionDeadline() {
    election_deadline_ =
        std::chrono::steady_clock::now() +
        std::chrono::milliseconds(
            RandomElectionTimeoutMs());
}

// --------------------------------------------------------------------------
// Utility
// --------------------------------------------------------------------------

size_t RaftNode::PeerIndex(
    int peer_id) const {

    for (size_t i = 0;
         i < peers_.size();
         ++i) {

        if (peers_[i].id ==
            peer_id) {

            return i;
        }
    }

    return peers_.size();
}

uint64_t RaftNode::LastLogIndex() const {
    return log_.back().index;
}

uint64_t RaftNode::LastLogTerm() const {
    return log_.back().term;
}

std::vector<std::string>
RaftNode::Split(
    const std::string& value,
    char delimiter) const {

    std::vector<std::string>
        result;

    std::stringstream stream(
        value);

    std::string field;

    while (std::getline(
        stream,
        field,
        delimiter)) {

        result.push_back(
            std::move(field));
    }

    return result;
}

std::string RaftNode::HexEncode(
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

std::string RaftNode::HexDecode(
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
                (high << 4) | low));
    }

    return result;
}

// --------------------------------------------------------------------------
// Start / Stop
// --------------------------------------------------------------------------

bool RaftNode::Start() {
    if (running_.exchange(true)) {
        return false;
    }

    socket_fd_ =
        socket(
            AF_INET,
            SOCK_DGRAM,
            0);

    if (socket_fd_ < 0) {
        running_ = false;

        return false;
    }

    sockaddr_in address{};

    address.sin_family =
        AF_INET;

    address.sin_addr.s_addr =
        htonl(INADDR_ANY);

    address.sin_port =
        htons(port_);

    if (bind(
            socket_fd_,
            reinterpret_cast<
                sockaddr*>(&address),
            sizeof(address)) < 0) {

        close(socket_fd_);

        socket_fd_ = -1;
        running_ = false;

        return false;
    }

    const int flags =
        fcntl(
            socket_fd_,
            F_GETFL,
            0);

    if (flags < 0 ||
        fcntl(
            socket_fd_,
            F_SETFL,
            flags | O_NONBLOCK) < 0) {

        close(socket_fd_);

        socket_fd_ = -1;
        running_ = false;

        return false;
    }

    receive_thread_ =
        std::thread(
            &RaftNode::ReceiveLoop,
            this);

    election_thread_ =
        std::thread(
            &RaftNode::ElectionLoop,
            this);

    heartbeat_thread_ =
        std::thread(
            &RaftNode::HeartbeatLoop,
            this);

    std::cout
        << "[RAFT "
        << id_
        << "] started UDP "
        << port_
        << "\n";

    return true;
}

void RaftNode::Stop() {
    if (!running_.exchange(false)) {
        return;
    }

    commit_cv_.notify_all();

    if (receive_thread_.joinable()) {
        receive_thread_.join();
    }

    if (election_thread_.joinable()) {
        election_thread_.join();
    }

    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }

    if (socket_fd_ >= 0) {
        close(socket_fd_);

        socket_fd_ = -1;
    }
}

// --------------------------------------------------------------------------
// UDP
// --------------------------------------------------------------------------

bool RaftNode::SendPacket(
    const Peer& peer,
    const std::string& packet) {

    return SendPacket(
        peer.host,
        peer.port,
        packet);
}

bool RaftNode::SendPacket(
    const std::string& host,
    uint16_t port,
    const std::string& packet) {

    // Peer.host may be either a numeric IPv4 address or a DNS name.
    // Resolve it for every send instead of caching the Pod IP, because
    // Kubernetes may recreate a Pod with the same stable DNS name but
    // a different IP address.
    addrinfo hints{};

    hints.ai_family =
        AF_INET;

    hints.ai_socktype =
        SOCK_DGRAM;

    hints.ai_protocol =
        IPPROTO_UDP;

    addrinfo* results =
        nullptr;

    const std::string service =
        std::to_string(port);

    const int rc =
        getaddrinfo(
            host.c_str(),
            service.c_str(),
            &hints,
            &results);

    if (rc != 0) {
        std::cerr
            << "[RAFT "
            << id_
            << "] resolve peer "
            << host
            << ":"
            << port
            << " failed: "
            << gai_strerror(rc)
            << "\n";

        return false;
    }

    bool sent_ok = false;

    for (addrinfo* current =
             results;
         current != nullptr;
         current =
             current->ai_next) {

        if (current->ai_addr ==
                nullptr ||
            current->ai_addrlen ==
                0) {

            continue;
        }

        const ssize_t sent =
            sendto(
                socket_fd_,
                packet.data(),
                packet.size(),
                0,
                current->ai_addr,
                current->ai_addrlen);

        if (sent ==
            static_cast<ssize_t>(
                packet.size())) {

            sent_ok = true;
            break;
        }
    }

    freeaddrinfo(results);

    return sent_ok;
}

void RaftNode::ReceiveLoop() {
    char buffer[
        kReceiveBufferSize];

    while (running_) {
        sockaddr_in source{};

        socklen_t source_len =
            sizeof(source);

        const ssize_t n =
            recvfrom(
                socket_fd_,
                buffer,
                sizeof(buffer),
                0,
                reinterpret_cast<
                    sockaddr*>(&source),
                &source_len);

        if (n > 0) {
            char ip_buffer[
                INET_ADDRSTRLEN];

            const char* ip =
                inet_ntop(
                    AF_INET,
                    &source.sin_addr,
                    ip_buffer,
                    sizeof(ip_buffer));

            HandlePacket(
                std::string(
                    buffer,
                    static_cast<size_t>(
                        n)),
                ip != nullptr
                    ? std::string(ip)
                    : std::string(
                          "127.0.0.1"),
                ntohs(
                    source.sin_port));

            continue;
        }

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }

            if (errno == EAGAIN ||
                errno == EWOULDBLOCK) {

                std::this_thread::sleep_for(
                    std::chrono::milliseconds(10));

                continue;
            }

            break;
        }
    }
}

// --------------------------------------------------------------------------
// State
// --------------------------------------------------------------------------

void RaftNode::BecomeFollower(
    uint64_t term) {

    std::lock_guard<std::mutex> lock(
        mutex_);

    if (term > current_term_) {
        current_term_ =
            term;

        voted_for_ =
            -1;

        // Persist currentTerm and votedFor before
        // responding to the remote peer.
        PersistStateLocked();
    }

    state_ =
        State::Follower;

    votes_received_ = 0;

    voted_peers_.clear();

    ResetElectionDeadline();

    commit_cv_.notify_all();

    std::cout
        << "[RAFT "
        << id_
        << "] -> FOLLOWER term="
        << current_term_
        << "\n";
}

void RaftNode::ResetLeaderState() {
    const uint64_t next =
        LastLogIndex() + 1;

    std::fill(
        next_index_.begin(),
        next_index_.end(),
        next);

    std::fill(
        match_index_.begin(),
        match_index_.end(),
        0);

    const size_t self =
        PeerIndex(id_);

    if (self <
        match_index_.size()) {

        match_index_[self] =
            LastLogIndex();
    }
}

// --------------------------------------------------------------------------
// Election
// --------------------------------------------------------------------------

void RaftNode::ElectionLoop() {
    while (running_) {
        bool timeout = false;

        {
            std::lock_guard<std::mutex> lock(
                mutex_);

            timeout =
                state_ != State::Leader &&
                std::chrono::steady_clock::now() >=
                    election_deadline_;
        }

        if (timeout) {
            StartElection();
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(5));
    }
}

void RaftNode::StartElection() {
    uint64_t term = 0;
    uint64_t last_index = 0;
    uint64_t last_term = 0;

    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        state_ =
            State::Candidate;

        ++current_term_;

        voted_for_ =
            id_;

        votes_received_ =
            1;

        voted_peers_.clear();

        voted_peers_.insert(id_);

        ResetElectionDeadline();

        if (!PersistStateLocked()) {
            std::cerr
                << "[RAFT "
                << id_
                << "] failed to persist "
                   "election state\n";
        }

        term =
            current_term_;

        last_index =
            LastLogIndex();

        last_term =
            LastLogTerm();

        std::cout
            << "[RAFT "
            << id_
            << "] -> CANDIDATE term="
            << term
            << "\n";
    }

    const std::string packet =
        BuildRequestVote(
            term,
            id_,
            last_index,
            last_term);

    for (const Peer& peer :
         peers_) {

        if (peer.id == id_) {
            continue;
        }

        SendPacket(
            peer,
            packet);
    }
}

// --------------------------------------------------------------------------
// RequestVote
// --------------------------------------------------------------------------

std::string RaftNode::BuildRequestVote(
    uint64_t term,
    int candidate_id,
    uint64_t last_log_index,
    uint64_t last_log_term) const {

    return
        std::string(kRequestVote) +
        "|" +
        std::to_string(term) +
        "|" +
        std::to_string(candidate_id) +
        "|" +
        std::to_string(last_log_index) +
        "|" +
        std::to_string(last_log_term);
}

std::string RaftNode::BuildRequestVoteResponse(
    uint64_t term,
    int voter_id,
    bool granted) const {

    return
        std::string(
            kRequestVoteResponse) +
        "|" +
        std::to_string(term) +
        "|" +
        std::to_string(voter_id) +
        "|" +
        (granted ? "1" : "0");
}

bool RaftNode::HandleRequestVote(
    const std::vector<std::string>& fields,
    std::string* response) {

    if (fields.size() != 5 ||
        response == nullptr) {

        return false;
    }

    const uint64_t term =
        std::stoull(fields[1]);

    const int candidate_id =
        std::stoi(fields[2]);

    const uint64_t candidate_last_index =
        std::stoull(fields[3]);

    const uint64_t candidate_last_term =
        std::stoull(fields[4]);

    bool granted = false;

    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        if (term < current_term_) {
            granted = false;
        } else {
            if (term > current_term_) {
                current_term_ =
                    term;

                voted_for_ =
                    -1;

                state_ =
                    State::Follower;

                PersistStateLocked();
            }

            const bool log_up_to_date =
                candidate_last_term >
                    LastLogTerm() ||
                (
                    candidate_last_term ==
                        LastLogTerm() &&
                    candidate_last_index >=
                        LastLogIndex());

            if ((voted_for_ == -1 ||
                 voted_for_ ==
                     candidate_id) &&
                log_up_to_date) {

                voted_for_ =
                    candidate_id;

                ResetElectionDeadline();

                granted = true;

                PersistStateLocked();
            }
        }

        *response =
            BuildRequestVoteResponse(
                current_term_,
                id_,
                granted);
    }

    return true;
}

bool RaftNode::HandleRequestVoteResponse(
    const std::vector<std::string>& fields) {

    if (fields.size() != 4) {
        return false;
    }

    const uint64_t term =
        std::stoull(fields[1]);

    const int voter_id =
        std::stoi(fields[2]);

    const bool granted =
        fields[3] == "1";

    std::lock_guard<std::mutex> lock(
        mutex_);

    if (term > current_term_) {
        current_term_ =
            term;

        state_ =
            State::Follower;

        voted_for_ =
            -1;

        votes_received_ =
            0;

        voted_peers_.clear();

        ResetElectionDeadline();

        PersistStateLocked();

        commit_cv_.notify_all();

        return true;
    }

    if (state_ !=
            State::Candidate ||
        term != current_term_) {

        return true;
    }

    if (!granted ||
        voted_peers_.count(
            voter_id) != 0) {

        return true;
    }

    voted_peers_.insert(
        voter_id);

    ++votes_received_;

    const size_t majority =
        peers_.size() / 2 + 1;

    if (static_cast<size_t>(
            votes_received_) >=
        majority) {

        state_ =
            State::Leader;

        ResetLeaderState();

        std::cout
            << "[RAFT "
            << id_
            << "] -> LEADER term="
            << current_term_
            << "\n";
    }

    return true;
}

// --------------------------------------------------------------------------
// AppendEntries
// --------------------------------------------------------------------------

std::string RaftNode::BuildAppendEntries(
    uint64_t term,
    int leader_id,
    uint64_t prev_log_index,
    uint64_t prev_log_term,
    uint64_t leader_commit,
    const std::vector<LogEntry>& entries) const {

    std::string packet =
        std::string(kAppendEntries) +
        "|" +
        std::to_string(term) +
        "|" +
        std::to_string(leader_id) +
        "|" +
        std::to_string(prev_log_index) +
        "|" +
        std::to_string(prev_log_term) +
        "|" +
        std::to_string(leader_commit);

    for (const LogEntry& entry :
         entries) {

        packet +=
            "|" +
            std::to_string(entry.index) +
            "," +
            std::to_string(entry.term) +
            "," +
            HexEncode(
                entry.command);
    }

    return packet;
}

std::string RaftNode::BuildAppendEntriesResponse(
    uint64_t term,
    int follower_id,
    bool success,
    uint64_t match_index) const {

    return
        std::string(
            kAppendEntriesResponse) +
        "|" +
        std::to_string(term) +
        "|" +
        std::to_string(follower_id) +
        "|" +
        (success ? "1" : "0") +
        "|" +
        std::to_string(match_index);
}

void RaftNode::SendAppendEntries(
    const Peer& peer) {

    uint64_t term = 0;
    uint64_t prev_log_index = 0;
    uint64_t prev_log_term = 0;
    uint64_t leader_commit = 0;

    std::vector<LogEntry>
        entries;

    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        if (state_ !=
            State::Leader) {

            return;
        }

        const size_t peer_index =
            PeerIndex(
                peer.id);

        if (peer_index >=
            next_index_.size()) {

            return;
        }

        uint64_t next =
            next_index_[peer_index];

        if (next == 0) {
            next = 1;
        }

        prev_log_index =
            next - 1;

        if (prev_log_index <
            log_.size()) {

            prev_log_term =
                log_[
                    static_cast<size_t>(
                        prev_log_index)]
                    .term;
        }

        for (uint64_t index = next;
             index <= LastLogIndex() &&
             index < log_.size();
             ++index) {

            entries.push_back(
                log_[
                    static_cast<size_t>(
                        index)]);
        }

        term =
            current_term_;

        leader_commit =
            commit_index_;
    }

    const std::string packet =
        BuildAppendEntries(
            term,
            id_,
            prev_log_index,
            prev_log_term,
            leader_commit,
            entries);

    SendPacket(
        peer,
        packet);
}

void RaftNode::RetryAppendEntries(
    const Peer& peer) {

    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        const size_t peer_index =
            PeerIndex(
                peer.id);

        if (peer_index >=
            next_index_.size()) {

            return;
        }

        if (next_index_[peer_index] >
            1) {

            --next_index_[peer_index];
        }
    }

    SendAppendEntries(
        peer);
}

bool RaftNode::HandleAppendEntries(
    const std::vector<std::string>& fields,
    std::string* response) {

    if (fields.size() < 6 ||
        response == nullptr) {

        return false;
    }

    const uint64_t term =
        std::stoull(fields[1]);

    const int leader_id =
        std::stoi(fields[2]);

    const uint64_t prev_log_index =
        std::stoull(fields[3]);

    const uint64_t prev_log_term =
        std::stoull(fields[4]);

    const uint64_t leader_commit =
        std::stoull(fields[5]);

    (void)leader_id;

    bool success = false;
    uint64_t match_index = 0;
    bool need_apply = false;

    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        if (term < current_term_) {

            *response =
                BuildAppendEntriesResponse(
                    current_term_,
                    id_,
                    false,
                    0);

            return true;
        }

        if (term > current_term_) {
            current_term_ =
                term;

            voted_for_ =
                -1;

            state_ =
                State::Follower;

            PersistStateLocked();
        }

        state_ =
            State::Follower;

        ResetElectionDeadline();

        if (prev_log_index >=
            log_.size()) {

            success = false;

        } else if (
            log_[
                static_cast<size_t>(
                    prev_log_index)]
                    .term !=
                prev_log_term) {

            success = false;

        } else {
            success = true;

            for (size_t i = 6;
                 i < fields.size();
                 ++i) {

                const auto parts =
                    Split(
                        fields[i],
                        ',');

                if (parts.size() != 3) {
                    continue;
                }

                const uint64_t
                    entry_index =
                        std::stoull(
                            parts[0]);

                const uint64_t
                    entry_term =
                        std::stoull(
                            parts[1]);

                const std::string
                    command =
                        HexDecode(
                            parts[2]);

                if (entry_index <
                    log_.size()) {

                    LogEntry& existing =
                        log_[
                            static_cast<size_t>(
                                entry_index)];

                    if (existing.term !=
                            entry_term ||
                        existing.command !=
                            command) {

                        log_.resize(
                            static_cast<size_t>(
                                entry_index));
                    }
                }

                if (entry_index >=
                    log_.size()) {

                    log_.push_back(
                        LogEntry{
                            entry_term,
                            entry_index,
                            command
                        });
                }
            }

            match_index =
                LastLogIndex();

            const uint64_t old_commit =
                commit_index_;

            if (leader_commit >
                commit_index_) {

                commit_index_ =
                    std::min(
                        leader_commit,
                        LastLogIndex());
            }

            need_apply =
                commit_index_ >
                old_commit;

            // Persist both the replicated log and any newly learned
            // commit index. This is required so a follower restart can
            // reconstruct its state machine from committed entries.
            if (!PersistStateLocked()) {
                std::cerr
                    << "[RAFT "
                    << id_
                    << "] failed to persist "
                       "replicated log/commit index\n";
            }
        }

        *response =
            BuildAppendEntriesResponse(
                current_term_,
                id_,
                success,
                success
                    ? match_index
                    : 0);
    }

    if (need_apply) {
        ApplyCommittedEntries();
    }

    return true;
}

bool RaftNode::HandleAppendEntriesResponse(
    const std::vector<std::string>& fields) {

    if (fields.size() != 5) {
        return false;
    }

    const uint64_t term =
        std::stoull(fields[1]);

    const int follower_id =
        std::stoi(fields[2]);

    const bool success =
        fields[3] == "1";

    const uint64_t match_index =
        std::stoull(fields[4]);

    bool need_apply = false;

    Peer retry_peer;

    bool need_retry = false;

    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        if (term > current_term_) {
            current_term_ =
                term;

            state_ =
                State::Follower;

            voted_for_ =
                -1;

            ResetElectionDeadline();

            PersistStateLocked();

            commit_cv_.notify_all();

            return true;
        }

        if (state_ !=
                State::Leader ||
            term != current_term_) {

            return true;
        }

        const size_t peer_index =
            PeerIndex(
                follower_id);

        if (peer_index >=
                peers_.size() ||
            follower_id == id_) {

            return true;
        }

        if (success) {
            match_index_[peer_index] =
                std::max(
                    match_index_[peer_index],
                    match_index);

            next_index_[peer_index] =
                match_index_[peer_index] + 1;

            const uint64_t old_commit =
                commit_index_;

            TryAdvanceCommitIndex();

            need_apply =
                commit_index_ >
                old_commit;

        } else {
            if (next_index_[peer_index] >
                1) {

                --next_index_[peer_index];
            }

            retry_peer =
                peers_[peer_index];

            need_retry = true;
        }
    }

    if (need_apply) {
        ApplyCommittedEntries();
    }

    if (need_retry) {
        SendAppendEntries(
            retry_peer);
    }

    return true;
}

// --------------------------------------------------------------------------
// Commit / Apply
// --------------------------------------------------------------------------

void RaftNode::TryAdvanceCommitIndex() {
    if (state_ !=
        State::Leader) {

        return;
    }

    const uint64_t old_commit =
        commit_index_;

    const size_t majority =
        peers_.size() / 2 + 1;

    for (uint64_t index =
             commit_index_ + 1;
         index <= LastLogIndex();
         ++index) {

        size_t replicated =
            0;

        for (uint64_t value :
             match_index_) {

            if (value >= index) {
                ++replicated;
            }
        }

        if (replicated >= majority &&
            log_[
                static_cast<size_t>(
                    index)]
                    .term ==
                current_term_) {

            commit_index_ =
                index;
        }
    }

    if (commit_index_ >
        old_commit) {

        // A committed index is part of the durable state needed by the
        // application state machine to recover after a restart.
        if (!PersistStateLocked()) {
            std::cerr
                << "[RAFT "
                << id_
                << "] failed to persist "
                   "commit index\n";
        }
    }

    commit_cv_.notify_all();
}

bool RaftNode::ApplyCommittedEntries() {
    while (true) {
        LogEntry entry;
        ApplyCallback callback;

        {
            std::lock_guard<std::mutex> lock(
                mutex_);

            if (last_applied_ >=
                commit_index_) {

                return true;
            }

            ++last_applied_;

            entry =
                log_[
                    static_cast<size_t>(
                        last_applied_)];

            callback =
                apply_callback_;
        }

        if (callback) {
            if (!callback(
                    entry.command)) {

                std::cerr
                    << "[RAFT "
                    << id_
                    << "] apply failed "
                       "index="
                    << entry.index
                    << "\n";

                return false;
            }
        }

        std::cout
            << "[RAFT "
            << id_
            << "] applied index="
            << entry.index
            << " term="
            << entry.term
            << "\n";

        commit_cv_.notify_all();
    }
}

// --------------------------------------------------------------------------
// Heartbeat
// --------------------------------------------------------------------------

void RaftNode::HeartbeatLoop() {
    while (running_) {
        if (state() ==
            State::Leader) {

            SendHeartbeats();
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(
                kHeartbeatIntervalMs));
    }
}

void RaftNode::SendHeartbeats() {
    for (const Peer& peer :
         peers_) {

        if (peer.id == id_) {
            continue;
        }

        SendAppendEntries(
            peer);
    }
}

// --------------------------------------------------------------------------
// Proposal
// --------------------------------------------------------------------------

bool RaftNode::Propose(
    const std::string& command) {

    uint64_t proposal_index = 0;
    uint64_t proposal_term = 0;

    std::vector<Peer>
        peers;

    {
        std::lock_guard<std::mutex> lock(
            mutex_);

        if (state_ !=
            State::Leader) {

            std::cout
                << "[RAFT "
                << id_
                << "] reject proposal: "
                   "not leader\n";

            return false;
        }

        proposal_term =
            current_term_;

        proposal_index =
            LastLogIndex() + 1;

        log_.push_back(
            LogEntry{
                proposal_term,
                proposal_index,
                command
            });

        const size_t self =
            PeerIndex(id_);

        if (self <
            match_index_.size()) {

            match_index_[self] =
                proposal_index;
        }

        if (!PersistStateLocked()) {
            std::cerr
                << "[RAFT "
                << id_
                << "] failed to persist "
                   "new log entry\n";

            log_.pop_back();

            match_index_[self] =
                LastLogIndex();

            return false;
        }

        peers =
            peers_;

        std::cout
            << "[RAFT "
            << id_
            << "] propose index="
            << proposal_index
            << " term="
            << proposal_term
            << "\n";
    }

    for (const Peer& peer :
         peers) {

        if (peer.id == id_) {
            continue;
        }

        SendAppendEntries(
            peer);
    }

    ApplyCommittedEntries();

    std::unique_lock<std::mutex> lock(
        mutex_);

    const bool finished =
        commit_cv_.wait_for(
            lock,
            std::chrono::milliseconds(
                kProposalTimeoutMs),
            [this,
             proposal_index,
             proposal_term] {

                return
                    !running_ ||
                    commit_index_ >=
                        proposal_index ||
                    state_ !=
                        State::Leader ||
                    current_term_ !=
                        proposal_term;
            });

    if (!finished) {
        return false;
    }

    return
        running_ &&
        commit_index_ >=
            proposal_index &&
        last_applied_ >=
            proposal_index;
}

// --------------------------------------------------------------------------
// Packet Dispatch
// --------------------------------------------------------------------------

void RaftNode::HandlePacket(
    const std::string& packet,
    const std::string& source_ip,
    uint16_t source_port) {

    const auto fields =
        Split(
            packet,
            '|');

    if (fields.empty()) {
        return;
    }

    try {
        if (fields[0] ==
            kRequestVote) {

            std::string response;

            if (HandleRequestVote(
                    fields,
                    &response)) {

                SendPacket(
                    source_ip,
                    source_port,
                    response);
            }

            return;
        }

        if (fields[0] ==
            kRequestVoteResponse) {

            HandleRequestVoteResponse(
                fields);

            return;
        }

        if (fields[0] ==
            kAppendEntries) {

            std::string response;

            if (HandleAppendEntries(
                    fields,
                    &response)) {

                SendPacket(
                    source_ip,
                    source_port,
                    response);
            }

            return;
        }

        if (fields[0] ==
            kAppendEntriesResponse) {

            const int follower_id =
                fields.size() >= 3
                    ? std::stoi(fields[2])
                    : -1;

            const bool success =
                fields.size() >= 4 &&
                fields[3] == "1";

            HandleAppendEntriesResponse(
                fields);

            if (!success &&
                follower_id >= 0) {

                const size_t index =
                    PeerIndex(
                        follower_id);

                if (index <
                    peers_.size()) {

                    RetryAppendEntries(
                        peers_[index]);
                }
            }

            return;
        }

    } catch (
        const std::exception& ex) {

        std::cerr
            << "[RAFT "
            << id_
            << "] invalid packet: "
            << ex.what()
            << "\n";
    }
}

} // namespace minikv
