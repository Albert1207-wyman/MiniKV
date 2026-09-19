#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "raft.h"

namespace {

minikv::RaftNode* g_raft = nullptr;

void HandleSignal(int) {
    if (g_raft != nullptr) {
        g_raft->Stop();
    }
}

bool ParseInt(const char* text, int* value) {
    if (text == nullptr || value == nullptr) {
        return false;
    }

    char* end = nullptr;
    const long parsed = std::strtol(text, &end, 10);

    if (end == text || *end != '\0') {
        return false;
    }

    *value = static_cast<int>(parsed);
    return true;
}

bool ParsePort(const char* text, uint16_t* port) {
    if (text == nullptr || port == nullptr) {
        return false;
    }

    char* end = nullptr;
    const long parsed = std::strtol(text, &end, 10);

    if (end == text || *end != '\0' ||
        parsed <= 0 || parsed > 65535) {
        return false;
    }

    *port = static_cast<uint16_t>(parsed);
    return true;
}

void PrintUsage(const char* program) {
    std::cout
        << "Usage:\n"
        << "  " << program
        << " <id> <raft_port> "
           "<peer0_port> <peer1_port> <peer2_port> "
           "[state_file]\n\n"

        << "Example:\n"
        << "  " << program
        << " 0 9300 9300 9301 9302 "
           "./raft_test_node0/state\n\n"

        << "  " << program
        << " 1 9301 9300 9301 9302 "
           "./raft_test_node1/state\n\n"

        << "  " << program
        << " 2 9302 9300 9301 9302 "
           "./raft_test_node2/state\n";
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 6 && argc != 7) {
        PrintUsage(argv[0]);
        return 1;
    }

    int node_id = -1;

    if (!ParseInt(argv[1], &node_id) ||
        node_id < 0 || node_id > 2) {

        std::cerr
            << "node id must be 0, 1 or 2\n";

        return 1;
    }

    uint16_t raft_port = 0;

    if (!ParsePort(argv[2], &raft_port)) {
        std::cerr
            << "invalid raft port\n";

        return 1;
    }

    std::vector<minikv::RaftNode::Peer> peers;

    for (int i = 0; i < 3; ++i) {
        uint16_t peer_port = 0;

        if (!ParsePort(argv[3 + i], &peer_port)) {
            std::cerr
                << "invalid peer port\n";

            return 1;
        }

        peers.push_back(
            minikv::RaftNode::Peer{
                i,
                "127.0.0.1",
                peer_port
            });
    }

    std::string state_file;

    if (argc == 7) {
        state_file = argv[6];
    } else {
        state_file =
            "./raft_node" +
            std::to_string(node_id) +
            "/raft_state";
    }

    minikv::RaftNode node(
        node_id,
        raft_port,
        std::move(peers),
        state_file);

    g_raft = &node;

    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    node.SetApplyCallback(
        [node_id](const std::string& command) {
            std::cout
                << "[RAFT-TEST "
                << node_id
                << "] apply command: "
                << command
                << "\n";

            return true;
        });

    if (!node.Start()) {
        std::cerr
            << "failed to start Raft node\n";

        g_raft = nullptr;
        return 1;
    }

    std::cout
        << "========================================\n"
        << " Raft standalone test node\n"
        << " Node ID   : "
        << node_id
        << "\n"
        << " Raft Port : "
        << raft_port
        << "\n"
        << " State File: "
        << state_file
        << "\n"
        << "========================================\n";

    while (true) {
        std::this_thread::sleep_for(
            std::chrono::seconds(1));

        const auto state = node.state();

        const char* state_name = "UNKNOWN";

        switch (state) {
            case minikv::RaftNode::State::Follower:
                state_name = "Follower";
                break;

            case minikv::RaftNode::State::Candidate:
                state_name = "Candidate";
                break;

            case minikv::RaftNode::State::Leader:
                state_name = "Leader";
                break;
        }

        std::cout
            << "[STATUS] node="
            << node_id
            << " state="
            << state_name
            << " term="
            << node.current_term()
            << " commit="
            << node.commit_index()
            << "\n";
    }

    node.Stop();
    g_raft = nullptr;

    return 0;
}
