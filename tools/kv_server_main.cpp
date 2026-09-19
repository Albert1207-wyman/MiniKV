#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "db.h"
#include "kv_server.h"
#include "options.h"

namespace {

minikv::KvServer* g_server = nullptr;

void HandleSignal(int) {
    if (g_server != nullptr) {
        g_server->RequestStop();
    }
}

bool ParseInt(
    const char* text,
    int* value) {

    if (text == nullptr ||
        value == nullptr) {

        return false;
    }

    char* end = nullptr;

    const long parsed =
        std::strtol(
            text,
            &end,
            10);

    if (end == text ||
        *end != '\0') {

        return false;
    }

    *value =
        static_cast<int>(
            parsed);

    return true;
}

bool ParsePort(
    const char* text,
    uint16_t* port) {

    if (text == nullptr ||
        port == nullptr) {

        return false;
    }

    char* end = nullptr;

    const long parsed =
        std::strtol(
            text,
            &end,
            10);

    if (end == text ||
        *end != '\0' ||
        parsed <= 0 ||
        parsed > 65535) {

        return false;
    }

    *port =
        static_cast<uint16_t>(
            parsed);

    return true;
}

void PrintUsage(
    const char* program) {

    std::cout
        << "Usage:\n"

        << "  Standalone:\n"
        << "    "
        << program
        << " <db_path> <client_port>\n\n"

        << "  Raft node (local mode):\n"
        << "    "
        << program
        << " <db_path> <client_port> "
           "<raft_id> <raft_port> "
           "<peer0_raft_port> "
           "<peer1_raft_port> "
           "<peer2_raft_port>\n\n"

        << "  Raft node (custom peer host mode):\n"
        << "    "
        << program
        << " <db_path> <client_port> "
           "<raft_id> <raft_port> "
           "<peer0_host> <peer0_port> "
           "<peer1_host> <peer1_port> "
           "<peer2_host> <peer2_port>\n\n"

        << "Examples:\n"

        << "  Node0 local mode:\n"
        << "    "
        << program
        << " ./raft_node0 9400 "
           "0 9300 9300 9301 9302\n\n"

        << "  Node1 local mode:\n"
        << "    "
        << program
        << " ./raft_node1 9401 "
           "1 9301 9300 9301 9302\n\n"

        << "  Node2 local mode:\n"
        << "    "
        << program
        << " ./raft_node2 9402 "
           "2 9302 9300 9301 9302\n\n"

        << "  Node0 Docker mode:\n"
        << "    "
        << program
        << " /data 9400 "
           "0 9300 "
           "minikv-node0 9300 "
           "minikv-node1 9301 "
           "minikv-node2 9302\n";
}

} // namespace

int main(
    int argc,
    char** argv) {

    if (argc != 3 &&
        argc != 8 &&
        argc != 11) {

        PrintUsage(argv[0]);

        return 1;
    }

    const std::string db_path =
        argv[1];

    uint16_t client_port = 0;

    if (!ParsePort(
            argv[2],
            &client_port)) {

        std::cerr
            << "invalid client port\n";

        return 1;
    }

    int raft_id = -1;

    uint16_t raft_port = 0;

    std::vector<
        minikv::RaftNode::Peer>
        raft_peers;

    if (argc == 8 ||
        argc == 11) {

        if (!ParseInt(
                argv[3],
                &raft_id) ||
            raft_id < 0 ||
            raft_id > 2) {

            std::cerr
                << "raft_id must be 0, 1 or 2\n";

            return 1;
        }

        if (!ParsePort(
                argv[4],
                &raft_port)) {

            std::cerr
                << "invalid raft port\n";

            return 1;
        }

        if (argc == 8) {

            /*
             * Existing local three-process mode.
             *
             * Peer hosts are all 127.0.0.1 so the existing
             * minikv_cluster.sh behavior remains unchanged.
             */
            for (int i = 0;
                 i < 3;
                 ++i) {

                uint16_t peer_port = 0;

                if (!ParsePort(
                        argv[5 + i],
                        &peer_port)) {

                    std::cerr
                        << "invalid peer raft port\n";

                    return 1;
                }

                raft_peers.push_back(
                    minikv::RaftNode::Peer{
                        i,
                        "127.0.0.1",
                        peer_port
                    });
            }

        } else {

            /*
             * Docker / container mode.
             *
             * Arguments are:
             *
             *   peer0_host peer0_port
             *   peer1_host peer1_port
             *   peer2_host peer2_port
             *
             * Docker Compose service names such as
             * minikv-node0 are passed directly to Peer.host.
             */
            for (int i = 0;
                 i < 3;
                 ++i) {

                const int host_arg =
                    5 + i * 2;

                const int port_arg =
                    host_arg + 1;

                if (argv[host_arg] == nullptr ||
                    argv[host_arg][0] == '\0') {

                    std::cerr
                        << "invalid peer host for node"
                        << i
                        << "\n";

                    return 1;
                }

                uint16_t peer_port = 0;

                if (!ParsePort(
                        argv[port_arg],
                        &peer_port)) {

                    std::cerr
                        << "invalid peer raft port for node"
                        << i
                        << "\n";

                    return 1;
                }

                raft_peers.push_back(
                    minikv::RaftNode::Peer{
                        i,
                        argv[host_arg],
                        peer_port
                    });
            }
        }
    }

    minikv::Options options;

    options.create_if_missing =
        true;

    minikv::DB* db = nullptr;

    const minikv::Status status =
        minikv::DB::Open(
            options,
            db_path,
            &db);

    if (!status.ok()) {

        std::cerr
            << "failed to open DB: "
            << status.ToString()
            << "\n";

        return 1;
    }

    std::string raft_state_file;

    if (raft_id >= 0) {

        raft_state_file =
            db_path +
            "/raft_state";
    }

    minikv::KvServer server(
        db,
        "0.0.0.0",
        client_port,
        raft_id,
        raft_port,
        std::move(raft_peers),
        raft_state_file);

    g_server =
        &server;

    std::signal(
        SIGINT,
        HandleSignal);

    std::signal(
        SIGTERM,
        HandleSignal);

    std::cout
        << "========================================\n"
        << " MiniKV Server V4\n"
        << " DB Path : "
        << db_path
        << "\n"
        << " Client  : 0.0.0.0:"
        << client_port
        << "\n";

    if (raft_id >= 0) {

        std::cout
            << " Raft ID : "
            << raft_id
            << "\n"
            << " Raft UDP: "
            << raft_port
            << "\n"
            << " Raft State: "
            << raft_state_file
            << "\n";

        if (argc == 11) {

            std::cout
                << " Peer Mode: custom hosts\n";

        } else {

            std::cout
                << " Peer Mode: 127.0.0.1\n";
        }

    } else {

        std::cout
            << " Raft    : disabled\n";
    }

    std::cout
        << "========================================\n";

    const int rc =
        server.Start();

    server.Stop();

    g_server =
        nullptr;

    delete db;

    return rc;
}
