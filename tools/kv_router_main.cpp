#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>

#include "kv_router.h"

namespace {

bool ParsePort(
    const char* text,
    uint16_t* port) {

    if (text == nullptr ||
        port == nullptr) {

        return false;
    }

    char* end = nullptr;

    const long value =
        std::strtol(
            text,
            &end,
            10);

    if (end == text ||
        *end != '\0' ||
        value <= 0 ||
        value > 65535) {

        return false;
    }

    *port =
        static_cast<uint16_t>(
            value);

    return true;
}

void PrintUsage(
    const char* program) {

    std::cout
        << "Usage:\n"
        << "  "
        << program
        << " <listen_host> <listen_port> "
        << "[9 backend ports]\n\n"

        << "Backend order:\n"
        << "  shard0: replica0 replica1 replica2\n"
        << "  shard1: replica0 replica1 replica2\n"
        << "  shard2: replica0 replica1 replica2\n\n"

        << "Example:\n"
        << "  "
        << program
        << " 0.0.0.0 9000 "
        << "9400 9401 9402 "
        << "9410 9411 9412 "
        << "9420 9421 9422\n";
}

} // namespace

int main(
    int argc,
    char** argv) {

    if (argc != 12) {
        PrintUsage(argv[0]);
        return 1;
    }

    const std::string listen_host =
        argv[1];

    uint16_t listen_port = 0;

    if (!ParsePort(
            argv[2],
            &listen_port)) {

        std::cerr
            << "invalid listen port\n";

        return 1;
    }

    minikv::KvRouter::ShardGroup
        groups[3];

    int argument_index = 3;

    for (size_t shard = 0;
         shard < 3;
         ++shard) {

        for (size_t replica = 0;
             replica < 3;
             ++replica) {

            uint16_t port = 0;

            if (!ParsePort(
                    argv[argument_index],
                    &port)) {

                std::cerr
                    << "invalid backend port: "
                    << argv[argument_index]
                    << "\n";

                return 1;
            }

            groups[shard][replica] =
                minikv::KvRouter::Backend{
                    "127.0.0.1",
                    port
                };

            ++argument_index;
        }
    }

    std::array<
        minikv::KvRouter::ShardGroup,
        minikv::KvRouter::kShardCount>
        shards = {
            groups[0],
            groups[1],
            groups[2]
        };

    minikv::KvRouter router(
        std::move(shards),
        listen_host,
        listen_port);

    return router.Start();
}