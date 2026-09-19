#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <netinet/tcp.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

#include "kv_protocol.h"

namespace {

bool ReadFull(int fd, void* buffer, size_t size) {
    char* p = static_cast<char*>(buffer);
    size_t received = 0;

    while (received < size) {
        ssize_t n = recv(fd, p + received, size - received, 0);

        if (n == 0) {
            return false;
        }

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }

        received += static_cast<size_t>(n);
    }

    return true;
}

bool WriteFull(int fd, const void* buffer, size_t size) {
    const char* p = static_cast<const char*>(buffer);
    size_t sent = 0;

    while (sent < size) {
        ssize_t n = send(
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

        sent += static_cast<size_t>(n);
    }

    return true;
}

bool SendFrame(int fd, const std::string& body) {
    if (body.empty() ||
        body.size() > minikv::kvproto::kMaxFrameSize) {
        return false;
    }

    uint32_t length = static_cast<uint32_t>(body.size());

    uint8_t header[4] = {
        static_cast<uint8_t>((length >> 24) & 0xff),
        static_cast<uint8_t>((length >> 16) & 0xff),
        static_cast<uint8_t>((length >> 8) & 0xff),
        static_cast<uint8_t>(length & 0xff)
    };

    return WriteFull(fd, header, sizeof(header)) &&
           WriteFull(fd, body.data(), body.size());
}

bool ReadFrame(int fd, std::string* body) {
    uint8_t header[4];

    if (!ReadFull(fd, header, sizeof(header))) {
        return false;
    }

    uint32_t length =
        (static_cast<uint32_t>(header[0]) << 24) |
        (static_cast<uint32_t>(header[1]) << 16) |
        (static_cast<uint32_t>(header[2]) << 8) |
        static_cast<uint32_t>(header[3]);

    if (length == 0 ||
        length > minikv::kvproto::kMaxFrameSize) {
        return false;
    }

    body->resize(length);

    return ReadFull(fd, body->data(), length);
}

bool SendRequest(
    int fd,
    const minikv::kvproto::Request& request,
    minikv::kvproto::Response* response) {

    std::string request_body =
        minikv::kvproto::EncodeRequest(request);

    if (!SendFrame(fd, request_body)) {
        return false;
    }

    std::string response_body;

    if (!ReadFrame(fd, &response_body)) {
        return false;
    }

    return minikv::kvproto::DecodeResponse(
        response_body,
        response);
}

} // namespace

int main(int argc, char** argv) {
    const char* host =
        (argc >= 2) ? argv[1] : "127.0.0.1";

    int port =
        (argc >= 3) ? std::atoi(argv[2]) : 9000;

    if (port <= 0 || port > 65535) {
        std::cerr << "invalid port\n";
        return 1;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0) {
        std::cerr << "socket failed: "
                  << std::strerror(errno)
                  << "\n";
        return 1;
    }

    int nodelay = 1;
    setsockopt(
        fd,
        IPPROTO_TCP,
        TCP_NODELAY,
        &nodelay,
        sizeof(nodelay));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(static_cast<uint16_t>(port));

    if (inet_pton(
            AF_INET,
            host,
            &server_addr.sin_addr) != 1) {
        std::cerr << "invalid host\n";
        close(fd);
        return 1;
    }

    if (connect(
            fd,
            reinterpret_cast<sockaddr*>(&server_addr),
            sizeof(server_addr)) < 0) {
        std::cerr << "connect failed: "
                  << std::strerror(errno)
                  << "\n";
        close(fd);
        return 1;
    }

    std::cout << "Connected to "
              << host
              << ":"
              << port
              << "\n";

    std::cout << "Commands:\n";
    std::cout << "  ping\n";
    std::cout << "  put <key> <value>\n";
    std::cout << "  get <key>\n";
    std::cout << "  del <key>\n";
    std::cout << "  quit\n";

    while (true) {
        std::cout << "kv> " << std::flush;

        std::string line;

        if (!std::getline(std::cin, line)) {
            break;
        }

        if (line == "quit" || line == "exit") {
            break;
        }

        std::istringstream iss(line);
        std::string command;
        iss >> command;

        minikv::kvproto::Request request;

        if (command == "ping") {
            request.command =
                minikv::kvproto::Command::kPing;
        } else if (command == "get") {
            request.command =
                minikv::kvproto::Command::kGet;

            if (!(iss >> request.key)) {
                std::cout << "usage: get <key>\n";
                continue;
            }
        } else if (command == "del") {
            request.command =
                minikv::kvproto::Command::kDelete;

            if (!(iss >> request.key)) {
                std::cout << "usage: del <key>\n";
                continue;
            }
        } else if (command == "put") {
            request.command =
                minikv::kvproto::Command::kPut;

            if (!(iss >> request.key)) {
                std::cout << "usage: put <key> <value>\n";
                continue;
            }

            std::getline(iss, request.value);

            if (!request.value.empty() &&
                request.value.front() == ' ') {
                request.value.erase(0, 1);
            }

            if (request.value.empty()) {
                std::cout << "usage: put <key> <value>\n";
                continue;
            }
        } else {
            std::cout << "unknown command\n";
            continue;
        }

        minikv::kvproto::Response response;

        if (!SendRequest(fd, request, &response)) {
            std::cerr << "server disconnected\n";
            break;
        }

        switch (response.status) {
            case minikv::kvproto::StatusCode::kOk:
                std::cout << "OK";
                break;

            case minikv::kvproto::StatusCode::kNotFound:
                std::cout << "NOT_FOUND";
                break;

            case minikv::kvproto::StatusCode::kError:
                std::cout << "ERROR";
                break;
        }

        if (!response.payload.empty()) {
            std::cout << ": " << response.payload;
        }

        std::cout << "\n";
    }

    shutdown(fd, SHUT_RDWR);
    close(fd);

    return 0;
}