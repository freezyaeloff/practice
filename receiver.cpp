#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using Clock = std::chrono::steady_clock;

const int PACKET_SIZE = 1200;
const uint32_t MAGIC = 0x5041434b;

std::string readLine(int socket) {
    std::string line;
    char c;

    while (true) {
        ssize_t n = recv(socket, &c, 1, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) throw std::runtime_error("TCP-соединение прервано");
        if (c == '\n') return line;
        line += c;
    }
}

long long socketDrops(int socket) {
    struct stat info{};
    if (fstat(socket, &info) < 0) return -1;

    std::ifstream file("/proc/self/net/udp");
    std::string line;
    std::getline(file, line);

    while (std::getline(file, line)) {
        std::istringstream row(line);
        std::string skip;
        unsigned long long inode;
        long long drops;

        for (int i = 0; i < 9; ++i) row >> skip;

        if (row >> inode >> skip >> skip >> drops)
            if (inode == info.st_ino) return drops;
    }

    return -1;
}

class Receiver {
    int listener = -1;
    int tcp = -1;
    int udp = -1;

public:
    ~Receiver() {
        close(listener);
        close(tcp);
        close(udp);
    }

    void run(int port, const char* csvPath, int bufferBytes, int graceMs) {
        listener = socket(AF_INET, SOCK_STREAM, 0);
        udp = socket(AF_INET, SOCK_DGRAM, 0);
        if (listener < 0 || udp < 0) throw std::runtime_error("Не удалось создать сокеты");

        if (bufferBytes && setsockopt(udp, SOL_SOCKET, SO_RCVBUF, &bufferBytes, sizeof(bufferBytes)) < 0)
            throw std::runtime_error("Не удалось изменить SO_RCVBUF");

        int rcvbuf;
        socklen_t optionSize = sizeof(rcvbuf);
        if (getsockopt(udp, SOL_SOCKET, SO_RCVBUF, &rcvbuf, &optionSize) < 0)
            throw std::runtime_error("Не удалось прочитать SO_RCVBUF");

        int reuse = 1;
        if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
            throw std::runtime_error("Ошибка настройки TCP-сокета");

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(port);

        if (bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 ||
            bind(udp, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 ||
            listen(listener, 1) < 0)
            throw std::runtime_error("Не удалось занять порт");

        std::cout << "Ожидание передатчика на порту " << port << std::endl;
        tcp = accept(listener, nullptr, nullptr);
        if (tcp < 0) throw std::runtime_error("Ошибка accept");

        uint32_t runId;
        int mbps, seconds, sndbuf;
        long long planned;

        std::istringstream config(readLine(tcp));
        if (!(config >> runId >> mbps >> seconds >> planned >> sndbuf) || planned < 1 || planned > 50000000)
            throw std::runtime_error("Не удалось прочитать параметры теста");

        std::vector<bool> received(static_cast<size_t>(planned), false);
        auto start = Clock::now();

        if (send(tcp, "R", 1, MSG_NOSIGNAL) != 1)
            throw std::runtime_error("Не удалось подтвердить готовность");

        long long unique = 0, duplicates = 0, reordered = 0;
        long long attempts = 0, sent = 0, errors = 0;
        uint32_t highest = 0;
        double elapsed = 0;
        bool finished = false;

        std::string report;
        auto deadline = Clock::now() + std::chrono::seconds(seconds + 30);
        std::vector<std::string> samples;
        double previousTime = 0;
        long long previousUnique = 0;

        auto sample = [&]() {
            double t = std::chrono::duration<double>(Clock::now() - start).count();
            double epoch = std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
            long long pendingGaps = unique ? static_cast<long long>(highest) + 1 - unique : 0;

            std::ostringstream row;
            row.precision(16);
            row << runId << ',' << epoch << ',' << t << ',' << t - previousTime << ',' << unique << ','
                << duplicates << ',' << reordered << ',' << pendingGaps << ',' << socketDrops(udp) << ','
                << (unique - previousUnique) * PACKET_SIZE * 8.0 / (t - previousTime) / 1e6;

            samples.push_back(row.str());
            previousTime = t;
            previousUnique = unique;
        };

        while (Clock::now() < deadline) {
            pollfd sockets[2] = {{udp, POLLIN, 0}, {finished ? -1 : tcp, POLLIN, 0}};

            if (poll(sockets, 2, 100) < 0) {
                if (errno == EINTR) continue;
                throw std::runtime_error("Ошибка poll");
            }

            if (sockets[1].revents) {
                char buffer[256];
                ssize_t n = recv(tcp, buffer, sizeof(buffer), MSG_DONTWAIT);

                if (n == 0) throw std::runtime_error("Передатчик отключился без полного отчёта");
                if (n < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
                    throw std::runtime_error("Ошибка чтения TCP");

                if (n > 0) report.append(buffer, static_cast<size_t>(n));

                if (report.find('\n') != std::string::npos) {
                    std::istringstream values(report);

                    if (!(values >> attempts >> sent >> errors >> elapsed) || sent < 0 ||
                        sent > planned || attempts < sent || attempts > planned ||
                        errors != attempts - sent || elapsed <= 0)
                        throw std::runtime_error("Некорректный итоговый отчёт");

                    finished = true;
                    deadline = Clock::now() + std::chrono::milliseconds(graceMs);
                }
            }

            if (sockets[0].revents & POLLIN) {
                for (int i = 0; i < 100; ++i) {
                    char packet[PACKET_SIZE + 1];
                    ssize_t n = recvfrom(udp, packet, sizeof(packet), MSG_DONTWAIT, nullptr, nullptr);

                    if (n < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) break;
                        throw std::runtime_error("Ошибка чтения UDP");
                    }

                    if (n != PACKET_SIZE) continue;

                    uint32_t header[3];
                    std::memcpy(header, packet, sizeof(header));
                    if (ntohl(header[0]) != MAGIC || ntohl(header[1]) != runId) continue;

                    uint32_t seq = ntohl(header[2]);
                    if (seq >= received.size()) continue;
                    if (received[seq]) { ++duplicates; continue; }

                    if (unique > 0 && seq < highest) ++reordered;
                    if (seq > highest) highest = seq;

                    received[seq] = true;
                    ++unique;
                }
            }

            if (std::chrono::duration<double>(Clock::now() - start).count() - previousTime >= 1)
                sample();
        }

        if (!finished) throw std::runtime_error("Нет отчёта передатчика: итоговые потери неизвестны");
        if (unique > sent || (unique > 0 && highest >= sent))
            throw std::runtime_error("Номера пакетов не совпадают с отчётом передатчика");

        sample();

        long long lost = sent - unique;
        long long drops = socketDrops(udp);
        double lossPercent = sent ? lost * 100.0 / sent : 0;
        double txMbps = sent * PACKET_SIZE * 8.0 / elapsed / 1000000;
        double rxMbps = unique * PACKET_SIZE * 8.0 / elapsed / 1000000;

        std::ofstream csv(csvPath, std::ios::app);
        if (!csv) throw std::runtime_error("Не удалось открыть CSV");

        if (csv.tellp() == 0)
            csv << "run_id,target_mbps,packet_bytes,seconds,actual_seconds,planned,attempts,sent,send_errors,unsent_slots,received,lost,loss_percent,duplicates,reordered,rx_socket_drops,tx_mbps,rx_mbps,sndbuf_actual,rcvbuf_actual,grace_ms\n";

        csv << runId << ',' << mbps << ',' << PACKET_SIZE << ',' << seconds << ',' << elapsed << ','
            << planned << ',' << attempts << ',' << sent << ',' << errors << ',' << planned - attempts << ','
            << unique << ',' << lost << ',';

        if (sent) csv << lossPercent;

        csv << ',' << duplicates << ',' << reordered << ',' << drops << ',' << txMbps << ',' << rxMbps
            << ',' << sndbuf << ',' << rcvbuf << ',' << graceMs << '\n';

        csv.close();
        if (!csv) throw std::runtime_error("Не удалось сохранить CSV");

        std::ofstream timeline(std::string(csvPath) + ".timeline.csv", std::ios::app);
        if (timeline.tellp() == 0)
            timeline << "run_id,unix_seconds,elapsed_seconds,interval_seconds,received,duplicates,reordered,pending_gaps,rx_socket_drops,interval_mbps\n";

        for (const auto& row : samples) timeline << row << '\n';
        timeline.close();

        std::ofstream missing(std::string(csvPath) + ".missing.csv", std::ios::app);
        if (missing.tellp() == 0) missing << "run_id,first_sequence,last_sequence\n";

        for (long long seq = 0; seq < sent; ++seq) {
            if (received[seq]) continue;
            long long first = seq;
            while (seq + 1 < sent && !received[seq + 1]) ++seq;
            missing << runId << ',' << first << ',' << seq << '\n';
        }

        missing.close();
        if (!timeline || !missing) throw std::runtime_error("Не удалось сохранить подробные метрики");

        std::cout << "Отправлено успешно: " << sent << ", принято уникальных: " << unique
                  << "\nПотеряно: " << lost;

        if (sent) std::cout << " (" << lossPercent << "%)";
        else std::cout << " (процент не определён: нет успешных отправок)";

        std::cout << "\nПовторов: " << duplicates << ", перестановок: " << reordered
                  << "\nСбросов UDP-сокета: " << drops << " (-1 = недоступно)"
                  << "\nTX: " << txMbps << " Мбит/с, RX: " << rxMbps << " Мбит/с"
                  << "\nМетрики: " << csvPath << '\n';
    }
};

int main(int argc, char* argv[]) {
    if (argc < 2 || argc > 5) {
        std::cout << "Запуск: ./receiver PORT [CSV] [RCVBUF_BYTES] [GRACE_MS]\nПример: ./receiver 5001\n";
        return 1;
    }

    try {
        int port = std::stoi(argv[1]);
        if (port < 1 || port > 65535) throw std::runtime_error("Порт: 1–65535");

        Receiver receiver;
        int bufferBytes = argc >= 4 ? std::stoi(argv[3]) : 0;
        int graceMs = argc == 5 ? std::stoi(argv[4]) : 2000;

        if (bufferBytes < 0 || graceMs < 100 || graceMs > 60000)
            throw std::runtime_error("Буфер >= 0; ожидание поздних пакетов: 100–60000 мс");

        receiver.run(port, argc >= 3 ? argv[2] : "receiver.csv", bufferBytes, graceMs);
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}