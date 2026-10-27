#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <sstream>
#include <vector>

using Clock = std::chrono::steady_clock;

const int PACKET_SIZE = 1200;
const uint32_t MAGIC = 0x5041434b;

void sendText(int socket, const std::string& text) {
    size_t sent = 0;

    while (sent < text.size()) {
        ssize_t n = send(socket, text.data() + sent, text.size() - sent, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) throw std::runtime_error("Ошибка отправки по TCP");
        sent += n;
    }
}

class Sender {
    int tcp = -1;
    int udp = -1;

public:
    ~Sender() {
        close(tcp);
        close(udp);
    }

    void run(const char* ip, int port, int mbps, int seconds, const char* csvPath, int bufferBytes) {
        tcp = socket(AF_INET, SOCK_STREAM, 0);
        udp = socket(AF_INET, SOCK_DGRAM, 0);
        if (tcp < 0 || udp < 0) throw std::runtime_error("Не удалось создать сокеты");

        if (bufferBytes && setsockopt(udp, SOL_SOCKET, SO_SNDBUF, &bufferBytes, sizeof(bufferBytes)) < 0)
            throw std::runtime_error("Не удалось изменить SO_SNDBUF");

        int sndbuf;
        socklen_t optionSize = sizeof(sndbuf);
        if (getsockopt(udp, SOL_SOCKET, SO_SNDBUF, &sndbuf, &optionSize) < 0)
            throw std::runtime_error("Не удалось прочитать SO_SNDBUF");

        sockaddr_in server{};
        server.sin_family = AF_INET;
        server.sin_port = htons(port);

        if (inet_pton(AF_INET, ip, &server.sin_addr) != 1)
            throw std::runtime_error("Нужен IPv4-адрес, например 127.0.0.1");

        if (connect(tcp, reinterpret_cast<sockaddr*>(&server), sizeof(server)) < 0)
            throw std::runtime_error("Не удалось подключиться: сначала запусти приёмник");

        uint32_t runId = static_cast<uint32_t>(std::time(nullptr));
        long long planned = 1LL * mbps * 1000000 * seconds / (PACKET_SIZE * 8);

        std::string config = std::to_string(runId) + " " + std::to_string(mbps) + " " +
                             std::to_string(seconds) + " " + std::to_string(planned) + " " +
                             std::to_string(sndbuf) + "\n";

        sendText(tcp, config);

        char ready;
        if (recv(tcp, &ready, 1, MSG_WAITALL) != 1 || ready != 'R')
            throw std::runtime_error("Приёмник не подтвердил готовность");

        char packet[PACKET_SIZE]{};
        uint32_t header[3] = {htonl(MAGIC), htonl(runId), 0};

        long long attempts = 0, sent = 0, errors = 0;
        long long interval = 1LL * PACKET_SIZE * 8 * 1000 / mbps;

        auto start = Clock::now();
        auto end = start + std::chrono::seconds(seconds);

        std::vector<std::string> samples;
        double previousTime = 0;
        long long previousSent = 0;

        auto sample = [&]() {
            double t = std::chrono::duration<double>(Clock::now() - start).count();
            double epoch = std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();

            std::ostringstream row;
            row.precision(16);

            row << runId << ',' << epoch << ',' << t << ',' << t - previousTime << ',' << attempts << ','
                << sent << ',' << errors << ',' << (sent - previousSent) * PACKET_SIZE * 8.0 / (t - previousTime) / 1e6;

            samples.push_back(row.str());
            previousTime = t;
            previousSent = sent;
        };

        std::cout << "Отправка: " << mbps << " Мбит/с, " << seconds << " с" << std::endl;

        for (long long slot = 0; slot < planned; ++slot) {
            std::this_thread::sleep_until(start + std::chrono::nanoseconds(slot * interval));

            auto now = Clock::now();
            if (now >= end) break;

            long long currentSlot = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start).count() / interval;
            if (currentSlot > slot) slot = currentSlot;
            if (slot >= planned) break;

            header[2] = htonl(static_cast<uint32_t>(sent));
            std::memcpy(packet, header, sizeof(header));
            ++attempts;

            if (sendto(udp, packet, sizeof(packet), MSG_DONTWAIT,
                       reinterpret_cast<sockaddr*>(&server), sizeof(server)) == PACKET_SIZE) {
                ++sent;
            } else {
                if (errors == 0) std::cerr << "Первая ошибка UDP: " << std::strerror(errno) << '\n';
                ++errors;
            }

            if (std::chrono::duration<double>(Clock::now() - start).count() - previousTime >= 1)
                sample();
        }

        std::this_thread::sleep_until(end);
        sample();

        double elapsed = std::chrono::duration<double>(Clock::now() - start).count();
        double actualMbps = sent * PACKET_SIZE * 8.0 / elapsed / 1000000;

        sendText(tcp, std::to_string(attempts) + " " + std::to_string(sent) + " " +
                      std::to_string(errors) + " " + std::to_string(elapsed) + "\n");

        std::ofstream csv(csvPath, std::ios::app);
        if (!csv) throw std::runtime_error("Не удалось открыть CSV");

        if (csv.tellp() == 0)
            csv << "run_id,target_mbps,packet_bytes,seconds,actual_seconds,planned,attempts,sent,send_errors,unsent_slots,actual_mbps,sndbuf_actual\n";

        csv << runId << ',' << mbps << ',' << PACKET_SIZE << ',' << seconds << ',' << elapsed << ','
            << planned << ',' << attempts << ',' << sent << ',' << errors << ','
            << planned - attempts << ',' << actualMbps << ',' << sndbuf << '\n';

        csv.close();
        if (!csv) throw std::runtime_error("Не удалось сохранить CSV");

        std::ofstream timeline(std::string(csvPath) + ".timeline.csv", std::ios::app);
        if (timeline.tellp() == 0)
            timeline << "run_id,unix_seconds,elapsed_seconds,interval_seconds,attempts,sent,send_errors,interval_mbps\n";

        for (const auto& row : samples) timeline << row << '\n';

        timeline.close();
        if (!timeline) throw std::runtime_error("Не удалось сохранить timeline CSV");

        std::cout << "Успешных отправок: " << sent << ", ошибок: " << errors
                  << "\nПропущено слотов отправки: " << planned - attempts
                  << "\nФактическая скорость: " << actualMbps << " Мбит/с"
                  << "\nМетрики: " << csvPath << '\n';
    }
};

int main(int argc, char* argv[]) {
    if (argc < 5 || argc > 7) {
        std::cout << "Запуск: ./sender IP PORT MBPS SECONDS [CSV] [SNDBUF_BYTES]\n"
                     "Пример: ./sender 127.0.0.1 5001 5 60\n";
        return 1;
    }

    try {
        int port = std::stoi(argv[2]), mbps = std::stoi(argv[3]), seconds = std::stoi(argv[4]);

        if (port < 1 || port > 65535 || mbps < 1 || mbps > 100 || seconds < 1 || seconds > 3600)
            throw std::runtime_error("Порт: 1–65535; скорость: 1–100 Мбит/с; длительность: 1–3600 с");

        Sender sender;
        int bufferBytes = argc == 7 ? std::stoi(argv[6]) : 0;

        if (bufferBytes < 0) throw std::runtime_error("Размер буфера не может быть отрицательным");

        sender.run(argv[1], port, mbps, seconds, argc >= 6 ? argv[5] : "sender.csv", bufferBytes);
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}