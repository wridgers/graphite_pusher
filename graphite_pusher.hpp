#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using metric_t = std::tuple<std::string, int, double>;

class GraphitePusher {
	public:
		GraphitePusher(const std::string& host="localhost", int port=2004)
			: _host(host), _port(port), _thread() {}

		~GraphitePusher() {
			stop();
		}

		void setFrequency(const double& frequency) {
			_frequency = frequency;
		}

		void start() {
			_thread = std::thread(&GraphitePusher::thread, this);
			_thread.detach();
		}

		void stop() {
			_running = false;
		}

		void shutdown() {
			while(not queue_empty()) {
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			}

			_running = false;
		}

		void push_sample(const std::string& path, double sample) {
			const auto epoch = std::chrono::system_clock::now().time_since_epoch();
			const auto ts = std::chrono::duration_cast<std::chrono::seconds>(epoch).count();

			push_sample(path, ts, sample);
		}

		void push_sample(const std::string& path, int ts, double sample) {
			std::lock_guard<std::mutex> lock(_mutex);
			_queue.emplace(std::make_tuple(path, ts, sample));
		}

	private:
		int setup_socket() {

			struct addrinfo hints = {}, *addrs;
			hints.ai_family = AF_UNSPEC;
			hints.ai_socktype = SOCK_STREAM;
			hints.ai_protocol = IPPROTO_TCP;

			int err = getaddrinfo(_host.c_str(), std::to_string(_port).c_str(), &hints, &addrs);

			if (err != 0) {
				std::perror("getaddrinfo");
				return -1;
			}

			int sock = -1;

			for(struct addrinfo *addr = addrs; addr != NULL; addr = addr->ai_next) {
				sock = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);

				if (sock == -1) {
					continue;
				}

				if (connect(sock, addr->ai_addr, addr->ai_addrlen) != -1) {
					break;
				}

				close(sock);
				sock = -1;
			}

			freeaddrinfo(addrs);

			return sock;
		}

		void thread() {
			const auto sleep = std::chrono::seconds((int)(60.0 / _frequency));

			int sock = -1;
			_running = true;

			while(_running.load()) {
				if (sock < 0) {
					sock = setup_socket();
				}

				if (sock >= 0 and not queue_empty()) {
					const std::vector<metric_t> metrics = std::move(get_metrics());
					const std::vector<char> message = std::move(build_message(metrics));

					if (send(sock, message.data(), message.size(), 0) < 0) {
						std::perror("send");
						close(sock);
						sock = -1;
					} else {
						// send failed, requeue metrics
						for (const auto& [path, ts, sample] : metrics) {
							push_sample(path, ts, sample);
						}
					}
				}

				std::this_thread::sleep_for(sleep);
			}
		}

		std::vector<metric_t> get_metrics() {
			std::unique_lock<std::mutex> lock(_mutex);
			std::vector<metric_t> metrics;

			while (not _queue.empty()) {
				metrics.emplace_back(std::move(_queue.front()));
				_queue.pop();
			}

			return metrics;
		}

		std::vector<char> build_message(const std::vector<metric_t>& metrics) {
			uint64_t payload_size = 4;
			for (const auto& metric : metrics) {
				const auto& path = std::get<std::string>(metric);
				payload_size += 30 + path.length();
			}

			// message is a 4 bytes header (length) + payload
			// https://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-pickle-protocol
			std::vector<char> message;
			message.reserve(4 + payload_size);

			message.emplace_back(payload_size >> 32);
			message.emplace_back(payload_size >> 16);
			message.emplace_back(payload_size >> 8);
			message.emplace_back(payload_size >> 0);

			// PROTO v2
			// https://github.com/python/cpython/blob/master/Lib/pickle.py
			message.emplace_back(0x80);
			message.emplace_back(0x02);

			// EMPTY LIST
			message.emplace_back(0x5d);

			for (const auto& [ path, ts, sample ] : metrics) {
				// BINPUT 0
				message.emplace_back(0x71);
				message.emplace_back(0x00);

				// BINUNICODE
				message.emplace_back(0x58);

				// BINUNICODE string length
				const char* str = path.c_str();
				const uint64_t len = path.length();

				message.emplace_back(len >> 0);
				message.emplace_back(len >> 8);
				message.emplace_back(len >> 16);
				message.emplace_back(len >> 32);

				// BINUNICODE string bytes
				for (uint64_t i = 0; i < len; ++i) {
					message.emplace_back(str[i]);
				}

				// BINPUT 1
				message.emplace_back(0x71);
				message.emplace_back(0x01);

				// BININT ts
				message.emplace_back(0x4a);
				const unsigned char* t = reinterpret_cast<const unsigned char*>(&ts);
				for (std::size_t i = 0; i < sizeof(int); ++i) {
					message.emplace_back(t[i]);
				}

				// BINFLOAT sample
				message.emplace_back(0x47);
				const unsigned char* s = reinterpret_cast<const unsigned char*>(&sample);
				for (std::size_t i = sizeof(double); i > 0; --i) {
					message.emplace_back(s[i - 1]);
				}

				message.emplace_back(0x86); // TUPLE2
				message.emplace_back(0x71); // BINPUT 2
				message.emplace_back(0x02);
				message.emplace_back(0x86); // TUPLE2
				message.emplace_back(0x71); // BINPUT 3
				message.emplace_back(0x03);
				message.emplace_back(0x61); // APPEND
			}

			message.emplace_back(0x2e); // STOP

			assert(message.size() == 4 + payload_size);

			return message;
		}

		bool queue_empty() {
			std::lock_guard<std::mutex> lock(_mutex);
			return _queue.empty();
		}

		std::string _host;
		int _port;
		double _frequency = 60.0;

		std::thread _thread;
		std::atomic<bool> _running;

		std::queue<metric_t> _queue;
		std::mutex _mutex;
};
