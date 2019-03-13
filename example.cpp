#include <chrono>
#include <memory>

#include "graphite_pusher.hpp"

int main() {
	const auto graphite = std::make_shared<GraphitePusher>("localhost", 2004);
	graphite->start();

	// timestamp == now
	graphite->push_sample("graphite_pusher.example_1", 12.345);

	for (double s = 0; s < 10.0; s += 0.1) {
		graphite->push_sample("graphite_pusher.example_2", s);
	}

	// specify timestamp
	const auto epoch = std::chrono::system_clock::now().time_since_epoch();
	const auto ts = std::chrono::duration_cast<std::chrono::seconds>(epoch).count();
	graphite->push_sample("graphite_pusher.example_3", ts, 100);

	// optional. block until metric queue is flushed.
	graphite->shutdown();

	return 0;
}
