example: example.cpp graphite_pusher.hpp
	g++ -std=c++17 -Wall -Wextra -pedantic-errors -Werror example.cpp -lpthread -o example

.PHONY: clean
clean:
	rm -f example
