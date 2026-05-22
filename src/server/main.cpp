// knotd — Knot server daemon entry point.
//
// Day 1 hello-world: initialize structured logging and exit. Real work
// starts in Week 2 (Raft) and is wired in here.

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

int main(int /*argc*/, char* /*argv*/[]) {
    auto logger = spdlog::stdout_color_mt("knotd");
    spdlog::set_default_logger(logger);
    spdlog::set_level(spdlog::level::info);
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%^%l%$] %v");

    spdlog::info("knotd v0.1.0 starting");
    spdlog::info("build: {} on {}", __DATE__, __TIME__);
    spdlog::info("hello, distributed world");

    return 0;
}
