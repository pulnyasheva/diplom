#include <Scheduler.h>

Scheduler::Scheduler(Logger *logger_)
    : logger(logger_) {
}

std::packaged_task<void()> Scheduler::createTask(const std::string& name, std::function<void()> func) {
    logger->log(LogLevel::DEBUG, fmt::format("Create task {}", name));
    return std::packaged_task(func);
}

void Scheduler::runTask(std::packaged_task<void()> task) {
    std::thread([task = std::move(task)]() mutable {
        task();
    }).detach();
}
