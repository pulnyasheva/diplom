#include <iostream>
#include <Scheduler.h>

Scheduler::Scheduler(Logger *logger_)
    : logger(logger_) {
}

std::packaged_task<bool()> Scheduler::createTask(const std::string& name, std::function<bool()> func) {
    logger->log(LogLevel::DEBUG, fmt::format("Create task {}", name));
    return std::packaged_task(func);
}

bool Scheduler::runTask(std::packaged_task<bool()> task) {
    std::future<bool> result = task.get_future();
    std::cout << "task" << std::endl;
    std::thread([task = std::move(task)]() mutable {
        task();
    }).detach();
    std::cout << "result" << std::endl;
    return result.get();
}
