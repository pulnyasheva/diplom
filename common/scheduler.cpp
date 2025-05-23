#include <iostream>
#include <fmt/format.h>

#include <common/scheduler.h>

scheduler::scheduler(logger *logger_)
    : current_logger(logger_) {
}

std::packaged_task<bool()> scheduler::createTask(const std::string& name, std::function<bool()> func) {
    current_logger->log_to_file(log_level::DEBUG, fmt::format("Create task {}", name));
    return std::packaged_task(func);
}

bool scheduler::runTask(std::packaged_task<bool()> task) {
    std::future<bool> result = task.get_future();
    std::cout << "task" << std::endl;
    std::thread([task = std::move(task)]() mutable {
        task();
    }).detach();
    std::cout << "result" << std::endl;
    return result.get();
}
