#pragma once

#include <future>
#include <functional>
#include <string>

#include <Logger.h>

class Scheduler {
public:
    explicit Scheduler(Logger *logger_);

    std::packaged_task<void()> createTask(const std::string& name, std::function<void()> func);

    void runTask(std::packaged_task<void()> task);

private:
    Logger *logger;
};
