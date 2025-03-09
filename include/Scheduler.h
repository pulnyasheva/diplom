#pragma once

#include <future>
#include <functional>
#include <string>

#include <Logger.h>

class Scheduler {
public:
    explicit Scheduler(Logger *logger_);

    std::packaged_task<bool()> createTask(const std::string& name, std::function<bool()> func);

    bool runTask(std::packaged_task<bool()> task);

private:
    Logger *logger;
};
