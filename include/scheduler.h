#pragma once

#include <future>
#include <functional>
#include <string>

#include <logger.h>

class scheduler {
public:
    explicit scheduler(logger *logger_);

    std::packaged_task<bool()> createTask(const std::string& name, std::function<bool()> func);

    bool runTask(std::packaged_task<bool()> task);

private:
    logger *current_logger;
};
