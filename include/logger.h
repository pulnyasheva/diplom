#pragma once

#include <string>
#include <fstream>
#include <mutex>

#include "fmt/format.h"

enum class log_level {
    DEBUG,
    INFO,
    WARNING,
    ERROR,
    CRITICAL
};

class logger {
public:
    logger(const std::string& file_name_);
    logger(logger&&) noexcept;
    ~logger();

    void log(log_level level, const std::string& message);

private:
    std::ofstream log_file;
    std::string file_name;
    std::mutex log_mutex;

    static std::string get_current_time();
    static std::string log_level_to_string(log_level level);
};
