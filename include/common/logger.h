#pragma once

#include <string>
#include <fstream>
#include <mutex>

enum log_level : int {
    CRITICAL = 0,
    ERR = 1,
    WARN = 2,
    INFO = 3,
    DEBUGER = 4
};

class logger {
public:
    logger(const std::string& file_name_, const std::string& url_);
    logger(logger&&) noexcept;
    ~logger();

    void log_to_file(log_level level, const std::string& message);

    void log_to_url(log_level level, const std::string& message);

private:
    std::ofstream log_file;
    std::string file_name;
    std::mutex log_mutex;
    std::string url;
    log_level current_log_level = log_level::INFO;

    static std::string get_current_time();
    static std::string log_level_to_string(log_level level);
};
