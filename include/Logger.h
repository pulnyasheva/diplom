#pragma once

#include <string>
#include <fstream>
#include <mutex>

enum class LogLevel {
    DEBUG,
    INFO,
    WARNING,
    ERROR,
    CRITICAL
};

class Logger {
public:
    Logger(const std::string& file_name_);
    Logger(Logger&&) noexcept;
    ~Logger();

    void log(LogLevel level, const std::string& message);

private:
    std::ofstream log_file;
    std::string file_name;
    std::mutex log_mutex;

    static std::string getCurrentTime();
    static std::string logLevelToString(LogLevel level);
};
