#include <chrono>
#include <iomanip>
#include <ctime>
#include <iostream>

#include <Logger.h>

Logger::Logger(const std::string& file_name_) : file_name(file_name_){
    log_file.open(file_name, std::ios::app);
    if (!log_file.is_open()) {
        std::cerr << "Failed to open log file: " << file_name << std::endl;
    }
}

Logger::Logger(Logger&& other) noexcept :
    log_file(std::move(other.log_file)),
    file_name(std::move(other.file_name)) {}

Logger::~Logger() {
    if (log_file.is_open()) {
        log_file.close();
    }
}

void Logger::log(LogLevel level, const std::string& message) {
    std::lock_guard guard(log_mutex);
    if (log_file.is_open()) {
        log_file << getCurrentTime() << " [" << logLevelToString(level) << "] " << message << std::endl;
    }
}

std::string Logger::getCurrentTime() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm local_tm = *std::localtime(&now_time);

    std::ostringstream oss;
    oss << std::put_time(&local_tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

std::string Logger::logLevelToString(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO: return "INFO";
        case LogLevel::WARNING: return "WARNING";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::CRITICAL: return "CRITICAL";
        default: return "UNKNOWN";
    }
}
