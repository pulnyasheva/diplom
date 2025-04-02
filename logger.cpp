#include <chrono>
#include <iomanip>
#include <ctime>
#include <iostream>

#include <logger.h>

#include "fmt/format.h"

logger::logger(const std::string& file_name_) : file_name(file_name_){
    log_file.open(file_name, std::ios::app);
    if (!log_file.is_open()) {
        std::cerr << "Failed to open log file: " << file_name << std::endl;
    }
}

logger::logger(logger&& other) noexcept :
    log_file(std::move(other.log_file)),
    file_name(std::move(other.file_name)) {}

logger::~logger() {
    if (log_file.is_open()) {
        log_file.close();
    }
}

void logger::log(log_level level, const std::string& message) {
    std::lock_guard guard(log_mutex);
    if (log_file.is_open()) {
        log_file << get_current_time() << " [" << log_level_to_string(level) << "] " << message << std::endl;
    }
}

std::string logger::get_current_time() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm local_tm = *std::localtime(&now_time);

    std::ostringstream oss;
    oss << std::put_time(&local_tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

std::string logger::log_level_to_string(log_level level) {
    switch (level) {
        case log_level::DEBUG: return "DEBUG";
        case log_level::INFO: return "INFO";
        case log_level::WARNING: return "WARNING";
        case log_level::ERROR: return "ERROR";
        case log_level::CRITICAL: return "CRITICAL";
        default: return "UNKNOWN";
    }
}
