#include <chrono>
#include <iomanip>
#include <ctime>
#include <iostream>
#include <curl/curl.h>
#include "fmt/format.h"

#include <common/logger.h>

logger::logger(const std::string &file_name_, const std::string &url_)
    : file_name(file_name_),
      url(url_) {
    if (!file_name.empty()) {
        log_file.open(file_name, std::ios::app);
        if (!log_file.is_open()) {
            std::cerr << "Failed to open log file: " << file_name << std::endl;
        }
    }
}

logger::logger(logger&& other) noexcept :
    log_file(std::move(other.log_file)),
    file_name(std::move(other.file_name)),
    url(std::move(other.url)) {}

logger::~logger() {
    if (log_file.is_open()) {
        log_file.close();
    }
}

void logger::log_to_console(log_level level, const std::string& message) {
    if (level > LOG_LEVEL_PROJECT) {
        return;
    }

    std::cout << get_current_time() << " [" << log_level_to_string(level) << "] " << message << std::endl;
}

void logger::log_to_file(log_level level, const std::string& message) {
    if (level > LOG_LEVEL_PROJECT) {
        return;
    }

    std::lock_guard guard(log_mutex);
    if (log_file.is_open()) {
        log_file << get_current_time() << " [" << log_level_to_string(level) << "] " << message << std::endl;
    }
}

void logger::log_to_url(log_level level, const std::string& message) {
    if (level > LOG_LEVEL_PROJECT) {
        return;
    }

    std::lock_guard guard(log_mutex);

    std::ostringstream oss;
    oss << get_current_time() << " [" << log_level_to_string(level) << "] " << message;

    CURL* curl = curl_easy_init();
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, oss.str().c_str());

        CURLcode res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << std::endl;
        }

        curl_easy_cleanup(curl);
    } else {
        std::cerr << "Failed to initialize CURL" << std::endl;
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
        case log_level::DEBUGER: return "DEBUG";
        case log_level::INFO: return "INFO";
        case log_level::WARN: return "WARNING";
        case log_level::ERR: return "ERROR";
        case log_level::CRITICAL: return "CRITICAL";
        default: return "UNKNOWN";
    }
}
