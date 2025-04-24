#include <fmt/format.h>

#include <common/exception.h>

exception::exception(error_codes code, const std::string &message)
    : std::runtime_error(message), error_code(code) {
}

int exception::code() const {
    return static_cast<int>(error_code);
}

void exception::add_context(const std::string &context) {
    message += " | Context: " + context;
}

const char* exception::what() const noexcept {
    std::string full_message = fmt::format("Error Code: {}, Message: {}", std::to_string(code()), message);
    return full_message.c_str();
}
