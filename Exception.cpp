#include <fmt/format.h>

#include <Exception.h>

Exception::Exception(ErrorCodes code, const std::string &message)
    : std::runtime_error(message), error_code(code) {
}

int Exception::code() const {
    return static_cast<int>(error_code);
}

void Exception::addContext(const std::string &context) {
    message += " | Context: " + context;
}

const char* Exception::what() const noexcept {
    std::string full_message = fmt::format("Error Code: {}, Message: {}", std::to_string(code()), message);
    return full_message.c_str();
}
