#pragma once

#include <stdexcept>

enum class error_codes {
    BAD_ARGUMENTS = 1,
    LOGICAL_ERROR = 2,
    NOT_FOUND = 3,
    INVALID_INPUT = 4
};

class exception : public std::runtime_error {
public:
    exception(error_codes code, const std::string &message);

    int code() const;

    void add_context(const std::string &context);

    const char* what() const noexcept override;

private:
    error_codes error_code;
    std::string message;
};
