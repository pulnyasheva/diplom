#pragma once

#include <stdexcept>

enum class ErrorCodes {
    BAD_ARGUMENTS = 1,
    LOGICAL_ERROR = 2,
    NOT_FOUND = 3,
    INVALID_INPUT = 4
};

class Exception : public std::runtime_error {
public:
    Exception(ErrorCodes code, const std::string &message);

    int code() const;

    void addContext(const std::string &context);

    const char* what() const noexcept override;

private:
    ErrorCodes error_code;
    std::string message;
};
