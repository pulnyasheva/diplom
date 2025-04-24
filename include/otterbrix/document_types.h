#pragma once

#include <cstdint>
#include <iostream>
#include <unordered_map>

enum class document_types : uint8_t
{
    BOOL = 0,
    INT8 = 1,
    INT16 = 2,
    INT32 = 3,
    INT64 = 4,
    FLOAT = 5,
    DOUBLE = 6,
    STRING = 7,
    ARRAY = 8,
    DICT = 9,
    NA = 127, // NULL
    INVALID = 255,
};

inline std::ostream& operator<<(std::ostream& os, document_types type) {
    const static std::unordered_map<document_types, std::string> document_types_to_str = {
        {document_types::BOOL, "BOOL"},
        {document_types::INT8, "INT8"},
        {document_types::INT16, "INT16"},
        {document_types::INT32, "INT32"},
        {document_types::INT64, "INT64"},
        {document_types::FLOAT, "FLOAT"},
        {document_types::DOUBLE, "DOUBLE"},
        {document_types::STRING, "STRING"},
        {document_types::NA, "NA"}, // NULL
        {document_types::INVALID, "INVALID"}};

    os << document_types_to_str.at(type);
    return os;
}