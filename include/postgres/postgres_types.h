#pragma once

#include <cstdint>
#include <iostream>
#include <unordered_map>

enum class postgre_sql_type_operation : uint8_t
{
    INSERT,
    UPDATE,
    DELETE,
    NOT_PROCESSED
};

enum class postgres_types : int32_t
{
    BOOL = 16,
    CHAR = 18,
    INT8 = 20,
    INT2 = 21,
    INT4 = 23,
    TEXT = 25,
    FLOAT = 700,
    DOUBLE = 701,
    VARCHAR = 1043,
    BIT = 1560,
    UUID = 2950,
    NUMERIC = 1700,
    ARRAY = -1
};

enum class postgres_array_types : int32_t
{
    BOOL = 1000,
    CHAR = 1002,
    INT8 = 1016,
    INT2 = 1005,
    INT4 = 1007,
    TEXT = 1009,
    FLOAT = 1021,
    DOUBLE = 1022,
    VARCHAR = 1015,
    BIT = 1561,
    UUID = 2951,
    NUMERIC = 1231,
};

static std::unordered_map<int32_t, postgres_types> str_to_document_types = {};

inline static postgres_types get_enum(int32_t num_value) {
    switch (num_value) {
        case static_cast<int32_t>(postgres_types::BOOL): return postgres_types::BOOL;
        case static_cast<int32_t>(postgres_types::CHAR): return postgres_types::CHAR;
        case static_cast<int32_t>(postgres_types::INT8): return postgres_types::INT8;
        case static_cast<int32_t>(postgres_types::INT2): return postgres_types::INT2;
        case static_cast<int32_t>(postgres_types::INT4): return postgres_types::INT4;
        case static_cast<int32_t>(postgres_types::TEXT): return postgres_types::TEXT;
        case static_cast<int32_t>(postgres_types::FLOAT): return postgres_types::FLOAT;
        case static_cast<int32_t>(postgres_types::DOUBLE): return postgres_types::DOUBLE;
        case static_cast<int32_t>(postgres_types::VARCHAR): return postgres_types::VARCHAR;
        case static_cast<int32_t>(postgres_types::BIT): return postgres_types::BIT;
        case static_cast<int32_t>(postgres_types::UUID): return postgres_types::UUID;
        case static_cast<int32_t>(postgres_types::NUMERIC): return postgres_types::NUMERIC;
        default: break;
    }

    switch (num_value) {
        case static_cast<int32_t>(postgres_array_types::BOOL):
        case static_cast<int32_t>(postgres_array_types::CHAR):
        case static_cast<int32_t>(postgres_array_types::INT8):
        case static_cast<int32_t>(postgres_array_types::INT2):
        case static_cast<int32_t>(postgres_array_types::INT4):
        case static_cast<int32_t>(postgres_array_types::TEXT):
        case static_cast<int32_t>(postgres_array_types::FLOAT):
        case static_cast<int32_t>(postgres_array_types::DOUBLE):
        case static_cast<int32_t>(postgres_array_types::VARCHAR):
        case static_cast<int32_t>(postgres_array_types::BIT):
        case static_cast<int32_t>(postgres_array_types::UUID):
        case static_cast<int32_t>(postgres_array_types::NUMERIC): return postgres_types::ARRAY;
        default: break;
    }

    auto it = str_to_document_types.find(num_value);
    if (it != str_to_document_types.end()) {
        return it->second;
    }

    throw std::invalid_argument("Invalid document type string: " + num_value);
}
