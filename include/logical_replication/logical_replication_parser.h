#pragma once

#include <unordered_map>
#include <unordered_set>

#include <common/logger.h>
#include <logical_replication/logical_replication_consumer.h>
#include <postgres/postgres_types.h>

const std::string emptyValue = "NULL";

class logical_replication_parser {
public:
    logical_replication_parser(
        std::string *current_lsn_,
        std::string *result_lsn_,
        bool *is_committed_,
        logger *logger_);

    void parse_binary_data(const char *replication_message,
                         size_t size,
                         postgre_sql_type_operation &type_operation,
                         int32_t &table_id_query,
                         std::vector<std::string> &result,
                         std::unordered_map<int32_t, std::string> &id_to_table_name,
                         std::unordered_set<int32_t> &id_skip_table_name,
                         std::unordered_map<int32_t, std::vector<std::pair<std::string, int32_t>>> &id_table_to_column,
                         std::unordered_map<int32_t, std::string>& old_value);

private:
    void parse_change_data(const char *message,
                         size_t &pos,
                         size_t size,
                         std::vector<std::string> &result,
                         std::unordered_map<int32_t, std::string> &old_result,
                         bool old_value);

    uint8_t hex_char_to_digit(char c);

    uint8_t hex_2_char_to_digit(const char * data);

    template<typename T>
    T hex_n_char_to_t(const char * message, size_t pos, size_t n);

    int8_t parse_int8(const char * message, size_t & pos, size_t size);

    int16_t parse_int16(const char * message, size_t & pos, size_t size);

    int32_t parse_int32(const char * message, size_t & pos, size_t size);

    int64_t parse_int64(const char * message, size_t & pos, size_t size);

    void parse_string(const char * message, size_t & pos, size_t size, std::string & result);

    bool *is_committed;

    std::string *current_lsn, *result_lsn;
    logger *current_logger;
};
