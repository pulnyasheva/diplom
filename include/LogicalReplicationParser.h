#pragma once

#include <unordered_map>
#include <unordered_set>

#include <Logger.h>

enum class PostgreSQLTypeOperation : uint8_t
{
    INSERT,
    UPDATE,
    DELETE
};

class LogicalReplicationParser {
public:
    LogicalReplicationParser(
        std::string *current_lsn_,
        std::string *result_lsn_,
        bool *is_committed_,
        Logger *logger_);

    void parseBinaryData(const char * replication_message, size_t size);

private:
    void parseChangeData(const char * message, size_t & pos, size_t size, PostgreSQLTypeOperation type, bool old_value);

    uint8_t hexCharToDigit(char c);

    uint8_t hex2CharToDigit(const char * data);

    template<typename T>
    T hexNCharToT(const char * message, size_t pos, size_t n);

    int8_t parseInt8(const char * message, size_t & pos, size_t size);

    int16_t parseInt16(const char * message, size_t & pos, size_t size);

    int32_t parseInt32(const char * message, size_t & pos, size_t size);

    int64_t parseInt64(const char * message, size_t & pos, size_t size);

    void parseString(const char * message, size_t & pos, size_t size, std::string & result);

    std::unordered_map<int32_t, std::string> id_to_table_name;
    std::unordered_set<int32_t> id_skip_table_name;

    bool *is_committed;

    std::string *current_lsn, *result_lsn;
    Logger *logger;
};
