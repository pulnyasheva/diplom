#include <cassert>
#include <iostream>
#include <vector>

#include <logical_replication_parser.h>
#include <exception.h>

logical_replication_parser::logical_replication_parser(
    std::string *current_lsn_,
    std::string *result_lsn_,
    bool *is_committed_,
    logger *logger_)
    : current_lsn(current_lsn_),
      result_lsn(result_lsn_),
      is_committed(is_committed_),
      current_logger(logger_) {
}

uint8_t logical_replication_parser::hex_char_to_digit(char c) {
    if (c >= '0' && c <= '9') {
        return static_cast<uint8_t>(c - '0'); //'0'-'9'
    }
    if (c >= 'A' && c <= 'F') {
        return static_cast<uint8_t>(c - 'A' + 10); //'A'-'F'
    }
    if (c >= 'a' && c <= 'f') {
        return static_cast<uint8_t>(c - 'a' + 10); //'a'-'f'
    }
    return 0xFF; //0xFF for other
}

uint8_t logical_replication_parser::hex_2_char_to_digit(const char *data) {
    return  hex_char_to_digit(data[0]) * 0x10 + hex_char_to_digit(data[1]);
}

template<typename T>
T logical_replication_parser::hex_n_char_to_t(const char * message, size_t pos, size_t n)
{
    T result = 0;
    for (size_t i = 0; i < n; ++i)
    {
        if (i) result <<= 8;
        result |= static_cast<uint32_t>(hex_2_char_to_digit(message + pos + 2 * i));
    }
    return result;
}

int8_t logical_replication_parser::parse_int8(const char * message, size_t & pos, size_t size)
{
    if (size < pos + 2) {
        throw exception(error_codes::LOGICAL_ERROR, "Message small from parse int8");
    }
    int8_t result = hex_2_char_to_digit(message + pos);
    pos += 2;
    return result;
}

int16_t logical_replication_parser::parse_int16(const char * message, size_t & pos, size_t size)
{
    if (size < pos + 4) {
        throw exception(error_codes::LOGICAL_ERROR, "Message small from parse int16");
    }
    int16_t result = hex_n_char_to_t<int16_t>(message, pos, 2);
    pos += 4;
    return result;
}

int32_t logical_replication_parser::parse_int32(const char * message, size_t & pos, size_t size)
{
    if (size < pos + 8) {
        throw exception(error_codes::LOGICAL_ERROR, "Message small from parse int32");
    }
    int32_t result = hex_n_char_to_t<int32_t>(message, pos, 4);
    pos += 8;
    return result;
}

int64_t logical_replication_parser::parse_int64(const char * message, size_t & pos, size_t size)
{
    if (size < pos + 16) {
        throw exception(error_codes::LOGICAL_ERROR, "Message small from parse int64");
    }
    int64_t result = hex_n_char_to_t<int64_t>(message, pos, 8);
    pos += 16;
    return result;
}

void logical_replication_parser::parse_string(const char * message, size_t & pos, size_t size, std::string & result)
{
    if (size < pos + 2) {
        throw exception(error_codes::LOGICAL_ERROR, "Message small from parse string");
    }
    char current = hex_2_char_to_digit(message + pos);
    pos += 2;
    while (pos < size && current != '\0')
    {
        result += current;
        current = hex_2_char_to_digit(message + pos);
        pos += 2;
    }
}

void logical_replication_parser::parse_change_data(const char *message,
                                               size_t &pos,
                                               size_t size,
                                               std::vector<std::string>& result,
                                               std::unordered_map<int32_t, std::string>& old_result,
                                               bool old_value = false)
{
    int16_t num_columns = parse_int16(message, pos, size);
    result.resize(num_columns);

    auto proccess_column_value = [&](int8_t identifier_data, int16_t column_idx)
    {
        current_logger->log_to_file(log_level::DEBUG, fmt::format("Identifier data: {}", identifier_data));
        switch (identifier_data)
        {
            case 'n': /// NULL
            {
                if (!old_value)
                    result[column_idx] = emptyValue;

                break;
            }
            case 't': /// Text
            {
                int32_t col_len = parse_int32(message, pos, size);
                std::string value;
                for (int32_t i = 0; i < col_len; ++i)
                    value += static_cast<char>(parse_int8(message, pos, size));

                if (old_value)
                    old_result[column_idx] = value;
                else
                    result[column_idx] = value;
                std::cout << "value: " << value << std::endl;
                break;
            }
            case 'u': /// Values that are too large (TOAST).
            {
                current_logger->log_to_file(log_level::WARNING,
                            fmt::format("Values too large in column: {}", column_idx));
                if (old_value)
                    old_result[column_idx] = emptyValue;
                else
                    result[column_idx] = emptyValue;
                break;
            }
            case 'b': /// Binary data.
            {
                int32_t col_len = parse_int32(message, pos, size);
                std::string binary_data;
                binary_data.resize(col_len);

                for (int32_t i = 0; i < col_len; ++i) {
                    binary_data[i] = static_cast<char>(parse_int8(message, pos, size));
                }

                if (old_value)
                    old_result[column_idx] = std::move(binary_data);
                else
                    result[column_idx] = std::move(binary_data);

                break;
            }
            default:
            {
                current_logger->log_to_file(log_level::WARNING, fmt::format("Unexpected identifier: {}", identifier_data));
                break;
            }
        }
    };

    for (int16_t column_idx = 0; column_idx < num_columns; ++column_idx)
    {
        try
        {
            proccess_column_value(parse_int8(message, pos, size), column_idx);
        }
        catch (std::exception &e)
        {
            current_logger->log_to_file(log_level::ERROR,
                        fmt::format(
                            "Got error while receiving value for column {} with {}",
                            column_idx, e.what()));

        }
    }
}

void logical_replication_parser::parse_binary_data(const char *replication_message,
                                               size_t size,
                                               postgre_sql_type_operation& type_operation,
                                               int32_t& table_id,
                                               std::vector<std::string>& result,
                                               std::unordered_map<int32_t, std::string>& id_to_table_name,
                                               std::unordered_set<int32_t>& id_skip_table_name,
                                               std::unordered_map<int32_t, std::vector<std::pair<std::string, int32_t>>>& id_table_to_column,
                                               std::unordered_map<int32_t, std::string>& old_value)
{
    // Skip '\x'
    size_t pos = 2;
    char type = parse_int8(replication_message, pos, size);
    current_logger->log_to_file(log_level::DEBUG, fmt::format("Message type: {}, lsn string: {}", type, *current_lsn));

    current_logger->log_to_file(log_level::DEBUG, fmt::format("Type operation: {}", type));
    std::cout << "Type operation: " << type << std::endl;
    switch (type)
    {
        case 'B': // Begin
        {
            parse_int64(replication_message, pos, size); // skip transaction end lsn
            parse_int64(replication_message, pos, size); // skip timestamp transaction commit
            type_operation = postgre_sql_type_operation::NOT_PROCESSED;
            break;
        }
        case 'I': // Insert
        {
            table_id = parse_int32(replication_message, pos, size);
            const auto & table_name = id_to_table_name[table_id];
            current_logger->log_to_file(log_level::DEBUG, fmt::format("Table name for insert: ", table_name));

            if (table_name.empty())
            {
                current_logger->log_to_file(log_level::WARNING, fmt::format("No table mapping for table id: {}.", table_id));
                return;
            }

            int8_t new_data = parse_int8(replication_message, pos, size);

            if (new_data)
                parse_change_data(replication_message, pos, size, result, old_value);
            type_operation = postgre_sql_type_operation::INSERT;
            break;
        }
        case 'U': // Update
        {
            table_id = parse_int32(replication_message, pos, size);
            const auto & table_name = id_to_table_name[table_id];
            current_logger->log_to_file(log_level::DEBUG, fmt::format("Table name for update: ", table_name));

            if (table_name.empty()) {
                current_logger->log_to_file(log_level::WARNING,
                            fmt::format("No table mapping for table id: {}.", table_id));
                return;
            }

            auto proccess_identifier = [&](int8_t identifier) -> bool
            {
                bool read_next = true;
                switch (identifier)
                {
                    case 'K':
                    case 'O': {
                        parse_change_data(replication_message, pos, size, result, old_value, true);
                        break;
                    }
                    case 'N': {
                        /// New row.
                        parse_change_data(replication_message, pos, size, result, old_value);
                        read_next = false;
                        break;
                    }
                }

                return read_next;
            };

            /// Read 'K' or 'O'
            bool read_next = proccess_identifier(parse_int8(replication_message, pos, size));

            /// Read 'N'
            if (read_next)
                proccess_identifier(parse_int8(replication_message, pos, size));

            type_operation = postgre_sql_type_operation::UPDATE;
            break;
        }
        case 'D': // Delete
        {
            table_id = parse_int32(replication_message, pos, size);
            const auto & table_name = id_to_table_name[table_id];
            current_logger->log_to_file(log_level::DEBUG, fmt::format("Table name for delete: ", table_name));

            if (table_name.empty())
            {
                current_logger->log_to_file(log_level::WARNING,
                            fmt::format("No table mapping for table id: {}.", table_id));
                return;
            }

            // skip replica identity
            parse_int8(replication_message, pos, size);

            parse_change_data(replication_message, pos, size, result, old_value);
            type_operation = postgre_sql_type_operation::DELETE;
            break;
        }
        case 'C': // Commit
        {
            constexpr size_t unused_flags_len = 1;
            constexpr size_t commit_lsn_len = 8;
            constexpr size_t transaction_end_lsn_len = 8;
            constexpr size_t transaction_commit_timestamp_len = 8;
            pos += unused_flags_len + commit_lsn_len + transaction_end_lsn_len + transaction_commit_timestamp_len;

            result_lsn = current_lsn;
            *is_committed = true;
            type_operation = postgre_sql_type_operation::NOT_PROCESSED;
            break;
        }
        case 'R': // Relation
        {
            table_id = parse_int32(replication_message, pos, size);
            current_logger->log_to_file(log_level::DEBUG, fmt::format("Table id: {}", table_id));

            std::string shema_namespace;
            std::string shema_name;
            parse_string(replication_message, pos, size, shema_namespace);
            parse_string(replication_message, pos, size, shema_name);

            std::string table_name;
            if (!shema_namespace.empty())
                table_name = shema_namespace + '.' + shema_name;
            else
                table_name = shema_name;

            current_logger->log_to_file(log_level::DEBUG, fmt::format("Table name: {}", table_name));

            if (!id_to_table_name.contains(table_id))
                id_to_table_name[table_id] = table_name;

            // 'n' и 'f' не обрабатываются
            char table_identity = parse_int8(replication_message, pos, size);
            if (table_identity != 'd' && table_identity != 'i')
            {
                current_logger->log_to_file(log_level::WARNING, fmt::format("Invalid identity: {}", table_identity));
                id_skip_table_name.emplace(table_id);
                return;
            }

            int16_t num_columns = parse_int16(replication_message, pos, size);

            /// bool - 16
            /// bytea - 17
            /// char - 18
            /// name - 19
            /// int8 - 20
            /// int2 - 21
            /// int4 - 23
            /// text - 25
            /// float4 - 700
            /// float8 - 701
            /// unknown - 705
            /// varchar - 1043
            /// date - 1082
            /// time - 1083
            /// timestamp - 1114
            /// bit - 1560
            /// numeric - 1700
            /// uuid - 2950
            int32_t data_type_id;
            int32_t type_modifier; // Дополнительные параметры типа

            std::vector<std::string> replication_columns;
            std::vector<std::pair<std::string, int32_t>> columns(num_columns);

            for (uint16_t i = 0; i < num_columns; ++i)
            {
                std::string column_name;
                parse_int8(replication_message, pos, size); // identity index for replica column
                parse_string(replication_message, pos, size, column_name);
                current_logger->log_to_file(log_level::DEBUG, fmt::format("Column name: {}", column_name));

                data_type_id = parse_int32(replication_message, pos, size);
                type_modifier = parse_int32(replication_message, pos, size);

                columns[i] = {column_name, data_type_id};
                replication_columns.emplace_back(column_name);
            }
            id_table_to_column[table_id] = columns;
            type_operation = postgre_sql_type_operation::NOT_PROCESSED;
            break;
        }
        case 'O': // Origin
        case 'Y': // Type
        case 'T': // Truncate
        {
            type_operation = postgre_sql_type_operation::NOT_PROCESSED;
            break;
        }
        default:
            throw exception(error_codes::LOGICAL_ERROR,
                    fmt::format("Unexpected byte1 value {}", type));
    }
}
