#include <cassert>
#include <iostream>
#include <set>
#include <vector>

#include <LogicalReplicationParser.h>
#include <Exception.h>

LogicalReplicationParser::LogicalReplicationParser(
    std::string *current_lsn_,
    std::string *final_lsn_,
    bool *committed_,
    Logger *logger_)
    : current_lsn(current_lsn_),
      final_lsn(final_lsn_),
      committed(committed_),
      logger(logger_) {
}

uint8_t LogicalReplicationParser::hexCharToDigit(char c) {
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

uint8_t LogicalReplicationParser::hex2CharToDigit(const char *data) {
    return  hexCharToDigit(data[0]) * 0x10 + hexCharToDigit(data[1]);
}

template<typename T>
T LogicalReplicationParser::hexNCharToT(const char * message, size_t pos, size_t n)
{
    T result = 0;
    for (size_t i = 0; i < n; ++i)
    {
        if (i) result <<= 8;
        result |= static_cast<uint32_t>(hex2CharToDigit(message + pos + 2 * i));
    }
    return result;
}

int8_t LogicalReplicationParser::parseInt8(const char * message, size_t & pos, size_t size)
{
    if (size < pos + 2) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Message small from parse int8");
    }
    int8_t result = hex2CharToDigit(message + pos);
    pos += 2;
    return result;
}

int16_t LogicalReplicationParser::parseInt16(const char * message, size_t & pos, size_t size)
{
    if (size < pos + 4) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Message small from parse int16");
    }
    int16_t result = hexNCharToT<int16_t>(message, pos, 2);
    pos += 4;
    return result;
}

int32_t LogicalReplicationParser::parseInt32(const char * message, size_t & pos, size_t size)
{
    if (size < pos + 8) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Message small from parse int32");
    }
    int32_t result = hexNCharToT<int32_t>(message, pos, 4);
    pos += 8;
    return result;
}

int64_t LogicalReplicationParser::parseInt64(const char * message, size_t & pos, size_t size)
{
    if (size < pos + 16) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Message small from parse int64");
    }
    int64_t result = hexNCharToT<uint64_t>(message, pos, 8);
    pos += 16;
    return result;
}

void LogicalReplicationParser::parseString(const char * message, size_t & pos, size_t size, std::string & result)
{
    if (size < pos + 2) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Message small from parse string");
    }
    char current = hex2CharToDigit(message + pos);
    pos += 2;
    while (pos < size && current != '\0')
    {
        result += current;
        current = hex2CharToDigit(message + pos);
        pos += 2;
    }
}

void LogicalReplicationParser::parseChangeData(
    const char *message,
    size_t &pos,
    size_t size,
    PostgreSQLTypeOperation type,
    bool old_value = false)
{
    int16_t num_columns = parseInt16(message, pos, size);

    auto proccess_column_value = [&](int8_t identifier_data)
    {
        logger->log(LogLevel::DEBUG, fmt::format("Identifier data: {}", identifier_data));
        switch (identifier_data)
        {
            case 'n': /// NULL
            {
                // Вставляем в целевую какую-то дефолтную константу
                break;
            }
            case 't': /// Text
            {
                int32_t col_len = parseInt32(message, pos, size);
                std::string value;
                for (int32_t i = 0; i < col_len; ++i)
                    value += static_cast<char>(parseInt8(message, pos, size));

                std::cout << "value: " << value << std::endl;
                // Вставляем в целевую таблицу
                break;
            }
            case 'u': /// Values that are too large to be stored directly.
            {
                logger->log(LogLevel::WARNING, "Values that are too large to be stored directly");
                // Вставляем в целевую какую-то дефолтную константу
                break;
            }
            case 'b': /// Binary data.
            {
                // Вставляем в целевую какую-то дефолтную константу (такие данные не будут обработаны)
                break;
            }
            default:
            {
                logger->log(LogLevel::WARNING, fmt::format("Unexpected identifier: {}", identifier_data));

                // Вставляем в целевую какую-то дефолтную константу
                break;
            }
        }
    };

    for (int column_idx = 0; column_idx < num_columns; ++column_idx)
    {
        try
        {
            proccess_column_value(parseInt8(message, pos, size));
        }
        catch (std::exception &e)
        {
            logger->log(LogLevel::ERROR,
                        fmt::format(
                            "Got error while receiving value for column {}, will insert default value. Error: {}",
                            column_idx, e.what()));

            // Вставляем в целевую какую-то дефолтную константу
        }
    }

    switch (type)
    {
        case PostgreSQLTypeOperation::INSERT:
        {
            // В целевую вставить

            break;
        }
        case PostgreSQLTypeOperation::DELETE:
        {
            // В целевой удалить

            break;
        }
        case PostgreSQLTypeOperation::UPDATE:
        {
            // if (old_value)
            //     // Обработка
            // else
            //     // Обработка

            break;
        }
    }
}

void LogicalReplicationParser::parseBinaryData(const char * replication_message, size_t size)
{
    // Skip '\x'
    size_t pos = 2;
    char type = parseInt8(replication_message, pos, size);
    logger->log(LogLevel::DEBUG, fmt::format("Message type: {}, lsn string: {}", type, *current_lsn));

    logger->log(LogLevel::DEBUG, fmt::format("Type operation: {}", type));
    std::cout << "Type operation: " << type << std::endl;
    switch (type)
    {
        case 'B': // Begin
        {
            parseInt64(replication_message, pos, size); // skip transaction end lsn
            parseInt64(replication_message, pos, size); // skip timestamp transaction commit
            break;
        }
        case 'I': // Insert
        {
            int32_t table_id = parseInt32(replication_message, pos, size);
            const auto & table_name = id_to_table_name[table_id];
            logger->log(LogLevel::DEBUG, fmt::format("Table name for insert: ", table_name));

            if (table_name.empty())
            {
                logger->log(LogLevel::WARNING, fmt::format("No table mapping for table id: {}.", table_id));
                return;
            }

            // Если в целевую таблицу добавлять не нужно, то return

            int8_t new_data = parseInt8(replication_message, pos, size);

            if (new_data)
                parseChangeData(replication_message, pos, size, PostgreSQLTypeOperation::INSERT);

            break;
        }
        case 'U': // Update
        {
            int32_t table_id = parseInt32(replication_message, pos, size);
            const auto & table_name = id_to_table_name[table_id];
            logger->log(LogLevel::DEBUG, fmt::format("Table name for update: ", table_name));

            if (table_name.empty()) {
                logger->log(LogLevel::WARNING,
                            fmt::format("No table mapping for table id: {}.", table_id));
                return;
            }

            // Если в целевую таблицу добавлять не нужно, то return

            auto proccess_identifier = [&](int8_t identifier) -> bool
            {
                bool read_next = true;
                switch (identifier)
                {
                    /// Old row.
                    case 'K': [[fallthrough]];
                    case 'O':
                    {
                        parseChangeData(replication_message, pos, size, PostgreSQLTypeOperation::UPDATE, true);
                        break;
                    }
                    case 'N':
                    {
                        /// New row.
                        parseChangeData(replication_message, pos, size, PostgreSQLTypeOperation::UPDATE);
                        read_next = false;
                        break;
                    }
                }

                return read_next;
            };

            /// Read either 'K' or 'O'. Never both of them. Also possible not to get both of them.
            bool read_next = proccess_identifier(parseInt8(replication_message, pos, size));

            /// 'N'. Always present, but could come in place of 'K' and 'O'.
            if (read_next)
                proccess_identifier(parseInt8(replication_message, pos, size));

            break;
        }
        case 'D': // Delete
        {
            int32_t table_id = parseInt32(replication_message, pos, size);
            const auto & table_name = id_to_table_name[table_id];
            logger->log(LogLevel::DEBUG, fmt::format("Table name for delete: ", table_name));

            if (table_name.empty())
            {
                logger->log(LogLevel::WARNING,
                            fmt::format("No table mapping for table id: {}.", table_id));
                return;
            }
            
            // Если данные целевой таблице не нужны, то return

            // skip replica identity
            parseInt8(replication_message, pos, size);

            parseChangeData(replication_message, pos, size, PostgreSQLTypeOperation::DELETE);
            break;
        }
        case 'C': // Commit
        {
            constexpr size_t unused_flags_len = 1;
            constexpr size_t commit_lsn_len = 8;
            constexpr size_t transaction_end_lsn_len = 8;
            constexpr size_t transaction_commit_timestamp_len = 8;
            pos += unused_flags_len + commit_lsn_len + transaction_end_lsn_len + transaction_commit_timestamp_len;

            final_lsn = current_lsn;
            *committed = true;
            break;
        }
        case 'R': // Relation
        {
            int32_t table_id = parseInt32(replication_message, pos, size);
            logger->log(LogLevel::DEBUG, fmt::format("Table id: ", table_id));

            std::string shema_namespace;
            std::string shema_name;
            parseString(replication_message, pos, size, shema_namespace);
            parseString(replication_message, pos, size, shema_name);

            std::string table_name;
            if (!shema_namespace.empty())
                table_name = shema_namespace + '.' + shema_name;
            else
                table_name = shema_name;

            logger->log(LogLevel::DEBUG, fmt::format("Table name: ", table_name));

            if (!id_to_table_name.contains(table_id))
                id_to_table_name[table_id] = table_name;

            // Если сохранять в таблицу ничего не надо, то return

            // Если сохранять в таблицу, сохранить

            // 'n' и 'f' не обрабатываются
            char table_identity = parseInt8(replication_message, pos, size);
            if (table_identity != 'd' && table_identity != 'i')
            {
                logger->log(LogLevel::WARNING, fmt::format("Invalid identity: {}", table_identity));
                id_skip_table_name.emplace(table_id);
                return;
            }

            int16_t num_columns = parseInt16(replication_message, pos, size);

            // Нужно проверить что в таблице, куда реплицируем нужное количество столбцов

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
            std::vector<std::pair<std::string, uint32_t>> columns;

            for (uint16_t i = 0; i < num_columns; ++i)
            {
                std::string column_name;
                parseInt8(replication_message, pos, size); // identity index for replica column
                parseString(replication_message, pos, size, column_name);
                logger->log(LogLevel::DEBUG, fmt::format("Column name: ", column_name));

                // Проверка, что в целевой таблице есть такая column (если нет, то тоже можно пометить, как скипнутую и сделать return)

                data_type_id = parseInt32(replication_message, pos, size);
                type_modifier = parseInt32(replication_message, pos, size);

                columns.push_back({column_name, data_type_id});
                replication_columns.emplace_back(column_name);
            }
            break;
        }
        case 'O': // Origin
            break;
        case 'Y': // Type
            break;
        case 'T': // Truncate
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    fmt::format("Unexpected byte1 value {} while parsing replication message", type));
    }
}
