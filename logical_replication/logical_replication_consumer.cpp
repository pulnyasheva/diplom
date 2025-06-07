#include <iostream>
#include <fmt/format.h>
#include <pqxx/pqxx>
#include <set>
#include <libpq-fe.h>

#include <logical_replication/logical_replication_consumer.h>
#include <logical_replication/logical_replication_parser.h>
#include <common/exception.h>

logical_replication_consumer::logical_replication_consumer(
    const std::string & connection_dsn_,
    std::shared_ptr<postgres::сonnection> connection_,
    const std::string & database_name_,
    const std::string &replication_slot_name_,
    const std::string &publication_name_,
    const std::string &start_lsn,
    size_t max_block_size_,
    logger *logger_,
    otterbrix_service *otterbrix_service_,
    std::pmr::memory_resource* resource_)
    : current_logger(logger_),
      replication_slot_name(replication_slot_name_),
      publication_name(publication_name_),
      database_name(database_name_),
      connection(std::move(connection_)),
      connection_dsn(connection_dsn_),
      current_lsn(start_lsn),
      result_lsn(start_lsn),
      current_lsn_value(get_lsn(start_lsn)),
      max_block_size(max_block_size_),
      current_postgres_settings(connection_dsn_, logger_),
      current_otterbrix_service(otterbrix_service_),
      resource(resource_){
}

uint64_t logical_replication_consumer::get_lsn(const std::string & lsn)
{
    uint32_t upper_half;
    uint32_t lower_half;
    std::sscanf(lsn.data(), "%X/%X", &upper_half, &lower_half);
    return (static_cast<uint64_t>(upper_half) << 32) + lower_half;
}

std::string logical_replication_consumer::lsn_to_string(uint64_t lsn_value) {
    uint32_t high_part = static_cast<uint32_t>(lsn_value >> 32);
    uint32_t low_part = static_cast<uint32_t>(lsn_value);
    return fmt::format("{:X}/{:X}", high_part, low_part);
}

std::vector<int32_t> logical_replication_consumer::get_primary_key(int32_t table_id) {
    if (id_to_primary_key.contains(table_id)) {
        return id_to_primary_key[table_id];
    }

    std::set<std::string> primary_key = current_postgres_settings.get_primary_key(id_to_table_name[table_id]);
    std::vector<int32_t> primary_key_columns;
    for (int32_t i = 0; i < id_table_to_column[table_id].size(); i++) {
        auto column = id_table_to_column[table_id][i];
        if (primary_key.contains(column.first)) {
            primary_key_columns.emplace_back(i);
        }
    }
    id_to_primary_key[table_id] = primary_key_columns;
    return id_to_primary_key[table_id];
}

void logical_replication_consumer::send_standby_status_update(PGconn *conn,
                                                              uint64_t received_lsn,
                                                              uint64_t flushed_lsn,
                                                              uint64_t applied_lsn,
                                                              bool request_reply_from_server = false) {
    const int reply_buf_size = 1 + 8 + 8 + 8 + 8 + 1;
    char reply_buf[reply_buf_size];
    int current_pos = 0;

    reply_buf[current_pos++] = 'r';

    uint64_t received_lsn_be = htobe64(received_lsn);
    uint64_t flushed_lsn_be = htobe64(flushed_lsn);
    uint64_t applied_lsn_be = htobe64(applied_lsn);

    memcpy(reply_buf + current_pos, &received_lsn_be, 8); current_pos += 8;
    memcpy(reply_buf + current_pos, &flushed_lsn_be, 8); current_pos += 8;
    memcpy(reply_buf + current_pos, &applied_lsn_be, 8); current_pos += 8;

    uint64_t client_time_us = 0;
    try {
        auto now = std::chrono::system_clock::now();
        auto pg_epoch = std::chrono::system_clock::from_time_t(946684800);
        auto diff = std::chrono::duration_cast<std::chrono::microseconds>(now - pg_epoch);
        client_time_us = diff.count();
    } catch (const std::exception& e) {
        current_logger->log_to_file(log_level::WARN, fmt::format("Could not get current time for feedback: {}", e.what()));
         client_time_us = 0;
    }

    uint64_t client_time_be = htobe64(client_time_us);
    memcpy(reply_buf + current_pos, &client_time_be, 8); current_pos += 8;

    reply_buf[current_pos++] = (request_reply_from_server ? 1 : 0);

    if (current_pos != reply_buf_size) {
        current_logger->log_to_file(log_level::ERR,
                                    fmt::format("Internal ERR: Feedback buffer size mismatch. Expected {}, got {}",
                                                reply_buf_size, current_pos));
    }

    if (PQputCopyData(conn, reply_buf, reply_buf_size) != 1) {
        current_logger->log_to_file(log_level::ERR,
                                    fmt::format("Failed to send feedback data (PQputCopyData): {}",
                                                PQerrorMessage(conn)));
    }

    int flush_result = PQflush(conn);
    if (flush_result < 0) {
        current_logger->log_to_file(log_level::ERR,
                                    fmt::format("Failed to flush feedback data (PQflush): {}", PQerrorMessage(conn)));
    }
}

PGconn *logical_replication_consumer::start_replication() {
    const std::string connection_dsn_replication = fmt::format("{}?replication=database", connection_dsn);
    PGconn *conn = PQconnectdb(connection_dsn_replication.c_str());

    if (PQstatus(conn) != CONNECTION_OK) {
        current_logger->log_to_file(log_level::ERR,
                                    fmt::format("Connection to database failed: {}", PQerrorMessage(conn)));
        PQfinish(conn);
    }

    const std::string start_repl_query = fmt::format(
        "START_REPLICATION SLOT {} LOGICAL {} (\"proto_version\" '1', \"publication_names\" '{}')",
        replication_slot_name,
        current_lsn,
        publication_name);

    PGresult *res = PQexec(conn, start_repl_query.c_str());

    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        current_logger->log_to_file(log_level::ERR,
                                    fmt::format("START_REPLICATION failed: {}", PQerrorMessage(conn)));
        current_logger->log_to_file(log_level::ERR,
                                    fmt::format("Result status: {}", PQresStatus(PQresultStatus(res))));
        PQclear(res);
        PQfinish(conn);
    }

    current_logger->log_to_file(log_level::INFO,
                                fmt::format("Replication started successfully from LSN {}", current_lsn));
    PQclear(res);
    return conn;
}

bool logical_replication_consumer::consume()
{
    PGconn *conn = start_replication();
    current_lsn_value = get_lsn(current_lsn);
    last_processed_lsn = current_lsn_value;
    last_received_lsn = current_lsn_value;

    try
    {
        logical_replication_parser parser = logical_replication_parser(&current_lsn, &result_lsn, &is_committed, current_logger);

        char *copy_buf = nullptr;
        auto last_feedback_time = std::chrono::steady_clock::now();
        const auto feedback_interval = std::chrono::seconds(10);

        while (true) {
            if (copy_buf) {
                PQfreemem(copy_buf);
                copy_buf = nullptr;
            }

            int bytes_read = PQgetCopyData(conn, &copy_buf, 0);

            if (bytes_read == -2) {
                current_logger->log_to_file(log_level::ERR, "End of replication stream.");
                break;
            }

            if (bytes_read == -1) {
                current_logger->log_to_file(log_level::ERR,
                                            fmt::format("Failed to read replication data: {}", PQerrorMessage(conn)));
                break;
            }

            if (bytes_read == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            // std::cout << "bytes read" << std::endl;

            const char *data = copy_buf;
            size_t len = static_cast<size_t>(bytes_read);

            char message_type = data[0]; // type message

            if (message_type == 'k') { // Primary keep-alive message
                if (len < 1 + 8 + 8 + 1) {
                    current_logger->log_to_file(log_level::ERR, fmt::format("Received truncated KeepAlive message."));
                    continue;
                }

                uint64_t server_lsn_be;
                memcpy(&server_lsn_be, data + 1, 8);
                uint64_t server_lsn = be64toh(server_lsn_be);
                last_received_lsn = std::max(last_received_lsn, server_lsn);

                bool reply_required = (data[1 + 8 + 8] == 1);

                if (reply_required) {
                    current_logger->log_to_file(log_level::ERR, fmt::format("Sending immediate feedback. Processed LSN: {}", lsn_to_string(last_processed_lsn)));
                    send_standby_status_update(conn, last_received_lsn, last_processed_lsn, last_processed_lsn, false);
                    last_feedback_time = std::chrono::steady_clock::now();
                }

            } else if (message_type == 'w') { // XLogData message
                if (len < 1 + 8 + 8 + 8) {
                    current_logger->log_to_file(log_level::ERR, "Received truncated XLogData message.");
                    continue;
                }

                uint64_t start_lsn_be, end_lsn_be, server_time_be;
                memcpy(&start_lsn_be, data + 1, 8);
                memcpy(&end_lsn_be, data + 1 + 8, 8);
                memcpy(&server_time_be, data + 1 + 8 + 8, 8);
                uint64_t wal_start_lsn = be64toh(start_lsn_be);
                uint64_t wal_end_lsn = be64toh(end_lsn_be);

                last_received_lsn = std::max(last_received_lsn, wal_end_lsn);

                const char* logical_data_start = data + 1 + 8 + 8 + 8;
                size_t logical_data_len = len - (1 + 8 + 8 + 8);

                current_logger->log_to_file(log_level::INFO, fmt::format(
                                                "Received XLogData. LSN Range: [{}, {}). Size: {}",
                                                lsn_to_string(wal_start_lsn), lsn_to_string(wal_end_lsn),
                                                logical_data_len));

                std::stringstream ss_hex_logical_replication;
                ss_hex_logical_replication << "\\x";

                for (int i = 0; i < logical_data_len; ++i) {
                    unsigned char current_byte = static_cast<unsigned char>(logical_data_start[i]);
                    ss_hex_logical_replication << std::hex
                            << std::setw(2)
                            << std::setfill('0')
                            << static_cast<int>(current_byte);
                }

                std::string compact_hex_logical_replication = ss_hex_logical_replication.str();

                try
                {
                    std::vector<std::string> result;
                    std::unordered_map<int32_t, std::string> old_value;
                    postgre_sql_type_operation type_operation;
                    int32_t table_id_query;
                    parser.parse_binary_data(compact_hex_logical_replication.c_str(),
                                             logical_data_len * 2 + 2,
                                             type_operation,
                                             table_id_query,
                                             result,
                                             id_to_table_name,
                                             id_skip_table_name,
                                             id_table_to_column,
                                             old_value);
                    current_otterbrix_service->data_handler(type_operation, id_to_table_name[table_id_query],
                                                            database_name,
                                                            get_primary_key(table_id_query), result,
                                                            id_table_to_column[table_id_query], old_value, resource);
                }
                catch (const exception &e)
                {
                    current_logger->log_to_file(log_level::ERR, fmt::format("ERR during parsing: {}", e.what()));
                }

                last_processed_lsn = last_received_lsn;
            } else {
                current_logger->log_to_file(log_level::WARN, fmt::format("Received unknown message type: {}", message_type));
            }

            auto now = std::chrono::steady_clock::now();
            if (now - last_feedback_time >= feedback_interval) {
                current_logger->log_to_file(log_level::INFO, fmt::format("Sending periodic feedback. Processed LSN: {}", lsn_to_string(last_processed_lsn)));
                send_standby_status_update(conn, last_received_lsn, last_processed_lsn, last_processed_lsn, false);
                last_feedback_time = now;
            }

            if (PQconsumeInput(conn) == 0) {
                current_logger->log_to_file(log_level::ERR,
                                            fmt::format("Failed to consume input: {}", PQerrorMessage(conn)));
                break;
            }
        }

        current_logger->log_to_file(log_level::INFO, "Stopping replication client.");
        if (copy_buf) {
            PQfreemem(copy_buf);
        }

        PQfinish(conn);
    }
    catch (const exception &e)
    {
        current_logger->log_to_file(log_level::ERR, fmt::format("Exception thrown in consume", e.what()));
        return false;
    }
    catch (const std::exception & e)
    {
        current_logger->log_to_file(log_level::ERR, e.what());
        return false;
    }

    return true;
}
