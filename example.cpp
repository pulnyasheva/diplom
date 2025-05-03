#include <iostream>
#include <string>
#include <libpq-fe.h>

#include <common/logger.h>
#include <logical_replication/logical_replication_handler.h>

bool exampleDatabaseQuery(const std::string conninfo) {
    try {
        pqxx::connection C(conninfo);
        if (!C.is_open()) {
            std::cerr << "Can't open database" << std::endl;
            return false;
        }

        pqxx::work tx(C);

        tx.exec("INSERT INTO students (first_name, last_name, age, email) "
                "VALUES ('John', 'Doe', 21, 'john.doe2@example.com'), "
                "('Lui', 'Doe', 21, 'lui.doe2@example.com'), "
                "('Lui', 'Brown', 27, 'lui.brown2@example.com');");

        // tx.exec("UPDATE students SET age = 50 WHERE email = 'john.doe@example.com';");

        // pqxx::result R = tx.exec("SELECT * FROM students");
        // for (size_t i = 0; i < R.columns(); ++i) {
        //     std::cout << R.column_name(i) << "\t";
        // }
        // std::cout << std::endl;
        //
        // for (const auto& row : R) {
        //     for (const auto& field : row) {
        //         std::cout << field.c_str() << "\t";
        //     }
        //     std::cout << std::endl;
        // }

        // tx.exec("UPDATE students SET id = 25 WHERE email = 'john.doe2@example.com';");
        //
        // tx.exec("DELETE FROM students WHERE first_name = 'Lui';");
        //
        // tx.exec("DELETE FROM students WHERE email = 'john.doe2@example.com';");

        tx.commit();
    } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
        return false;
    }

    return true;
}

// void send_standby_status_update(PGconn *conn, uint64_t received_lsn, uint64_t flushed_lsn, uint64_t applied_lsn, bool reply_requested) {
//     // Формат сообщения Standby Status Update (тип 'r')
//     // 1 байт: 'r'
//     // 8 байт: LSN последнего полученного сегмента WAL
//     // 8 байт: LSN последнего сброшенного на диск сегмента WAL
//     // 8 байт: LSN последнего примененного сегмента WAL
//     // 8 байт: Время отправки (в микросекундах с 2000-01-01 00:00:00)
//     // 1 байт: Флаг запроса ответа (0 или 1)
//
//     std::vector<char> feedback_message(1 + 8 + 8 + 8 + 8 + 1);
//     char *buf = feedback_message.data();
//     size_t offset = 0;
//
//     buf[offset] = 'r'; // identity message
//     offset += 1;
//
//     // LSN big-endian
//     uint64_t lsn_be = htobe64(received_lsn);
//     memcpy(buf + offset, &lsn_be, 8);
//     offset += 8;
//
//     lsn_be = htobe64(flushed_lsn);
//     memcpy(buf + offset, &lsn_be, 8);
//     offset += 8;
//
//     lsn_be = htobe64(applied_lsn);
//     memcpy(buf + offset, &lsn_be, 8);
//     offset += 8;
//
//     auto now = std::chrono::system_clock::now();
//     auto epoch_pg = std::chrono::system_clock::from_time_t(946684800); // 2000-01-01 00:00:00 UTC
//     auto diff_us = std::chrono::duration_cast<std::chrono::microseconds>(now - epoch_pg).count();
//     uint64_t time_be = htobe64(static_cast<uint64_t>(diff_us));
//     memcpy(buf + offset, &time_be, 8);
//     offset += 8;
//
//     buf[offset] = reply_requested ? 1 : 0;
//     offset += 1;
//
//     if (PQputCopyData(conn, buf, feedback_message.size()) <= 0) {
//         std::cerr << "ERROR: Failed to send standby status update: " << PQerrorMessage(conn) << std::endl;
//     }
// }

void send_standby_status_update(PGconn *conn,
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
         std::cerr << "Warning: Could not get current time for feedback: " << e.what() << std::endl;
         client_time_us = 0;
    }

    uint64_t client_time_be = htobe64(client_time_us);
    memcpy(reply_buf + current_pos, &client_time_be, 8); current_pos += 8;

    reply_buf[current_pos++] = (request_reply_from_server ? 1 : 0);

    if (current_pos != reply_buf_size) {
         std::cerr << "Internal error: Feedback buffer size mismatch. Expected "
                   << reply_buf_size << ", got " << current_pos << std::endl;
    }

    if (PQputCopyData(conn, reply_buf, reply_buf_size) != 1) {
        std::cerr << "ERROR: Failed to send feedback data (PQputCopyData): " << PQerrorMessage(conn) << std::endl;
    }

    int flush_result = PQflush(conn);
    if (flush_result < 0) {
        std::cerr << "ERROR: Failed to flush feedback data (PQflush): " << PQerrorMessage(conn) << std::endl;
    }
}

std::string lsn_to_string(uint64_t lsn_value) {
    uint32_t high_part = static_cast<uint32_t>(lsn_value >> 32);
    uint32_t low_part = static_cast<uint32_t>(lsn_value);
    return fmt::format("{:X}/{:X}", high_part, low_part);
}

uint64_t lsn_string_to_u64(const std::string& lsn_str) {
    if (lsn_str.empty() || lsn_str == "0/0") {
        return 0;
    }

    uint32_t high_part = 0;
    uint32_t low_part = 0;

    int fields_scanned = sscanf(lsn_str.c_str(), "%X/%X", &high_part, &low_part);

    if (fields_scanned == 2) {
        uint64_t result_lsn = (static_cast<uint64_t>(high_part) << 32) | low_part;
        return result_lsn;
    } else {
        throw std::runtime_error(fmt::format("Invalid LSN string format: '{}'", lsn_str));
    }
}

int main() {
    std::string conninfo = "postgresql://postgres:postgres@172.29.190.7:5432/postgres";
    const std::string slot = "postgres_students_ch_replication_slot";
    const std::string publication_name = " postgres_students_ch_publication";

    std::vector<std::string> tables = {"students"};
    logical_replication_handler replication_handler = logical_replication_handler(
        "postgres",
        "students",
        conninfo,
        "log.txt",
        "",
        tables,
        100);

    std::string start_lsn_string;
    replication_handler.start_synchronization(start_lsn_string);

    // if (!exampleDatabaseQuery(conninfo)) {
    //     return 1;
    // }
    //
    // std::cout << replication_handler.run_consumer() << std::endl;

    conninfo = "postgresql://postgres:postgres@172.29.190.7:5432/postgres?replication=database";
    PGconn *conn = PQconnectdb(conninfo.c_str());

    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return 1;
    }

    std::string start_repl_query = fmt::format(
        "START_REPLICATION SLOT {} LOGICAL {} (\"proto_version\" '1', \"publication_names\" '{}')",
        slot,
        start_lsn_string,
        publication_name);

    PGresult *res = PQexec(conn, start_repl_query.c_str());

    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        std::cerr << "START_REPLICATION failed: " << PQerrorMessage(conn) << std::endl;
        std::cerr << "Result status: " << PQresStatus(PQresultStatus(res)) << std::endl;
        PQclear(res);
        PQfinish(conn);
        return 1;
    }

    std::cout << "Replication started successfully from LSN " << start_lsn_string << std::endl;
    PQclear(res);

    uint64_t current_lsn = lsn_string_to_u64(start_lsn_string);
    uint64_t last_processed_lsn = current_lsn;
    uint64_t last_received_lsn = current_lsn;

    char *copy_buf = nullptr;
    auto last_feedback_time = std::chrono::steady_clock::now();
    const auto feedback_interval = std::chrono::seconds(2);

    while (true) {
        if (copy_buf) {
            PQfreemem(copy_buf);
            copy_buf = nullptr;
        }

        int bytes_read = PQgetCopyData(conn, &copy_buf, 0);
        // std:: cout << bytes_read << std::endl;
        // std::cout << "buf " << copy_buf << std::endl;
        // std::string raw_data_as_string(copy_buf, bytes_read);

        // std::cout << "Converted raw data to std::string. String length: "
        //               << raw_data_as_string.length() << std::endl;
        // if (raw_data_as_string.length() != static_cast<size_t>(bytes_read)) {
        //     std::cerr << "WARNING: String length mismatch after conversion!" << std::endl;
        // }
        // std::cout << "String Content (Hex Dump):" << std::endl;
        // const int bytes_per_line = 16;
        // // Итерируем по символам СОЗДАННОЙ СТРОКИ
        // for (size_t i = 0; i < raw_data_as_string.length(); ++i) {
        //     // Доступ к символу строки через оператор []
        //     unsigned char current_byte = static_cast<unsigned char>(raw_data_as_string[i]);
        //
        //     std::cout << std::hex << std::setw(2) << std::setfill('0')
        //               << static_cast<int>(current_byte) // Выводим числовое значение байта в HEX
        //               << " ";
        //
        //     if ((i + 1) % bytes_per_line == 0) {
        //         std::cout << std::endl;
        //     }
        // }
        // if (raw_data_as_string.length() % bytes_per_line != 0) {
        //     std::cout << std::endl;
        // }
        // std::cout << std::dec; // Возвращаем десятичный вывод
        // std::cout << "--- End of String Content Dump ---" << std::endl;

        if (bytes_read == -2) {
            std::cerr << "ERROR: End of replication stream." << std::endl;
            break;
        } else if (bytes_read == -1) {
            std::cerr << "ERROR: Failed to read replication data: " << PQerrorMessage(conn) << std::endl;
            break;
        } else if (bytes_read == 0) {
             std::this_thread::sleep_for(std::chrono::milliseconds(10)); // Небольшая пауза
             continue;
        } else {
            const char *data = copy_buf;
            size_t len = static_cast<size_t>(bytes_read);

            char message_type = data[0]; // type message

            if (message_type == 'k') { // Primary keep-alive message
                std::cout << "type k " << std::endl;
                // Сообщение содержит:
                // 8 байт: LSN конца WAL на сервере (Big Endian)
                // 8 байт: Текущее время сервера (мкс с 2000-01-01) (Big Endian)
                // 1 байт: Запрос ответа (1 = требуется немедленный ответ)

                if (len < 1 + 8 + 8 + 1) {
                     std::cerr << "ERROR: Received truncated KeepAlive message." << std::endl;
                     continue;
                }

                uint64_t server_lsn_be;
                memcpy(&server_lsn_be, data + 1, 8);
                uint64_t server_lsn = be64toh(server_lsn_be);
                last_received_lsn = std::max(last_received_lsn, server_lsn);

                bool reply_required = (data[1 + 8 + 8] == 1);

                std::cout << fmt::format("Received KeepAlive from server. Server LSN: {}. Reply required: {}",
                                         lsn_to_string(server_lsn), reply_required) << std::endl;

                if (reply_required) {
                    std::cout << fmt::format("Sending immediate feedback. Processed LSN: {}, {}", lsn_to_string(last_received_lsn), lsn_to_string(last_processed_lsn)) << std::endl;
                    send_standby_status_update(conn, last_received_lsn, last_processed_lsn, last_processed_lsn, false);
                    last_feedback_time = std::chrono::steady_clock::now();
                    std::cout << "Sent immediate feedback reply." << std::endl;
                }

            } else if (message_type == 'w') { // XLogData message
                std::cout << "type w" << std::endl;
                // Сообщение содержит:
                // 8 байт: Начальный LSN данных WAL (Big Endian)
                // 8 байт: Конечный LSN данных WAL (Big Endian)
                // 8 байт: Время отправки сервером (мкс с 2000-01-01) (Big Endian)
                // N байт: Сами данные WAL (которые содержат логические изменения Begin/Commit/Insert/...)

                if (len < 1 + 8 + 8 + 8) {
                     std::cerr << "ERROR: Received truncated XLogData message." << std::endl;
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

                std::cout << "Payload length: " << logical_data_len << " bytes" << std::endl;

                std::cout << fmt::format("Received XLogData. LSN Range: [{}, {}). Size: {}",
                                         lsn_to_string(wal_start_lsn), lsn_to_string(wal_end_lsn), logical_data_len) << std::endl;

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
                std::cout << "Formatted Payload String:" << std::endl;
                std::cout << compact_hex_logical_replication << std::endl;

                last_processed_lsn = last_received_lsn;

                // --- ВЫЗОВ ВАШЕГО АДАПТИРОВАННОГО ПАРСЕРА ---
                // parser.process_wal_data(logical_data, logical_data_len);
                // Внутри парсер должен разбирать сообщения (BEGIN, COMMIT, INSERT и т.д.)
                // и обновлять last_processed_lsn *после* успешной обработки/сохранения данных COMMIT.
                // Например:
                // if (parser_sees_commit(logical_data, logical_data_len, &commit_lsn)) {
                //     // ... сохранить изменения ...
                //     last_processed_lsn = commit_lsn;
                //     // Сохранить last_processed_lsn в надежное хранилище!
                //     save_last_processed_lsn(last_processed_lsn);
                // }

                // ВАЖНО: Обновляйте last_processed_lsn только ПОСЛЕ того, как
                // вы успешно обработали и сохранили данные до этого LSN.
                // И не забывайте сохранять его персистентно для перезапуска!

            } else {
                std::cerr << "WARNING: Received unknown message type: " << message_type << std::endl;
            }
        }

        // --- Периодическая отправка обратной связи ---
        auto now = std::chrono::steady_clock::now();
        if (now - last_feedback_time >= feedback_interval) {

            std::cout << "send feedback" << std::endl;
            std::cout << fmt::format("Sending periodic feedback. Processed LSN: {}, {}", lsn_to_string(last_received_lsn), lsn_to_string(last_processed_lsn)) << std::endl;
              // Отправляем LSN последнего обработанного (и сохраненного!) блока данных
              // Received и Flushed часто равны Processed в простых клиентах
            send_standby_status_update(conn, last_received_lsn, last_processed_lsn, last_processed_lsn, false);
            last_feedback_time = now;
        }

        // Проверяем, нет ли асинхронных сообщений от сервера (ошибки и т.д.)
        if (PQconsumeInput(conn) == 0) {
             std::cerr << "ERROR: Failed to consume input: " << PQerrorMessage(conn) << std::endl;
             break;
        }
        // Здесь можно проверять PGnotify сообщения, если они нужны
        // PGnotify *notify;
        // while ((notify = PQnotifies(conn)) != NULL) { ... PQfreemem(notify); }

    }

    std::cout << "Stopping replication client." << std::endl;
    if (copy_buf) {
        PQfreemem(copy_buf);
    }

    PQfinish(conn);

    // if (!exampleDatabaseQuery(conninfo)) {
    //     return 1;
    // }
    //
    //  std::cout << replication_handler.run_consumer() << std::endl;

    return 0;
}
