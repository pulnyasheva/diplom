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

    std::vector<std::string> tables = {"students"};
    logical_replication_handler replication_handler = logical_replication_handler(
        "postgres",
        "students",
        conninfo,
        "log.txt",
        "",
        tables,
        100);

    replication_handler.start_synchronization();

    // if (!exampleDatabaseQuery(conninfo)) {
    //     return 1;
    // }

     std::cout << replication_handler.run_consumer() << std::endl;

    return 0;
}
