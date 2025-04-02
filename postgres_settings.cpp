#include <iostream>
#include <postgres_settings.h>

postgres_settings::postgres_settings(std::shared_ptr<postgres::сonnection> connection_, logger *logger_)
    : current_logger(logger_),
      connection(std::move(connection_)) {};

std::set<std::string> postgres_settings::get_primary_key(std::string& table_name) {
    std::string query_str = fmt::format(
            "SELECT kcu.column_name "
            "FROM information_schema.table_constraints tco "
            "JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tco.constraint_name "
            "WHERE tco.constraint_type = 'PRIMARY KEY' "
            "AND tco.table_name = '{}';",
            table_name
        );

    std::set<std::string> primary_key;
    try {
        pqxx::nontransaction tx(connection->get_ref());
        pqxx::result result{tx.exec(query_str)};

        for (const auto& row : result) {
            primary_key.insert(row[0].as<std::string>());
        }
    } catch (const std::exception& e) {
        current_logger->log(log_level::ERROR, e.what());
    }

    return primary_key;
}