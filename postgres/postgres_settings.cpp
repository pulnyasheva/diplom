#include <fmt/format.h>

#include <postgres/postgres_settings.h>

namespace {
    struct SchemaTable {
        std::string schema;
        std::string table;
    };

    SchemaTable parseQualifiedName(std::string &qualified_name) {
        SchemaTable result;

        size_t dot_pos = qualified_name.find('.');

        if (dot_pos == std::string_view::npos) {
            result.table = std::string(qualified_name);
        } else {
            result.schema = std::string(qualified_name.substr(0, dot_pos));
            result.table = std::string(qualified_name.substr(dot_pos + 1));
        }

        return result;
    }
}

postgres_settings::postgres_settings(const std::string & connection_dsn_, logger *logger_)
    : connection_dsn(connection_dsn_),
      current_logger(logger_) {
};

std::set<std::string> postgres_settings::get_primary_key(std::string &qualified_name) {
    SchemaTable shema_table = parseQualifiedName(qualified_name);
    std::string query_str;
    if (shema_table.schema.empty()) {
        query_str = fmt::format(
            "SELECT kcu.column_name "
            "FROM information_schema.table_constraints tco "
            "JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tco.constraint_name "
            "WHERE tco.constraint_type = 'PRIMARY KEY' "
            "AND tco.table_name = '{}';",
            shema_table.table
        );
    } else {
        query_str = fmt::format(
            "SELECT kcu.column_name "
            "FROM information_schema.table_constraints tco "
            "JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tco.constraint_name "
            "WHERE tco.constraint_type = 'PRIMARY KEY' "
            "AND tco.table_schema = '{}' "
            "AND tco.table_name = '{}';",
            shema_table.schema,
            shema_table.table
        );
    }

    std::set<std::string> primary_key;
    try {
        postgres::сonnection replication_connection(connection_dsn, current_logger, true);
        pqxx::nontransaction tx(replication_connection.get_ref());
        pqxx::result result{tx.exec(query_str)};

        for (const auto &row: result) {
            primary_key.insert(row[0].as<std::string>());
        }
    } catch (const std::exception &e) {
        current_logger->log_to_file(log_level::ERROR, e.what());
    }

    return primary_key;
}
