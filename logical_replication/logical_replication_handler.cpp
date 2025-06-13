#include <boost/mpl/placeholders.hpp>
#include <fmt/format.h>
#include <iostream>

#include <logical_replication/logical_replication_handler.h>
#include <postgres/сonnection.h>
#include <common/exception.h>

namespace {
    std::string get_publication_name(const std::string & postgres_database, const std::string & postgres_table)
    {
        return fmt::format(
            "{}_ch_publication",
            postgres_table.empty() ? postgres_database : fmt::format("{}_{}", postgres_database, postgres_table));
    }

    void check_replication_slot(std::string name)
    {
        for (const auto &c: name) {
            const bool ok = (std::isalpha(c) && std::islower(c)) || std::isdigit(c) || c == '_';
            if (!ok) {
                throw exception(
                    error_codes::BAD_ARGUMENTS,
                    fmt::format(
                        "Replication slot can contain lower-case letters, numbers, and the underscore character {}",
                        name));
            }
        }

        if (name.size() > DB::replication_slot_name_max_size)
            throw exception(error_codes::LOGICAL_ERROR,
                            fmt::format("Big replication slot size: {}", name));
    }

    std::string normalize_replication_slot(std::string name) {
        std::transform(name.begin(), name.end(), name.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        std::replace(name.begin(), name.end(), '-', '_');
        return name;
    }

    std::string get_replication_slot_name(const std::string & postgres_database, const std::string & postgres_table)
    {
        std::string slot_name = fmt::format("{}_{}_ch_replication_slot", postgres_database, postgres_table);
        slot_name = normalize_replication_slot(slot_name);
        return slot_name;
    }

    std::string create_tables_names(std::vector<std::string> tables_array) {
        std::ostringstream oss;
        for (size_t i = 0; i < tables_array.size(); ++i) {
            oss << tables_array[i];
            if (i != tables_array.size() - 1) {
                oss << ",";
            }
        }
        return oss.str();
    }

    std::string double_quote_string(const std::string& str) {
        std::stringstream ss;
        ss << "\"";
        for (char c : str) {
            if (c == '"') {
                ss << "\"\"";
            } else {
                ss << c;
            }
        }
        ss << "\"";
        return ss.str();
    }
}

logical_replication_handler::logical_replication_handler(
    const std::string &postgres_database_,
    const std::string &postgres_name_,
    const std::string &connection_dsn_,
    const std::string &file_name_,
    const std::string &url_log_,
    std::vector<std::string> &tables_array_,
    ReaderWriterQueue<result_node> &queue_,
    std::pmr::memory_resource* resource_,
    size_t max_block_size_,
    const bool user_managed_slot_,
    const std::string user_snapshot_)
    : connection_dsn(connection_dsn_),
      current_logger(file_name_, url_log_),
      tables_array(tables_array_),
      database_name(postgres_database_),
      tables_names(create_tables_names(tables_array_)),
      replication_slot(get_replication_slot_name(postgres_database_, postgres_name_)),
      publication_name(get_publication_name(postgres_database_, postgres_name_)),
      user_managed_slot(user_managed_slot_),
      user_snapshot(user_snapshot_),
      max_block_size(max_block_size_),
      current_otterbrix_service(queue_),
      resource(resource_)
{
    if (tables_names.empty()) {
        throw exception(error_codes::BAD_ARGUMENTS, "Can not have tables list");
    }

    check_replication_slot(replication_slot);

    current_logger.log_to_console(log_level::DEBUGER, fmt::format(
               "Using replication slot {} and publication {}",
               replication_slot,
               double_quote_string(publication_name)));
}

bool logical_replication_handler::run_consumer() {
    return get_consumer()->consume();
}

void logical_replication_handler::start_synchronization() {
    postgres::сonnection replication_connection(connection_dsn, &current_logger, true);
    pqxx::nontransaction tx(replication_connection.get_ref());
    create_publication(tx);

    std::string snapshot_name;
    std::string start_lsn;
    auto tmp_connection = std::make_shared<postgres::сonnection>(connection_dsn, &current_logger);

    auto initial_sync = [&]() {
        current_logger.log_to_console(log_level::DEBUGER, "Starting tables sync load");

        try
        {
            if (user_managed_slot)
            {
                if (user_snapshot.empty())
                    throw exception(error_codes::BAD_ARGUMENTS, "User snapshot not have");

                snapshot_name = user_snapshot;
            }
            else
            {
                create_replication_slot(tx, start_lsn, snapshot_name);
            }

            for (std::string table_name : tables_array) {
                load_from_snapshot(*tmp_connection, snapshot_name, table_name);
            }
        }
        catch (exception &e)
        {
            current_logger.log_to_console(log_level::ERR, e.what());
        }
    };

    if (!has_replication_slot(tx, start_lsn)) {
        initial_sync();
    } else {
        if (!user_managed_slot) {
            drop_replication_slot(tx);
        }
        initial_sync();
    }

    tx.commit();

    consumer = std::make_shared<logical_replication_consumer>(
        connection_dsn,
        std::move(tmp_connection),
        database_name,
        replication_slot,
        publication_name,
        start_lsn,
        max_block_size,
        &current_logger,
        &current_otterbrix_service,
        resource);
    current_logger.log_to_console(log_level::DEBUGER, "Consumer created");
}

logical_replication_handler::consumer_ptr logical_replication_handler::get_consumer()
{
    if (!consumer)
        throw exception(error_codes::LOGICAL_ERROR, "Consumer not initialized");

    return consumer;
}

bool logical_replication_handler::has_publication(pqxx::nontransaction & tx)
{
    std::string query_str = fmt::format("SELECT exists (SELECT 1 FROM pg_publication WHERE pubname = '{}')",
                                        publication_name);
    pqxx::result result{tx.exec(query_str)};

    if (result.empty())
        throw exception(error_codes::LOGICAL_ERROR,
            fmt::format("Publication does not exist: {}", publication_name));

    return result[0][0].as<std::string>() == "t";
}

void logical_replication_handler::create_publication(pqxx::nontransaction &tx) {
    auto publication_exists = has_publication(tx);

    if (!publication_exists) {
        if (tables_names.empty())
            throw std::logic_error("No table found for replication");

        std::string query_str = fmt::format("CREATE PUBLICATION {} FOR TABLE ONLY {}",
                                            publication_name, tables_names);
        try
        {
            tx.exec(query_str);
            current_logger.log_to_console(log_level::DEBUGER, fmt::format(
                       "Created publication {} with tables: {}", publication_name, tables_names));
        }
        catch (const std::exception& e)
        {
            throw exception(error_codes::LOGICAL_ERROR, fmt::format("While creating publication {}", e.what()));
        }
    } else {
        current_logger.log_to_console(log_level::DEBUGER, fmt::format("Using publication {}", publication_name));
    }
}

bool logical_replication_handler::has_replication_slot(pqxx::nontransaction & tx, std::string & start_lsn)
{
    std::string slot_name = replication_slot;

    std::string query_str = fmt::format(
        "SELECT active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'",
        slot_name
    );
    pqxx::result result{tx.exec(query_str)};

    if (result.empty())
        return false;

    start_lsn = result[0][2].as<std::string>();

    current_logger.log_to_console(log_level::DEBUGER, fmt::format(
               "Replication slot {} already exists.", slot_name));
    return true;
}

void logical_replication_handler::create_replication_slot(pqxx::nontransaction &tx,
                                                      std::string &start_lsn,
                                                      std::string &snapshot_name) {
    std::string query_str;
    std::string slot_name = replication_slot;

    query_str = fmt::format("CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT",
                            double_quote_string(slot_name));

    try
    {
        pqxx::result result{tx.exec(query_str)};
        start_lsn = result[0][1].as<std::string>();
        snapshot_name = result[0][2].as<std::string>();
        current_logger.log_to_console(log_level::INFO, fmt::format(
                       "Created replication slot: {}, start lsn: {}, snapshot: {}",
                       replication_slot, start_lsn, snapshot_name));
    }
    catch (std::exception &e)
    {
        throw exception(error_codes::LOGICAL_ERROR, fmt::format(
                            "While creating PostgreSQL replication slot {}: {}", slot_name, e.what()));
    }
}

void logical_replication_handler::drop_replication_slot(pqxx::nontransaction & tx)
{
    std::string slot_name = replication_slot;

    std::string query_str = fmt::format("SELECT pg_drop_replication_slot('{}')", slot_name);

    tx.exec(query_str);
    current_logger.log_to_console(log_level::INFO, fmt::format("Dropped replication slot: {}", slot_name));
}

void logical_replication_handler::load_from_snapshot(postgres::сonnection &connection,
                                                 std::string &snapshot_name,
                                                 const std::string &table_name) {
    auto tx = std::make_shared<pqxx::replication_transaction>(connection.get_ref());

    std::string query_str = fmt::format("SET TRANSACTION SNAPSHOT '{}'", snapshot_name);
    tx->exec(query_str);

    query_str = fmt::format("SELECT * FROM ONLY {}", table_name);

    current_logger.log_to_console(log_level::DEBUGER, fmt::format("Loading PostgreSQL table {}", table_name));

    // Getting data from the database to start syncing
    pqxx::result result = tx->exec(query_str);

    current_otterbrix_service.data_handler(result, table_name, database_name, resource);
}