#include <fmt/format.h>
#include <pqxx/transaction_base.hxx>
#include <iostream>
#include <unordered_set>

#include <LogicalReplicationHandler.h>
#include <Connection.h>
#include <Exception.h>

namespace {
    std::string getPublicationName(const std::string & postgres_database, const std::string & postgres_table)
    {
        return fmt::format(
            "{}_ch_publication",
            postgres_table.empty() ? postgres_database : fmt::format("{}_{}", postgres_database, postgres_table));
    }

    void checkReplicationSlot(std::string name)
    {
        for (const auto &c: name) {
            const bool ok = (std::isalpha(c) && std::islower(c)) || std::isdigit(c) || c == '_';
            if (!ok) {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    fmt::format(
                        "Replication slot can contain lower-case letters, numbers, and the underscore character. Got: {}",
                        name));
            }
        }

        if (name.size() > DB::replication_slot_name_max_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            fmt::format("Too big replication slot size: {}", name));
    }

    std::string normalizeReplicationSlot(std::string name) {
        std::transform(name.begin(), name.end(), name.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        std::replace(name.begin(), name.end(), '-', '_');
        return name;
    }

    std::string getReplicationSlotName(const std::string & postgres_database, const std::string & postgres_table)
    {
        std::string slot_name = fmt::format("{}_{}_ch_replication_slot", postgres_database, postgres_table);
        slot_name = normalizeReplicationSlot(slot_name);
        return slot_name;
    }

    std::string createTableList(std::vector<std::string> tables_array) {
        std::ostringstream oss;
        for (size_t i = 0; i < tables_array.size(); ++i) {
            oss << tables_array[i];
            if (i != tables_array.size() - 1) {
                oss << ",";
            }
        }
        return oss.str();
    }

    std::string doubleQuoteString(const std::string& str) {
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

LogicalReplicationHandler::LogicalReplicationHandler(
    const std::string &postgres_database_,
    const std::string &postgres_table_,
    const std::string &connection_info_,
    const std::string &file_name_,
    std::vector<std::string> &tables_array_,
    size_t max_block_size_,
    const bool user_managed_slot_,
    const std::string user_snapshot_)
    : connection_info(connection_info_),
      logger(file_name_),
      tables_array(tables_array_),
      tables_list(createTableList(tables_array_)),
      replication_slot(getReplicationSlotName(postgres_database_, postgres_table_)),
      publication_name(getPublicationName(postgres_database_, postgres_table_)),
      user_managed_slot(user_managed_slot_),
      user_snapshot(user_snapshot_),
      max_block_size(max_block_size_)
{
    if (tables_list.empty()) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot have tables list at the same time");
    }

    checkReplicationSlot(replication_slot);

    logger.log(LogLevel::DEBUG, fmt::format(
               "Using replication slot {} and publication {}",
               replication_slot,
               doubleQuoteString(publication_name)));
}

bool LogicalReplicationHandler::runConsumer() {
    return getConsumer()->consume();
}

void LogicalReplicationHandler::startSynchronization() {
    postgres::Connection replication_connection(connection_info, &logger, true);
    pqxx::nontransaction tx(replication_connection.getRef());
    createPublicationIfNeeded(tx);

    std::string snapshot_name;
    std::string start_lsn;
    auto tmp_connection = std::make_shared<postgres::Connection>(connection_info, &logger);

    auto initial_sync = [&]() {
        logger.log(LogLevel::DEBUG, "Starting tables sync load");

        try
        {
            if (user_managed_slot)
            {
                if (user_snapshot.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Using a user-defined replication slot must");

                snapshot_name = user_snapshot;
            }
            else
            {
                createReplicationSlot(tx, start_lsn, snapshot_name);
            }

            for (std::string table_name : tables_array) {
                loadFromSnapshot(*tmp_connection, snapshot_name, table_name);
            }
        }
        catch (Exception &e)
        {
            logger.log(LogLevel::ERROR, e.what());
        }
    };

    if (!isReplicationSlotExist(tx, start_lsn)) {
        initial_sync();
    } else {
        dropReplicationSlot(tx);
        initial_sync();
    }

    tx.commit();

    consumer = std::make_shared<LogicalReplicationConsumer>(
        std::move(tmp_connection),
        replication_slot,
        publication_name,
        start_lsn,
        max_block_size,
        &logger);
    logger.log(LogLevel::DEBUG, "Consumer created");
}

LogicalReplicationHandler::ConsumerPtr LogicalReplicationHandler::getConsumer()
{
    if (!consumer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Consumer not initialized");

    return consumer;
}

bool LogicalReplicationHandler::isPublicationExist(pqxx::nontransaction & tx)
{
    std::string query_str = fmt::format("SELECT exists (SELECT 1 FROM pg_publication WHERE pubname = '{}')",
                                        publication_name);
    pqxx::result result{tx.exec(query_str)};

    if (result.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            fmt::format("Publication does not exist: {}", publication_name));

    return result[0][0].as<std::string>() == "t";
}

void LogicalReplicationHandler::createPublicationIfNeeded(pqxx::nontransaction &tx) {
    auto publication_exists = isPublicationExist(tx);

    if (!publication_exists) {
        if (tables_list.empty())
            throw std::logic_error("No table found to be replicated");

        std::string query_str = fmt::format("CREATE PUBLICATION {} FOR TABLE ONLY {}",
                                            publication_name, tables_list);
        try
        {
            tx.exec(query_str);
            logger.log(LogLevel::DEBUG, fmt::format(
                       "Created publication {} with tables list: {}", publication_name, tables_list));
        }
        catch (const std::exception& e)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, fmt::format("While creating pg_publication {}", e.what()));
        }
    } else {
        logger.log(LogLevel::DEBUG, fmt::format("Using existing publication ({}) version", publication_name));
    }
}

bool LogicalReplicationHandler::isReplicationSlotExist(pqxx::nontransaction & tx, std::string & start_lsn)
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

    logger.log(LogLevel::DEBUG, fmt::format(
               "Replication slot {} already exists (active: {}). Restart lsn position: {}, confirmed flush lsn: {}",
               slot_name, result[0][0].as<bool>(), result[0][1].as<std::string>(), start_lsn));
    return true;
}

void LogicalReplicationHandler::createReplicationSlot(pqxx::nontransaction &tx,
                                                      std::string &start_lsn,
                                                      std::string &snapshot_name) {
    std::string query_str;
    std::string slot_name = replication_slot;

    query_str = fmt::format("CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT",
                            doubleQuoteString(slot_name));

    try
    {
        pqxx::result result{tx.exec(query_str)};
        start_lsn = result[0][1].as<std::string>();
        snapshot_name = result[0][2].as<std::string>();
        logger.log(LogLevel::INFO, fmt::format(
                       "Created replication slot: {}, start lsn: {}, snapshot: {}",
                       replication_slot, start_lsn, snapshot_name));
    }
    catch (std::exception &e)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, fmt::format(
                            "While creating PostgreSQL replication slot {}: {}", slot_name, e.what()));
    }
}

void LogicalReplicationHandler::dropReplicationSlot(pqxx::nontransaction & tx)
{
    std::string slot_name = replication_slot;

    std::string query_str = fmt::format("SELECT pg_drop_replication_slot('{}')", slot_name);

    tx.exec(query_str);
    logger.log(LogLevel::INFO, fmt::format("Dropped replication slot: {}", slot_name));
}

void LogicalReplicationHandler::loadFromSnapshot(postgres::Connection &connection,
                                                 std::string &snapshot_name,
                                                 const std::string &table_name) {
    auto tx = std::make_shared<pqxx::ReplicationTransaction>(connection.getRef());

    std::string query_str = fmt::format("SET TRANSACTION SNAPSHOT '{}'", snapshot_name);
    tx->exec(query_str);

    query_str = fmt::format("SELECT * FROM ONLY {}", table_name);

    logger.log(LogLevel::DEBUG, fmt::format("Loading PostgreSQL table {}", table_name));

    // Getting data from the database to start syncing
    pqxx::result result = tx->exec(query_str);

    for (const auto& row : result)
    {
        for (const auto& field : row)
        {
            std::cout << field.c_str() << " ";
        }
        std::cout << std::endl;
    }
}