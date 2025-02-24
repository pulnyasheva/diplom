#include <fmt/format.h>
#include <pqxx/pqxx>
#include <pqxx/stream>

#include "LogicalReplicationHandler.h"

#include <iostream>
#include <unordered_set>

#include "Connection.h"

bool isReplicationSlotExist(Logger logger, pqxx::nontransaction & tx, std::string & start_lsn, bool temporary, std::string slot)
{
    std::string slot_name;
    if (temporary)
        slot_name = slot + "_tmp";
    else
        slot_name = slot;

    std::string query_str = fmt::format("SELECT active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'", slot_name);
    pqxx::result result{tx.exec(query_str)};

    /// Replication slot does not exist
    if (result.empty())
        return false;

    start_lsn = result[0][2].as<std::string>();

    logger.log(LogLevel::DEBUG, fmt::format("Replication slot {} already exists (active: {}). Restart lsn position: {}, confirmed flush lsn: {}",
            slot_name, result[0][0].as<bool>(), result[0][1].as<std::string>(), start_lsn));

    return true;
}

void createReplicationSlot(Logger logger,
        pqxx::nontransaction & tx, std::string & start_lsn, std::string & snapshot_name, bool temporary, std::string slot)
{

    std::string query_str;
    std::string slot_name;
    if (temporary)
        slot_name = slot + "_tmp";
    else
        slot_name = slot;

    query_str = fmt::format("CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT", slot_name);

    try
    {
        pqxx::result result{tx.exec(query_str)};
        start_lsn = result[0][1].as<std::string>();
        snapshot_name = result[0][2].as<std::string>();
        logger.log(LogLevel::DEBUG, fmt::format("Created replication slot: {}, start lsn: {}, snapshot: {}", slot,
                                                start_lsn, snapshot_name));
    }
    catch (std::exception & e)
    {
        logger.log(LogLevel::ERROR, fmt::format("while creating PostgreSQL replication slot {}", slot_name));
        throw;
    }
}

std::string advanceLSN(bool &committed, Logger &logger, std::shared_ptr<pqxx::nontransaction> tx, std::string replication_slot_name)
{
    std::string query_str = fmt::format("SELECT end_lsn FROM pg_replication_slot_advance('{}', '{}')", replication_slot_name, final_lsn);
    pqxx::result result{tx->exec(query_str)};

    std::string final_lsn = result[0][0].as<std::string>();
    logger.log(LogLevel::DEBUG, "Advanced LSN up to: {}", final_lsn);
    committed = false;
    return final_lsn;
}

void updateLsn(std::string& current_lsn, std::unordered_set<std::string>& tables_to_sync, postgres::Connection *connection,
    Logger logger, bool & committed, std::string replication_slot_name)
{
    try
    {
        auto tx = std::make_shared<pqxx::nontransaction>(connection->getRef());
        current_lsn = advanceLSN(committed, logger, tx, replication_slot_name);
        tables_to_sync.clear();
        tx->commit();
    }
    catch (...)
    {
        // Какая-то обработка
    }
}

// Попытка прочитать
bool consume(bool &committed, std::make_shared<pqxx::nontransaction> &tx, Logger logger, postgres::Connection *connection,
             std::string replication_slot_name, std::string max_block_size, std::string publication_name) {
    std::unordered_set<std::string> tables_to_sync;
    std::string current_lsn = advanceLSN(committed, logger, tx, replication_slot_name);
    std::string lsn_value = current_lsn;
    bool slot_empty = true;
    try {
        auto tx = std::make_shared<pqxx::nontransaction>(connection->getRef());


        std::string query_str = fmt::format(
                "select lsn, data FROM pg_logical_slot_peek_binary_changes("
                "'{}', NULL, {}, 'publication_names', '{}', 'proto_version', '1')",
                replication_slot_name, max_block_size, publication_name);

        auto stream{pqxx::stream_from::query(*tx, query_str)};

        while (true)
        {
            const std::vector<pqxx::zview> * row{stream.read_row()};

            if (!row)
            {
                stream.complete();

                if (slot_empty)
                {
                    tx->commit();
                    return false;
                }

                break;
            }

            slot_empty = false;
            current_lsn = (*row)[0];
            lsn_value = current_lsn;

        }
    }
    catch (const std::exception &)
    {
        // Какая-то обработка
        return false;
    }
    catch (const pqxx::broken_connection &)
    {
        logger.log(LogLevel::DEBUG, "Connection was broken");
        connection->tryUpdateConnection();
        return false;
    }
    catch (const pqxx::sql_error & e)
    {
        std::string error_message = e.what();
        connection->tryUpdateConnection();
        return false;
    }
    catch (const pqxx::conversion_error & e)
    {
        logger.log(LogLevel::DEBUG, fmt::format("Conversion error: {}", e.what()));
        return false;
    }
    catch (...)
    {
        // Какая-то обработка
        return false;
    }

    if (!tables_to_sync.empty())
    {
        // syncTables();
    }
    else if (committed)
    {
        updateLsn();
    }

    return true;
}


namespace {
    std::string getPublicationName(const std::string & postgres_database, const std::string & postgres_table)
    {
        return fmt::format(
            "{}_ch_publication",
            postgres_table.empty() ? postgres_database : fmt::format("{}_{}", postgres_database, postgres_table));
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
    bool is_attach_,
    bool is_materialized_postgresql_database_,
    const std::string &file_name_,
    std::string &tables_list_)
    : connection_info(connection_info_),
      logger(file_name_),
      tables_list(tables_list_),
      // tables_list(replication_settings[MaterializedPostgreSQLSetting::materialized_postgresql_tables_list]),
      publication_name(getPublicationName(postgres_database_, postgres_table_))
{
    // if (!schema_list.empty() && !tables_list.empty())
    //     throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot have schema list and tables list at the same time");
    //
    // if (!schema_list.empty() && !postgres_schema.empty())
    //     throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot have schema list and common schema at the same time");
    //
    // checkReplicationSlot(replication_slot);
    //
    // LOG_INFO(log, "Using replication slot {} and publication {}", replication_slot, doubleQuoteString(publication_name));
    //
    // startup_task = getContext()->getSchedulePool().createTask("PostgreSQLReplicaStartup", [this]{ checkConnectionAndStart(); });
    // consumer_task = getContext()->getSchedulePool().createTask("PostgreSQLReplicaConsume", [this]{ consumerFunc(); });
    // cleanup_task = getContext()->getSchedulePool().createTask("PostgreSQLReplicaCleanup", [this]{ cleanupFunc(); });
}

void LogicalReplicationHandler::startSynchronization(bool throw_on_error) {
    postgres::Connection replication_connection(connection_info, &logger, true);
    pqxx::nontransaction tx(replication_connection.getRef());
    createPublicationIfNeeded(tx);

    // /// List of nested tables (table_name -> nested_storage), which is passed to replication consumer.
    // std::unordered_map<String, StorageInfo> nested_storages;

    // /// snapshot_name is initialized only if a new replication slot is created.
    // /// start_lsn is initialized in two places:
    // /// 1. if replication slot does not exist, start_lsn will be returned with its creation return parameters;
    // /// 2. if replication slot already exist, start_lsn is read from pg_replication_slots as
    // ///    `confirmed_flush_lsn` - the address (LSN) up to which the logical slot's consumer has confirmed receiving data.
    // ///    Data older than this is not available anymore.
    std::string snapshot_name;
    std::string start_lsn;

    /// Also lets have a separate non-replication connection, because we need two parallel transactions and
    /// one connection can have one transaction at a time.
    auto tmp_connection = std::make_shared<std::string>(connection_info);

    auto initial_sync = [&]() {
        logger.log(LogLevel::DEBUG, "Starting tables sync load");


        createReplicationSlot(tx, start_lsn, snapshot_name);


        // for (const auto &[table_name, storage]: materialized_storages) {
        //     try {
        //         nested_storages.emplace(table_name, loadFromSnapshot(*tmp_connection, snapshot_name, table_name,
        //                                                              storage->as<StorageMaterializedPostgreSQL>()));
        //     } catch (Exception &e) {
        //         e.addMessage("while loading table `{}`.`{}`", postgres_database, table_name);
        //         tryLogCurrentException(log);
        //
        //         /// Throw in case of single MaterializedPostgreSQL storage, because initial setup is done immediately
        //         /// (unlike database engine where it is done in a separate thread).
        //         if (throw_on_error && !is_materialized_postgresql_database)
        //             throw;
        //     }
        // }
    };

    /// There is one replication slot for each replication handler. In case of MaterializedPostgreSQL database engine,
    /// there is one replication slot per database. Its lifetime must be equal to the lifetime of replication handler.
    /// Recreation of a replication slot imposes reloading of all tables.
    if (!isReplicationSlotExist(tx, start_lsn, /* temporary */false)) {

        initial_sync();
    }
    /// Synchronization and initial load already took place - do not create any new tables, just fetch StoragePtr's
    /// and pass them to replication consumer.
    else {
        // for (const auto &[table_name, storage]: materialized_storages) {
        //     auto *materialized_storage = storage->as<StorageMaterializedPostgreSQL>();
        //     try {
        //         auto table_structure = fetchTableStructure(tx, table_name);
        //         if (!table_structure->physical_columns)
        //             throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns");
        //         auto storage_info = StorageInfo(materialized_storage->getNested(),
        //                                         table_structure->physical_columns->attributes);
        //         nested_storages.emplace(table_name, std::move(storage_info));
        //     } catch (Exception &e) {
        //         e.addMessage("while loading table {}.{}", postgres_database, table_name);
        //         tryLogCurrentException(log);
        //
        //         if (throw_on_error)
        //             throw;
        //     }
        // }
    }

    tx.commit();

    consume();
}

bool LogicalReplicationHandler::isPublicationExist(pqxx::nontransaction & tx)
{
    std::string query_str = fmt::format("SELECT exists (SELECT 1 FROM pg_publication WHERE pubname = '{}')",
                                        publication_name);
    pqxx::result result{tx.exec(query_str)};

    if (result.empty()) {
        logger.log(LogLevel::ERROR, fmt::format("Publication does not exist: {}", publication_name));
        throw std::runtime_error("Publication does not exist");
    }

    return result[0][0].as<std::string>() == "t";
}

void LogicalReplicationHandler::createPublicationIfNeeded(pqxx::nontransaction &tx) {
    auto publication_exists = isPublicationExist(tx);

    if (!publication_exists) {
        // if (tables_list.empty()) {
        //     if (materialized_storages.empty())
        //         throw std::logic_error("No tables to replicate");
        //
        //     WriteBufferFromOwnString buf;
        //     for (const auto &storage_data: materialized_storages) {
        //         buf << doubleQuoteWithSchema(storage_data.first);
        //         buf << ",";
        //     }
        //     tables_list = buf.str();
        //     tables_list.resize(tables_list.size() - 1);
        // }

        if (tables_list.empty())
            throw std::logic_error("No table found to be replicated");

        std::string query_str = fmt::format("CREATE PUBLICATION {} FOR TABLE ONLY {}",
                                            publication_name, tables_list);
        try {
            tx.exec(query_str);
            logger.log(LogLevel::DEBUG,
                       fmt::format("Created publication {} with tables list: {}", publication_name, tables_list));
        } catch (const std::exception& e) {
            logger.log(LogLevel::ERROR, fmt::format("while creating pg_publication {}", e.what()));
            throw;
        }
    } else {
        logger.log(LogLevel::DEBUG, fmt::format("Using existing publication ({}) version", publication_name));
    }
}
