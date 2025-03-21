#pragma once

#include <pqxx/pqxx>
#include <future>

#include <Logger.h>
#include <Connection.h>
#include <LogicalReplicationConsumer.h>
#include <Scheduler.h>

namespace pqxx {
    using ReplicationTransaction = transaction<repeatable_read, write_policy::read_only>;
}

namespace DB {
    static constexpr size_t replication_slot_name_max_size = 64;
}

class LogicalReplicationHandler {
public:
    using ConsumerPtr = std::shared_ptr<LogicalReplicationConsumer>;

    LogicalReplicationHandler(
            const std::string & postgres_database_,
            const std::string & postgres_table_,
            const std::string & connection_dsn_,
            const std::string & file_name_,
            std::vector<std::string> & tables_array_,
            size_t max_block_size_,
            bool user_managed_slot = false,
            std::string user_snapshot = "");

    /// Start replication.
    void startSynchronization();

    bool runConsumer();

    ConsumerPtr getConsumer();

private:
    bool hasPublication(pqxx::nontransaction & tx);

    void createPublication(pqxx::nontransaction &tx);

    bool hasReplicationSlot(pqxx::nontransaction & tx, std::string & start_lsn);

    void createReplicationSlot(pqxx::nontransaction & tx, std::string & start_lsn, std::string & snapshot_name);

    void dropReplicationSlot(pqxx::nontransaction & tx);

    void loadFromSnapshot(postgres::Connection & connection, std::string & snapshot_name, const std::string &table_name);

    std::string connection_dsn;
    Logger logger;

    std::vector<std::string> tables_array;
    /// A coma-separated list of tables, which are going to be replicated for database engine. By default, a whole database is replicated.
    std::string tables_names;

    const bool user_managed_slot;
    const std::string user_snapshot;
    const std::string replication_slot;
    const std::string publication_name;
    size_t max_block_size;

    ConsumerPtr consumer;
};
