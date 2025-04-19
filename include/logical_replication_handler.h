#pragma once

#include <pqxx/pqxx>
#include <future>

#include <logger.h>
#include <сonnection.h>
#include <logical_replication_consumer.h>
#include <scheduler.h>

namespace pqxx {
    using replication_transaction = transaction<repeatable_read, write_policy::read_only>;
}

namespace DB {
    static constexpr size_t replication_slot_name_max_size = 64;
}

class logical_replication_handler {
public:
    using consumer_ptr = std::shared_ptr<logical_replication_consumer>;

    logical_replication_handler(
            const std::string & postgres_database_,
            const std::string & postgres_name_,
            const std::string & connection_dsn_,
            const std::string & file_name_,
            const std::string &url_log_,
            std::vector<std::string> & tables_array_,
            size_t max_block_size_,
            bool user_managed_slot = false,
            std::string user_snapshot = "");

    /// Start replication.
    void start_synchronization();

    bool run_consumer();

    consumer_ptr get_consumer();

private:
    bool has_publication(pqxx::nontransaction & tx);

    void create_publication(pqxx::nontransaction &tx);

    bool has_replication_slot(pqxx::nontransaction & tx, std::string & start_lsn);

    void create_replication_slot(pqxx::nontransaction & tx, std::string & start_lsn, std::string & snapshot_name);

    void drop_replication_slot(pqxx::nontransaction & tx);

    void load_from_snapshot(postgres::сonnection & connection, std::string & snapshot_name, const std::string &table_name);

    std::string connection_dsn;
    logger current_logger;

    std::vector<std::string> tables_array;
    std::string database_name;
    std::string tables_names;

    const bool user_managed_slot;
    const std::string user_snapshot;
    const std::string replication_slot;
    const std::string publication_name;
    size_t max_block_size;

    consumer_ptr consumer;

    otterbrix_service current_otterbrix_service;
};
