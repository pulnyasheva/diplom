#pragma once

#include <pqxx/pqxx>

#include <Logger.h>

class LogicalReplicationHandler {
public:
    LogicalReplicationHandler(
            const std::string & postgres_database_,
            const std::string & postgres_table_,
            const std::string & connection_info_,
            bool is_attach_,
            bool is_materialized_postgresql_database_,
            const std::string & file_name_,
            std::string & tables_list_);

    /// Start replication setup immediately.
    void startSynchronization(bool throw_on_error);

private:
    bool isPublicationExist(pqxx::nontransaction & tx);

    void createPublicationIfNeeded(pqxx::nontransaction & tx);

    std::string connection_info;
    Logger logger;

    /// A coma-separated list of tables, which are going to be replicated for database engine. By default, a whole database is replicated.
    std::string tables_list;

    // const bool user_managed_slot;
    // const String user_provided_snapshot;
    // const String replication_slot;
    // const String tmp_replication_slot;
    const std::string publication_name;
};
