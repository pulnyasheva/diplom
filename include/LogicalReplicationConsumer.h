#pragma once

#include <cstdint>

#include <Connection.h>
#include <Logger.h>

class LogicalReplicationConsumer {
public:
    LogicalReplicationConsumer(
    std::shared_ptr<postgres::Connection> connection_,
    const std::string & replication_slot_name_,
    const std::string & publication_name_,
    const std::string & start_lsn,
    size_t max_block_size_,
    Logger *logger_);

    bool consume();

private:
    uint64_t getLSNValue(const std::string & lsn);

    void updateLsn();

    std::string advanceLSN(std::shared_ptr<pqxx::nontransaction> tx);

    Logger *logger;
    const std::string replication_slot_name, publication_name;

    bool committed = false;

    std::shared_ptr<postgres::Connection> connection;

    std::string current_lsn, final_lsn;
    uint64_t lsn_value;

    size_t max_block_size;
};
