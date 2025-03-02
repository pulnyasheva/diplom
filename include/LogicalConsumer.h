#pragma once

#include <cstdint>

#include <Connection.h>
#include <Logger.h>

class LogicalConsumer {
public:
    LogicalConsumer(
    std::shared_ptr<postgres::Connection> connection_,
    const std::string & replication_slot_name_,
    const std::string & publication_name_,
    const std::string & start_lsn,
    size_t max_block_size_,
    Logger *logger_);

    bool consume();

private:
    static uint64_t getLSNValue(const std::string & lsn)
    {
        uint32_t upper_half;
        uint32_t lower_half;
        std::sscanf(lsn.data(), "%X/%X", &upper_half, &lower_half); /// NOLINT
        return (static_cast<uint64_t>(upper_half) << 32) + lower_half;
    }

    Logger *logger;
    const std::string replication_slot_name, publication_name;

    bool committed = false;

    std::shared_ptr<postgres::Connection> connection;

    std::string current_lsn, final_lsn;
    uint64_t lsn_value;

    size_t max_block_size;
};
