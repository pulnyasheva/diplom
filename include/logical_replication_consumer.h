#pragma once

#include <cstdint>
#include <unordered_set>

#include <сonnection.h>
#include <logger.h>
#include <otterbrix_service.h>
#include <postgres_settings.h>
#include <set>

class logical_replication_consumer {
public:
    logical_replication_consumer(
    std::shared_ptr<postgres::сonnection> connection_,
    const std::string & database_name_,
    const std::string & replication_slot_name_,
    const std::string & publication_name_,
    const std::string & start_lsn,
    size_t max_block_size_,
    logger *logger_);

    bool consume();

private:
    uint64_t get_lsn(const std::string & lsn);

    void update_lsn();

    std::string lsn(std::shared_ptr<pqxx::nontransaction> tx);

    std::vector<int32_t> get_primary_key(int32_t table_id);

    logger *current_logger;
    postgres_settings current_postgres_settings;
    const std::string replication_slot_name, publication_name;
    const std::string database_name;

    bool is_committed = false;

    std::shared_ptr<postgres::сonnection> connection;

    std::unordered_map<int32_t, std::string> id_to_table_name;
    std::unordered_map<int32_t, std::vector<int32_t>> id_to_primary_key;
    std::unordered_set<int32_t> id_skip_table_name;
    std::unordered_map<int32_t, std::vector<std::pair<std::string, int32_t>>> id_table_to_column;

    std::string current_lsn, result_lsn;
    uint64_t lsn_value;

    size_t max_block_size;

    otterbrix_service current_otterbrix_service;
};
