#pragma once

#include <cstdint>
#include <unordered_set>
#include <libpq-fe.h>

#include <postgres/сonnection.h>
#include <common/logger.h>
#include <otterbrix/otterbrix_service.h>
#include <postgres/postgres_settings.h>

class logical_replication_consumer {
public:
    logical_replication_consumer(
    const std::string & connection_dsn_,
    std::shared_ptr<postgres::сonnection> connection_,
    const std::string & database_name_,
    const std::string & replication_slot_name_,
    const std::string & publication_name_,
    const std::string & start_lsn,
    size_t max_block_size_,
    logger *logger_,
    otterbrix_service *otterbrix_service,
    std::pmr::memory_resource* resource_);

    bool consume();

private:
    uint64_t get_lsn(const std::string & lsn);

    std::string lsn_to_string(uint64_t lsn_value);

    std::vector<int32_t> get_primary_key(int32_t table_id);

    void send_standby_status_update(PGconn *conn,
                                    uint64_t received_lsn,
                                    uint64_t flushed_lsn,
                                    uint64_t applied_lsn,
                                    bool request_reply_from_server);

    PGconn *start_replication();

    logger *current_logger;
    postgres_settings current_postgres_settings;
    const std::string replication_slot_name, publication_name;
    const std::string database_name;

    bool is_committed = false;

    const std::string connection_dsn;
    std::shared_ptr<postgres::сonnection> connection;

    std::unordered_map<int32_t, std::string> id_to_table_name;
    std::unordered_map<int32_t, std::vector<int32_t>> id_to_primary_key;
    std::unordered_set<int32_t> id_skip_table_name;
    std::unordered_map<int32_t, std::vector<std::pair<std::string, int32_t>>> id_table_to_column;

    std::string current_lsn, result_lsn;
    uint64_t current_lsn_value;
    uint64_t last_processed_lsn;
    uint64_t last_received_lsn;

    size_t max_block_size;

    otterbrix_service *current_otterbrix_service;
    std::pmr::memory_resource* resource;
};
