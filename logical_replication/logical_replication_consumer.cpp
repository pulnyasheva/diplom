#include <iostream>
#include <fmt/format.h>
#include <pqxx/pqxx>
#include <set>

#include <logical_replication/logical_replication_consumer.h>
#include <logical_replication/logical_replication_parser.h>
#include <common/exception.h>

logical_replication_consumer::logical_replication_consumer(
    const std::string & connection_dsn_,
    std::shared_ptr<postgres::сonnection> connection_,
    const std::string & database_name_,
    const std::string &replication_slot_name_,
    const std::string &publication_name_,
    const std::string &start_lsn,
    size_t max_block_size_,
    logger *logger_)
    : current_logger(logger_),
      replication_slot_name(replication_slot_name_),
      publication_name(publication_name_),
      database_name(database_name_),
      connection(std::move(connection_)),
      current_lsn(start_lsn),
      result_lsn(start_lsn),
      lsn_value(get_lsn(start_lsn)),
      max_block_size(max_block_size_),
      current_postgres_settings(connection_dsn_, logger_) {
}

uint64_t logical_replication_consumer::get_lsn(const std::string & lsn)
{
    uint32_t upper_half;
    uint32_t lower_half;
    std::sscanf(lsn.data(), "%X/%X", &upper_half, &lower_half);
    return (static_cast<uint64_t>(upper_half) << 32) + lower_half;
}

void logical_replication_consumer::update_lsn()
{
    try
    {
        auto tx = std::make_shared<pqxx::nontransaction>(connection->get_ref());
        current_lsn = lsn(tx);
        tx->commit();
    }
    catch (std::exception &e)
    {
        current_logger->log_to_file(log_level::ERROR, fmt::format("Error for update lsn: {}", e.what()));
    }
}

std::string logical_replication_consumer::lsn(std::shared_ptr<pqxx::nontransaction> tx) {
    std::string query_str = fmt::format("SELECT end_lsn FROM pg_replication_slot_advance('{}', '{}')",
                                        replication_slot_name, result_lsn);
    pqxx::result result{tx->exec(query_str)};

    result_lsn = result[0][0].as<std::string>();
    current_logger->log_to_file(log_level::DEBUG, fmt::format("LSN up to: {}", get_lsn(result_lsn)));
    is_committed = false;
    return result_lsn;
}

std::vector<int32_t> logical_replication_consumer::get_primary_key(int32_t table_id) {
    if (id_to_primary_key.contains(table_id)) {
        return id_to_primary_key[table_id];
    }

    std::set<std::string> primary_key = current_postgres_settings.get_primary_key(id_to_table_name[table_id]);
    std::vector<int32_t> primary_key_columns;
    for (int32_t i = 0; i < id_table_to_column[table_id].size(); i++) {
        auto column = id_table_to_column[table_id][i];
        if (primary_key.contains(column.first)) {
            primary_key_columns.emplace_back(i);
        }
    }
    id_to_primary_key[table_id] = primary_key_columns;
    return id_to_primary_key[table_id];
}

bool logical_replication_consumer::consume()
{
    update_lsn();
    bool is_slot_empty = true;
    try
    {
        auto tx = std::make_shared<pqxx::nontransaction>(connection->get_ref());

        std::string query_str = fmt::format(
                "select lsn, data FROM pg_logical_slot_peek_binary_changes("
                "'{}', NULL, {}, 'publication_names', '{}', 'proto_version', '1')",
                replication_slot_name, max_block_size, publication_name);

        pqxx::stream_from stream{pqxx::stream_from::query(*tx, query_str)};

        logical_replication_parser parser = logical_replication_parser(&current_lsn, &result_lsn, &is_committed, current_logger);

        while (true)
        {
            const std::vector<pqxx::zview> * row{stream.read_row()};

            if (!row)
            {
                stream.complete();

                if (is_slot_empty)
                {
                    tx->commit();
                    return false;
                }

                break;
            }

            is_slot_empty = false;
            current_lsn = (*row)[0];
            lsn_value = get_lsn(current_lsn);

            std::cout << fmt::format("Current message: {}", (*row)[1]) << std::endl;

            try
            {
                std::vector<std::string> result;
                std::unordered_map<int32_t, std::string> old_value;
                postgre_sql_type_operation type_operation;
                int32_t table_id_query;
                parser.parse_binary_data((*row)[1].c_str(),
                                       (*row)[1].size(),
                                       type_operation,
                                       table_id_query,
                                       result,
                                       id_to_table_name,
                                       id_skip_table_name,
                                       id_table_to_column,
                                       old_value);
                current_otterbrix_service.data_handler(type_operation, id_to_table_name[table_id_query], database_name,
                                                       get_primary_key(table_id_query), result,
                                                       id_table_to_column[table_id_query], old_value);
            }
            catch (const exception &e)
            {
                current_logger->log_to_file(log_level::ERROR, fmt::format("Error during parsing: {}", e.what()));
            }
        }
    }
    catch (const exception &e)
    {
        current_logger->log_to_file(log_level::ERROR, fmt::format("Exception thrown in consume", e.what()));
        return false;
    }
    catch (const pqxx::broken_connection &)
    {
        current_logger->log_to_file(log_level::ERROR, "Connection was broken");
        connection->try_refresh_connection();
        return false;
    }
    catch (const pqxx::sql_error &e)
    {
        std::string error_message = e.what();
        if (!error_message.find("out of relcache_callback_list slots"))
            current_logger->log_to_file(log_level::ERROR, fmt::format("Exception caught: {}", error_message));

        connection->try_refresh_connection();
        return false;
    }
    catch (const pqxx::conversion_error & e)
    {
        current_logger->log_to_file(log_level::ERROR, fmt::format("Conversion error: {}", e.what()));
        return false;
    }
    catch (const pqxx::internal_error & e)
    {
        current_logger->log_to_file(log_level::ERROR, fmt::format("PostgreSQL library internal error: {}", e.what()));
        return false;
    }
    catch (const std::exception & e)
    {
        current_logger->log_to_file(log_level::ERROR, e.what());
        return false;
    }

    if (is_committed)
    {
        current_logger->log_to_file(log_level::DEBUG, "Update lsn");
        update_lsn();
    }

    return true;
}
