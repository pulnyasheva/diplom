#include <iostream>
#include <fmt/format.h>
#include <pqxx/pqxx>

#include <LogicalReplicationConsumer.h>
#include <LogicalReplicationParser.h>
#include <Exception.h>

LogicalReplicationConsumer::LogicalReplicationConsumer(
    std::shared_ptr<postgres::Connection> connection_,
    const std::string &replication_slot_name_,
    const std::string &publication_name_,
    const std::string &start_lsn,
    size_t max_block_size_,
    Logger *logger_)
    : logger(logger_),
      replication_slot_name(replication_slot_name_),
      publication_name(publication_name_),
      connection(std::move(connection_)),
      current_lsn(start_lsn),
      result_lsn(start_lsn),
      lsn_value(getLSN(start_lsn)),
      max_block_size(max_block_size_) {
}

uint64_t LogicalReplicationConsumer::getLSN(const std::string & lsn)
{
    uint32_t upper_half;
    uint32_t lower_half;
    std::sscanf(lsn.data(), "%X/%X", &upper_half, &lower_half);
    return (static_cast<uint64_t>(upper_half) << 32) + lower_half;
}

void LogicalReplicationConsumer::updateLSN()
{
    try
    {
        auto tx = std::make_shared<pqxx::nontransaction>(connection->getRef());
        current_lsn = advanceLSN(tx);
        tx->commit();
    }
    catch (std::exception &e)
    {
        logger->log(LogLevel::ERROR, fmt::format("Error for update lsn: {}", e.what()));
    }
}

std::string LogicalReplicationConsumer::advanceLSN(std::shared_ptr<pqxx::nontransaction> tx) {
    std::string query_str = fmt::format("SELECT end_lsn FROM pg_replication_slot_advance('{}', '{}')",
                                        replication_slot_name, result_lsn);
    pqxx::result result{tx->exec(query_str)};

    result_lsn = result[0][0].as<std::string>();
    logger->log(LogLevel::DEBUG, fmt::format("Advanced LSN up to: {}", getLSN(result_lsn)));
    is_committed = false;
    return result_lsn;
}

bool LogicalReplicationConsumer::consume()
{
    updateLSN();
    bool is_slot_empty = true;
    try
    {
        auto tx = std::make_shared<pqxx::nontransaction>(connection->getRef());

        /// Read up to max_block_size rows changes (upto_n_changes parameter). It might return larger number as the limit
        /// is checked only after each transaction block.
        /// Returns less than max_block_changes, if reached end of wal. Sync to table in this case.
        std::string query_str = fmt::format(
                "select lsn, data FROM pg_logical_slot_peek_binary_changes("
                "'{}', NULL, {}, 'publication_names', '{}', 'proto_version', '1')",
                replication_slot_name, max_block_size, publication_name);

        auto stream{pqxx::stream_from::query(*tx, query_str)};

        LogicalReplicationParser parser = LogicalReplicationParser(&current_lsn, &result_lsn, &is_committed, logger);

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
            lsn_value = getLSN(current_lsn);

            std::cout << fmt::format("Current message: {}", (*row)[1]) << std::endl;

            try
            {
                parser.parseBinaryData((*row)[1].c_str(), (*row)[1].size());
            }
            catch (const Exception &e)
            {
                logger->log(LogLevel::ERROR, fmt::format("Error during parsing: {}", e.what()));
            }
        }
    }
    catch (const Exception &e)
    {
        logger->log(LogLevel::ERROR, fmt::format("Exception thrown in consume", e.what()));
        return false;
    }
    catch (const pqxx::broken_connection &)
    {
        logger->log(LogLevel::ERROR, "Connection was broken");
        connection->tryRefreshConnection();
        return false;
    }
    catch (const pqxx::sql_error &e)
    {
        std::string error_message = e.what();
        if (!error_message.find("out of relcache_callback_list slots"))
            logger->log(LogLevel::ERROR, fmt::format("Exception caught: {}", error_message));

        connection->tryRefreshConnection();
        return false;
    }
    catch (const pqxx::conversion_error & e)
    {
        logger->log(LogLevel::ERROR, fmt::format("Conversion error: {}", e.what()));
        return false;
    }
    catch (const pqxx::internal_error & e)
    {
        logger->log(LogLevel::ERROR, fmt::format("PostgreSQL library internal error: {}", e.what()));
        return false;
    }
    catch (const std::exception & e)
    {
        logger->log(LogLevel::ERROR, e.what());
        return false;
    }

    if (is_committed)
    {
        logger->log(LogLevel::DEBUG, "Update lsn");
        updateLSN();
    }

    return true;
}
