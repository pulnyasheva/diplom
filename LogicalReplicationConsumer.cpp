#include <iostream>
#include <fmt/format.h>
#include <pqxx/pqxx>

#include <LogicalReplicationConsumer.h>
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
      final_lsn(start_lsn),
      lsn_value(getLSNValue(start_lsn)),
      max_block_size(max_block_size_) {
}

bool LogicalReplicationConsumer::consume()
{
    bool slot_empty = true;
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
            lsn_value = getLSNValue(current_lsn);

            std::cout << fmt::format("Current message: {}", (*row)[1]) << std::endl;

            // Нужна логика для парсинга бинарных данных
        }
    }
    catch (const Exception & e)
    {
        logger->log(LogLevel::ERROR, fmt::format("Exception thrown in consume", e.what()));
        return false;
    }
    catch (const pqxx::broken_connection &)
    {
        logger->log(LogLevel::ERROR, "Connection was broken");
        connection->tryUpdateConnection();
        return false;
    }
    catch (const pqxx::sql_error & e)
    {
        /// For now sql replication interface is used and it has the problem that it registers relcache
        /// callbacks on each pg_logical_slot_get_changes and there is no way to invalidate them:
        /// https://github.com/postgres/postgres/blob/master/src/backend/replication/pgoutput/pgoutput.c#L1128
        /// So at some point will get out of limit and then they will be cleaned.
        std::string error_message = e.what();
        if (!error_message.find("out of relcache_callback_list slots"))
            logger->log(LogLevel::ERROR, fmt::format("Exception caught: {}",  error_message));

        connection->tryUpdateConnection();
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

    // Нужно обновить lsn, но не всегда (надо подумать когда)

    return true;
}
