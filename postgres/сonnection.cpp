#include <fmt/format.h>

#include <postgres/сonnection.h>
#include <common/logger.h>

namespace postgres
{
    сonnection::сonnection(const std::string & connection_dsn_, logger *logger_, bool replication_, size_t attempt_count_)
        : connection_dsn(connection_dsn_),
          replication(replication_),
          attempt_count(attempt_count_),
          current_logger(logger_) {

        if (replication)
            connection_dsn = fmt::format("{}?replication=database", connection_dsn);
    }

    void сonnection::retry_execution(const std::function<void(pqxx::nontransaction &)> & exec)
    {
        for (size_t attempt_ind = 0; attempt_ind < attempt_count; ++attempt_ind)
        {
            try
            {
                pqxx::nontransaction tx(get_ref());
                exec(tx);
                break;
            }
            catch (const pqxx::broken_connection & e)
            {
                current_logger->log_to_file(log_level::DEBUG, fmt::format(
                                "Cannot retry to connection failure, attempt: {}/{}. Message: {}",
                                attempt_ind, attempt_count, e.what()));

                if (attempt_ind + 1 == attempt_count)
                    throw;
            }
        }
    }

    pqxx::connection & сonnection::get_ref()
    {
        connect();
        return *connection;
    }

    void сonnection::try_refresh_connection()
    {
        try
        {
            refresh_connection();
        }
        catch (const pqxx::broken_connection & e)
        {
            current_logger->log_to_file(log_level::DEBUG, fmt::format("Unable to update connection: {}", e.what()));
        }
    }

    void сonnection::refresh_connection()
    {
        try {
            connection = std::make_unique<pqxx::connection>(connection_dsn);

            if (replication)
                connection->set_session_var("default_transaction_isolation", "repeatable read");

            current_logger->log_to_file(log_level::DEBUG, fmt::format("New connection {}", connection_dsn));
        } catch (const std::exception& e) {
            current_logger->log_to_file(log_level::ERROR, fmt::format("Connection update failed: {}", e.what()));
            throw;
        }
    }
    

    void сonnection::connect()
    {
        if (!connection || !connection->is_open())
            try_refresh_connection();
    }

}
