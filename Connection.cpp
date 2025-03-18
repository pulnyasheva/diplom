#include <fmt/format.h>

#include <Connection.h>
#include <Logger.h>

namespace postgres
{
    Connection::Connection(const std::string & connection_dsn_, Logger *logger_, bool replication_, size_t attempt_count_)
        : connection_dsn(connection_dsn_),
          replication(replication_),
          attempt_count(attempt_count_),
          logger(logger_) {

        if (replication)
            connection_dsn = fmt::format("{}?replication=database", connection_dsn);
    }

    void Connection::retryExecution(const std::function<void(pqxx::nontransaction &)> & exec)
    {
        for (size_t attempt_ind = 0; attempt_ind < attempt_count; ++attempt_ind)
        {
            try
            {
                pqxx::nontransaction tx(getRef());
                exec(tx);
                break;
            }
            catch (const pqxx::broken_connection & e)
            {
                logger->log(LogLevel::DEBUG, fmt::format(
                                "Cannot execute query due to connection failure, attempt: {}/{}. (Message: {})",
                                attempt_ind, attempt_count, e.what()));

                if (attempt_ind + 1 == attempt_count)
                    throw;
            }
        }
    }

    pqxx::connection & Connection::getRef()
    {
        connect();
        return *connection;
    }

    void Connection::tryRefreshConnection()
    {
        try
        {
            refreshConnection();
        }
        catch (const pqxx::broken_connection & e)
        {
            logger->log(LogLevel::DEBUG, fmt::format("Unable to update connection: {}", e.what()));
        }
    }

    void Connection::refreshConnection()
    {
        try {
            /// Always throws if there is no connection.
            connection = std::make_unique<pqxx::connection>(connection_dsn);

            if (replication)
                connection->set_variable("default_transaction_isolation", "'repeatable read'");

            logger->log(LogLevel::DEBUG, fmt::format("New connection {}", connection_dsn));
        } catch (const std::exception& e) {
            logger->log(LogLevel::ERROR, fmt::format("Connection update failed: {}", e.what()));
            throw;
        }
    }
    

    void Connection::connect()
    {
        if (!connection || !connection->is_open())
            refreshConnection();
    }

}
