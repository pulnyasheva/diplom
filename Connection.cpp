#include <fmt/format.h>

#include "Connection.h"
#include "Logger.h"

namespace postgres
{
    Connection::Connection(const std::string & connection_info_, Logger *logger_, bool replication_, size_t num_tries_)
        : connection_info(connection_info_),
          replication(replication_),
          num_tries(num_tries_),
          logger(logger_) {

        if (replication)
            connection_info = fmt::format("{}?replication=database", connection_info);
    }

    void Connection::execWithRetry(const std::function<void(pqxx::nontransaction &)> & exec)
    {
        for (size_t try_no = 0; try_no < num_tries; ++try_no)
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
                                try_no, num_tries, e.what()));

                if (try_no + 1 == num_tries)
                    throw;
            }
        }
    }

    pqxx::connection & Connection::getRef()
    {
        connect();
        return *connection;
    }

    void Connection::tryUpdateConnection()
    {
        try
        {
            updateConnection();
        }
        catch (const pqxx::broken_connection & e)
        {
            logger->log(LogLevel::DEBUG, fmt::format("Unable to update connection: {}", e.what()));
        }
    }

    void Connection::updateConnection()
    {
        try {
            /// Always throws if there is no connection.
            connection = std::make_unique<pqxx::connection>(connection_info);

            if (replication)
                connection->set_variable("default_transaction_isolation", "'repeatable read'");

            logger->log(LogLevel::DEBUG, fmt::format("New connection {}", connection_info));
        } catch (const std::exception& e) {
            logger->log(LogLevel::ERROR, fmt::format("Connection update failed: {}", e.what()));
            throw;
        }
    }
    

    void Connection::connect()
    {
        if (!connection || !connection->is_open())
            updateConnection();
    }

}
