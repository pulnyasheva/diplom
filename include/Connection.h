#pragma once

#include <functional>
#include <pqxx/pqxx>
#include <boost/noncopyable.hpp>

#include <Logger.h>

namespace pqxx
{
    using ConnectionPtr = std::unique_ptr<connection>;
}

namespace postgres
{
    class Connection : boost::noncopyable
    {
    public:
        explicit Connection(
            const std::string & connection_dsn_,
            Logger *logger_,
            bool replication_ = false,
            size_t attempt_count = 3);

        void retryExecution(const std::function<void(pqxx::nontransaction &)> & exec);

        pqxx::connection & getRef();

        void connect();

        void refreshConnection();

        void tryRefreshConnection();

        bool isConnected() const { return connection != nullptr && connection->is_open(); }

        const std::string & getConnectionDSN() { return connection_dsn; }

    private:
        pqxx::ConnectionPtr connection;
        std::string connection_dsn;

        bool replication;
        size_t attempt_count;

        Logger *logger;
    };

    using ConnectionPtr = std::unique_ptr<Connection>;
}
