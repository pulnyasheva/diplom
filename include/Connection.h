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
            const std::string & connection_info_,
            Logger *logger_,
            bool replication_ = false,
            size_t num_tries = 3);

        void execWithRetry(const std::function<void(pqxx::nontransaction &)> & exec);

        pqxx::connection & getRef();

        void connect();

        void updateConnection();

        void tryUpdateConnection();

        bool isConnected() const { return connection != nullptr && connection->is_open(); }

        const std::string & getConnectionInfo() { return connection_info; }

    private:
        pqxx::ConnectionPtr connection;
        std::string connection_info;

        bool replication;
        size_t num_tries;

        Logger *logger;
    };

    using ConnectionPtr = std::unique_ptr<Connection>;
}
