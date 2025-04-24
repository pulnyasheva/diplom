#pragma once

#include <functional>
#include <pqxx/pqxx>
#include <boost/noncopyable.hpp>

#include <common/logger.h>

namespace pqxx
{
    using connection_ptr = std::unique_ptr<connection>;
}

namespace postgres
{
    class сonnection : boost::noncopyable
    {
    public:
        explicit сonnection(
            const std::string & connection_dsn_,
            logger *logger_,
            bool replication_ = false,
            size_t attempt_count = 3);

        void retry_execution(const std::function<void(pqxx::nontransaction &)> & exec);

        pqxx::connection & get_ref();

        void connect();

        void refresh_connection();

        void try_refresh_connection();

        bool is_connected() const { return connection != nullptr && connection->is_open(); }

        const std::string & get_connection_dsn() { return connection_dsn; }

    private:
        pqxx::connection_ptr connection;
        std::string connection_dsn;

        bool replication;
        size_t attempt_count;

        logger *current_logger;
    };

    using connection_ptr = std::unique_ptr<сonnection>;
}
