#pragma once

#include <string>
#include <set>

#include <postgres/сonnection.h>

class postgres_settings {
public:
    postgres_settings(const std::string & connection_dsn_, logger *logger_);

    std::set<std::string> get_primary_key(std::string& table_name);
private:
    logger *current_logger;
    std::string connection_dsn;
};
