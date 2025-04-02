#pragma once

#include <string>
#include <vector>

#include <сonnection.h>
#include <set>

class postgres_settings {
public:
    postgres_settings(std::shared_ptr<postgres::сonnection> connection_, logger *logger_);

    std::set<std::string> get_primary_key(std::string& table_name);
private:
    logger *current_logger;
    std::shared_ptr<postgres::сonnection> connection;
};
