#pragma once

#include <memory_resource>
#include <vector>
#include <pqxx/pqxx>
#include <spdlog/spdlog.h>

#include <postgres/postgres_types.h>

#include <components/expressions/key.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/expressions/forward.hpp>

using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

class otterbrix_service {
public:
    std::shared_ptr<spdlog::logger> underlying_logger;

    void data_handler(postgre_sql_type_operation type_operation,
                      const std::string &table_name,
                      const std::string &database_name,
                      const std::vector<int32_t> &primary_key,
                      const std::vector<std::string> &result,
                      const std::vector<std::pair<std::string, int32_t>> &columns,
                      const std::unordered_map<int32_t, std::string> &old_value);

    void data_handler(pqxx::result &result,
                      const std::string &table_name,
                      const std::string &database_name);
private:
    std::pair<components::expressions::expression_ptr,
    components::logical_plan::parameter_node_ptr> make_expression_match(
        std::pmr::memory_resource* resource,
        const std::vector<int32_t> &primary_key,
        const std::vector<std::string> &result,
        const std::vector<std::pair<std::string, int32_t>> &columns);

    std::pair<components::expressions::expression_ptr,
    components::logical_plan::parameter_node_ptr> make_expression_match(
    std::pmr::memory_resource* resource,
    const std::unordered_map<int32_t, std::string> &old_value,
    const std::vector<std::pair<std::string, int32_t>> &columns);
};
