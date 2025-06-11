#pragma once

#include <memory_resource>
#include <vector>
#include <pqxx/pqxx>
#include <future>
#include <spdlog/spdlog.h>

#include <postgres/postgres_types.h>

#include <components/expressions/key.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/expressions/forward.hpp>
#include <lock_free_queue/readerwriterqueue.h>
#include <integration/cpp/otterbrix.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/log/log.hpp>

#include <logical_replication/logical_replication_parser.h>

using namespace moodycamel;

using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

struct result_node {
    components::logical_plan::node_ptr node;
    components::logical_plan::parameter_node_ptr parameter;
    bool has_parameter;
};

class otterbrix_service {
public:
    explicit otterbrix_service(
        ReaderWriterQueue<result_node> &queue_shapshots_,
        ReaderWriterQueue<std::future<std::vector<result_node>>> &result_queue_)
        : queue_shapshots(queue_shapshots_),
          result_queue(result_queue_) {
    }
    otterbrix_service(otterbrix_service&&) noexcept;

    std::shared_ptr<spdlog::logger> underlying_logger;

    void data_handler(postgre_sql_type_operation type_operation,
                      const std::string &table_name,
                      const std::string &database_name,
                      const std::vector<int32_t> &primary_key,
                      const std::vector<std::string> &result,
                      const std::vector<std::pair<std::string, int32_t>> &columns,
                      const std::unordered_map<int32_t, std::string> &old_value,
                      std::pmr::memory_resource* resource,
                      result_node &result_node);

    void data_handler(pqxx::result &result,
                      const std::string &table_name,
                      const std::string &database_name,
                      std::pmr::memory_resource* resource);
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

    void add_parameter_value(components::logical_plan::parameter_node_ptr &params,
                             unsigned short num,
                             const std::string &name,
                             const std::string &value,
                             std::pmr::memory_resource *resource);

    ReaderWriterQueue<result_node> &queue_shapshots;
    ReaderWriterQueue<std::future<std::vector<result_node>>> &result_queue;
};
