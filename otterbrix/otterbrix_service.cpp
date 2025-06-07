#include <spdlog/sinks/stdout_color_sinks.h>
#include <actor-zeta/base/address.hpp>

#include <otterbrix/otterbrix_service.h>
#include <otterbrix/otterbrix_converter.h>

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/log/log.hpp>
#include <integration/cpp/wrapper_dispatcher.hpp>

using expressions::compare_type;
using key = expressions::key_t;
using id_par = core::parameter_id_t;

void otterbrix_service::data_handler(postgre_sql_type_operation type_operation,
                                    const std::string &table_name,
                                    const std::string &database_name,
                                    const std::vector<int32_t> &primary_key,
                                    const std::vector<std::string> &result,
                                    const std::vector<std::pair<std::string, int32_t>> &columns,
                                    const std::unordered_map<int32_t, std::string> &old_value,
                                    std::pmr::memory_resource* resource) {
    switch (type_operation) {
        case postgre_sql_type_operation::INSERT: {
            tsl::doc_result doc_result = tsl::logical_replication_to_docs(resource, columns.size(), columns, result, type_operation);
            auto insert_node = logical_plan::make_node_insert(resource,
                                                              {database_name, table_name},
                                                              doc_result.document);
            std::cout << "create node insert" << std::endl;
            result_node result_node;
            result_node.node = insert_node;
            result_node.has_parameter = false;
            queue.enqueue(result_node);
            break;
        }
        case postgre_sql_type_operation::UPDATE: {
            tsl::doc_result doc_result = tsl::logical_replication_to_docs(resource, columns.size(), columns, result, type_operation);
            std::pair<expressions::expression_ptr, logical_plan::parameter_node_ptr> expression;
            if (!old_value.empty()) {
                expression = make_expression_match(resource, old_value, columns);
            } else {
                expression = make_expression_match(resource, primary_key, result, columns);
            }

            auto node_match = logical_plan::make_node_match(resource,
                                                            {database_name, table_name},
                                                            std::move(expression.first));
            auto node_update = logical_plan::make_node_update_one(resource,
                                                                  {database_name, table_name},
                                                                  node_match,
                                                                  doc_result.document);
            std::cout << "create node update" << std::endl;
            result_node result_node;
            result_node.node = node_update;
            result_node.parameter = expression.second;
            result_node.has_parameter = true;
            queue.enqueue(result_node);
            break;
        }
        case postgre_sql_type_operation::DELETE: {
            std::pair<expressions::expression_ptr, logical_plan::parameter_node_ptr> expression
                = make_expression_match(resource, primary_key, result, columns);
            auto node_match = logical_plan::make_node_match(resource,
                                                            {database_name, table_name},
                                                            std::move(expression.first));
            auto node_delete = logical_plan::make_node_delete_one(resource,
                                                              {database_name, table_name},
                                                              node_match);
            std::cout << "create node delete" << std::endl;
            result_node result_node;
            result_node.node = node_delete;
            result_node.parameter = expression.second;
            result_node.has_parameter = true;
            queue.enqueue(result_node);
            break;
        }
    }
}

void otterbrix_service::data_handler(pqxx::result &result,
                                     const std::string &table_name,
                                     const std::string &database_name,
                                     std::pmr::memory_resource* resource) {
    tsl::docs_result docs_result = tsl::postgres_to_docs(resource, result);

    for (auto doc: docs_result.document) {
        auto insert_node = logical_plan::make_node_insert(resource,
                                                          {database_name, table_name},
                                                          doc);
        std::cout << database_name << std::endl;
        std::cout << "create insert node snapshot" << std::endl;
        result_node result_node;
        result_node.node = insert_node;
        result_node.has_parameter = false;
        queue.enqueue(result_node);
    }
}

std::pair<expressions::expression_ptr, logical_plan::parameter_node_ptr> otterbrix_service::make_expression_match(
    std::pmr::memory_resource* resource,
    const std::unordered_map<int32_t, std::string> &old_value,
    const std::vector<std::pair<std::string, int32_t>> &columns) {
    std::vector<std::pair<int32_t, std::string>> primary_key;
    primary_key.reserve(old_value.size());

    for (const auto &entry : old_value) {
        primary_key.emplace_back(entry.first, entry.second);
    }

    size_t primary_key_size = primary_key.size();
    if (primary_key_size == 1) {
        auto params = logical_plan::make_parameter_node(resource);
        if (columns[primary_key[0].first].first == "_id") {
            std::cout << "parameter _id" << std::endl;
            params->add_parameter(id_par{1}, tsl::gen_id(std::stoll(primary_key[0].second), resource));
        } else {
            params->add_parameter(id_par{1}, primary_key[0].second);
        }
        auto expr = make_compare_expression(resource,
                                            compare_type::eq,
                                            key{columns[primary_key[0].first].first},
                                            id_par{1});
        return {expr, params};
    }

    unsigned short index = 0;
    auto expr_result = components::expressions::make_compare_union_expression(resource, compare_type::union_and);
    auto expr = expr_result;
    auto params = logical_plan::make_parameter_node(resource);

    while (primary_key_size > 2) {
        auto expr_union = components::expressions::make_compare_union_expression(resource, compare_type::union_and);
        auto expr_eq = components::expressions::make_compare_expression(resource,
                                                                        compare_type::eq,
                                                                        key{columns[primary_key[index].first].first},
                                                                        id_par{static_cast<unsigned short>(index + 1)});
        params->add_parameter(id_par{static_cast<unsigned short>(index + 1)}, primary_key[index].second);
        expr->append_child(expr_union);
        expr->append_child(expr_eq);
        expr = expr_union;
        index += 1;
        primary_key_size -= 1;
    }

    auto expr_eq_left = components::expressions::make_compare_expression(resource,
                                                                         compare_type::eq,
                                                                         key{columns[primary_key[index].first].first},
                                                                         id_par{static_cast<unsigned short>(index + 1)});
    params->add_parameter(id_par{static_cast<unsigned short>(index + 1)}, primary_key[index].second);
    auto expr_eq_right = components::expressions::make_compare_expression(resource,
                                                                          compare_type::eq,
                                                                          key{columns[primary_key[index + 1].first].first},
                                                                          id_par{static_cast<unsigned short>(index + 2)});
    params->add_parameter(id_par{static_cast<unsigned short>(index + 2)}, primary_key[index + 1].second);
    expr->append_child(expr_eq_left);
    expr->append_child(expr_eq_right);

    return {expr_result, params};
}

std::pair<expressions::expression_ptr, logical_plan::parameter_node_ptr> otterbrix_service::make_expression_match(
    std::pmr::memory_resource* resource,
    const std::vector<int32_t> &primary_key,
    const std::vector<std::string> &result,
    const std::vector<std::pair<std::string, int32_t>> &columns) {
    size_t primary_key_size = primary_key.size();
    if (primary_key_size == 1) {
        auto params = logical_plan::make_parameter_node(resource);
        if (columns[primary_key[0]].first == "_id") {
            std::cout << "parameter _id" << std::endl;
            params->add_parameter(id_par{1}, tsl::gen_id(std::stoll(result[primary_key[0]]), resource));
        } else {
            params->add_parameter(id_par{1}, result[primary_key[0]]);
        }
        auto expr = make_compare_expression(resource,
                                            compare_type::eq,
                                            key{std::string("/") + columns[primary_key[0]].first},
                                            id_par{1});
        return {expr, params};
    }

    int32_t index = 0;
    auto expr_result = components::expressions::make_compare_union_expression(resource, compare_type::union_and);
    auto expr = expr_result;
    auto params = logical_plan::make_parameter_node(resource);

    while (primary_key_size > 2) {
        auto expr_union = components::expressions::make_compare_union_expression(resource, compare_type::union_and);
        auto expr_eq = components::expressions::make_compare_expression(resource,
                                                                        compare_type::eq,
                                                                        key{columns[primary_key[index]].first},
                                                                        id_par{static_cast<unsigned short>(index + 1)});
        params->add_parameter(id_par{static_cast<unsigned short>(index + 1)}, result[primary_key[index]]);
        expr->append_child(expr_union);
        expr->append_child(expr_eq);
        expr = expr_union;
        index += 1;
        primary_key_size -= 1;
    }

    auto expr_eq_left = components::expressions::make_compare_expression(resource,
                                                                         compare_type::eq,
                                                                         key{columns[primary_key[index]].first},
                                                                         id_par{static_cast<unsigned short>(index + 1)});
    params->add_parameter(id_par{static_cast<unsigned short>(index + 1)}, result[primary_key[index]]);
    auto expr_eq_right = components::expressions::make_compare_expression(resource,
                                                                          compare_type::eq,
                                                                          key{columns[primary_key[index + 1]].first},
                                                                          id_par{static_cast<unsigned short>(index + 2)});
    params->add_parameter(id_par{static_cast<unsigned short>(index + 2)}, result[primary_key[index + 1]]);
    expr->append_child(expr_eq_left);
    expr->append_child(expr_eq_right);

    return {expr_result, params};
}