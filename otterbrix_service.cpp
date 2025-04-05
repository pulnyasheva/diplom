#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>

#include <otterbrix_service.h>
#include <otterbrix_converter.h>

using namespace components;
using expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

void otterbrix_service::data_handler(postgre_sql_type_operation &type_operation,
                                    const std::string &table_name,
                                    const std::string &database_name,
                                    const std::vector<int32_t> &primary_key,
                                    const std::vector<std::string> &result,
                                    const std::vector<std::pair<std::string, int32_t>> &columns,
                                    const std::unordered_map<int32_t, std::string> &old_value) {
    auto resource = std::pmr::synchronized_pool_resource();
    switch (type_operation) {
        case postgre_sql_type_operation::INSERT: {
            tsl::doc_result doc_result = tsl::logical_replication_to_docs(&resource, columns.size(), columns, result);
            auto insert_node = logical_plan::make_node_insert(std::pmr::get_default_resource(),
                                                              {database_name, table_name},
                                                              doc_result.document);
        }
        case postgre_sql_type_operation::UPDATE: {
            tsl::doc_result doc_result = tsl::logical_replication_to_docs(&resource, columns.size(), columns, result);
            std::pair<expressions::expression_ptr, logical_plan::parameter_node_ptr> expression;
            if (old_value.empty()) {
                expression = make_expression_match(&resource, old_value, columns);
            } else {
                expression = make_expression_match(&resource, primary_key, result, columns);
            }

            auto node_match = logical_plan::make_node_match(&resource,
                                                            {database_name, table_name},
                                                            std::move(expression.first));
            auto node_update = logical_plan::make_node_update_one(&resource,
                                                                  {database_name, table_name},
                                                                  &node_match,
                                                                  doc_result.document);
        }
        case postgre_sql_type_operation::DELETE: {
            std::pair<expressions::expression_ptr, logical_plan::parameter_node_ptr> expression
                = make_expression_match(&resource, primary_key, result, columns);
            auto node_match = logical_plan::make_node_match(&resource,
                                                            {database_name, table_name},
                                                            std::move(expression.first));
            auto node_delete = logical_plan::make_node_delete(&resource,  {database_name, table_name}, &node_match);
        }
    }
}

void otterbrix_service::data_handler(pqxx::result &result,
                                     const std::string &table_name,
                                     const std::string &database_name) {
    auto resource = std::pmr::synchronized_pool_resource();
    tsl::docs_result docs_result = tsl::postgres_to_docs(&resource, result);

    for (auto doc: docs_result) {
        auto insert_node = logical_plan::make_node_insert(std::pmr::get_default_resource(),
                                                          {database_name, table_name},
                                                          doc.document);
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
        auto params = logical_plan::make_parameter_node(&resource);
        params->add_parameter(id_par{1}, new_value(primary_key[0].second));
        auto expr = components::expressions::make_compare_expression(&resource,
                                                                     compare_type::eq,
                                                                     key{columns[primary_key[0].first].first},
                                                                     id_par{1});
        return {expr, params};
    }

    int32_t index = 0;
    auto expr_result = components::expressions::make_compare_union_expression(&resource, compare_type::union_and);
    auto expr = expr_result;
    auto params = logical_plan::make_parameter_node(&resource);

    while (primary_key_size > 2) {
        auto expr_union = components::expressions::make_compare_union_expression(&resource, compare_type::union_and);
        auto expr_eq = components::expressions::make_compare_expression(&resource,
                                                                        compare_type::eq,
                                                                        key{columns[primary_key[index].first].first},
                                                                        id_par{index + 1});
        params->add_parameter(id_par{index + 1}, new_value(primary_key[index].second));
        expr->append_child(expr_union);
        expr->append_child(expr_eq);
        expr = expr_union;
        index += 1;
        primary_key_size -= 1;
    }

    auto expr_eq_left = components::expressions::make_compare_expression(&resource,
                                                                         compare_type::eq,
                                                                         key{columns[primary_key[index].first].first},
                                                                         id_par{index + 1});
    params->add_parameter(id_par{index + 1}, new_value(primary_key[index].second));
    auto expr_eq_right = components::expressions::make_compare_expression(&resource,
                                                                          compare_type::eq,
                                                                          key{columns[primary_key[index + 1].first].first},
                                                                          id_par{index + 2});
    params->add_parameter(id_par{index + 2}, new_value(primary_key[index + 1].second));
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
        auto params = logical_plan::make_parameter_node(&resource);
        params->add_parameter(id_par{1}, new_value(result[primary_key[0]]));
        auto expr = components::expressions::make_compare_expression(&resource,
                                                                     compare_type::eq,
                                                                     key{columns[primary_key[0]].first},
                                                                     id_par{1});
        return {expr, params};
    }

    int32_t index = 0;
    auto expr_result = components::expressions::make_compare_union_expression(&resource, compare_type::union_and);
    auto expr = expr_result;
    auto params = logical_plan::make_parameter_node(&resource);

    while (primary_key_size > 2) {
        auto expr_union = components::expressions::make_compare_union_expression(&resource, compare_type::union_and);
        auto expr_eq = components::expressions::make_compare_expression(&resource,
                                                                        compare_type::eq,
                                                                        key{columns[primary_key[index]].first},
                                                                        id_par{index + 1});
        params->add_parameter(id_par{index + 1}, new_value(result[primary_key[index]]));
        expr->append_child(expr_union);
        expr->append_child(expr_eq);
        expr = expr_union;
        index += 1;
        primary_key_size -= 1;
    }

    auto expr_eq_left = components::expressions::make_compare_expression(&resource,
                                                                         compare_type::eq,
                                                                         key{columns[primary_key[index]].first},
                                                                         id_par{index + 1});
    params->add_parameter(id_par{index + 1}, new_value(result[primary_key[index]]));
    auto expr_eq_right = components::expressions::make_compare_expression(&resource,
                                                                          compare_type::eq,
                                                                          key{columns[primary_key[index + 1]].first},
                                                                          id_par{index + 2});
    params->add_parameter(id_par{index + 2}, new_value(result[primary_key[index + 1]]));
    expr->append_child(expr_eq_left);
    expr->append_child(expr_eq_right);

    return {expr_result, params};
}