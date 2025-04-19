#pragma once

#undef DAY
#undef SECOND

#include <exception>
#include <iostream>
#include <memory_resource>
#include <optional>
#include <vector>
#include <pqxx/pqxx>

#include <components/document/document.hpp>
#include <document_types.h>

using namespace components::document;
using namespace components;

namespace tsl {
    struct column_info {
        document_types type;
        std::string name;
    };

    struct doc_result {
        std::vector<column_info> schema;
        document_ptr document;
    };

    struct docs_result {
        std::vector<column_info> schema;
        std::vector<document_ptr> document;
    };

    doc_result logical_replication_to_docs(std::pmr::memory_resource *res, int16_t num_columns,
                                           const std::vector<std::pair<std::string, int32_t> > &columns,
                                           const std::vector<std::string> &result);

    docs_result postgres_to_docs(std::pmr::memory_resource *res, const pqxx::result &result);

    std::optional<std::vector<column_info>> merge_schemas(const std::vector<std::vector<column_info>>& schemas);
}
