#pragma once

#undef DAY
#undef SECOND

#include <exception>
#include <iostream>
#include <memory_resource>
#include <optional>
#include <vector>

#include <DocumentTypes.h>

using namespace components::document;
using namespace components;

namespace tsl {
    struct column_info {
        DocumentTypes type;
        std::string name;
    };

    struct doc_result {
        std::vector<column_info> schema;
        components::document::document_ptr document;
    };

    doc_result postgres_to_docs(const int16_t &num_columns,
                               const std::vector<std::pair<std::string, int32_t>>& columns,
                               const std::vector<std::string> &result);

    std::optional<std::vector<column_info>> merge_schemas(const std::vector<std::vector<column_info>>& schemas);
}
