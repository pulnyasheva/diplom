#include <functional>
#include <sstream>

#include <otterbrix/document_types.h>
#include <otterbrix/otterbrix_converter.h>
#include <logical_replication/logical_replication_parser.h>
#include <postgres/postgres_types.h>

using logical_replication_to_otterbrix_doc = std::function<void(components::document::document_ptr,
                                                                const std::vector<std::string> &)>;
using logical_replication_to_otterbrix_doc_impl =
    std::function<void(
        components::document::document_ptr,
        const std::string &,
        const std::vector<std::string> &,
        const int16_t &)>;

using postgres_to_otterbrix_doc = std::function<void(components::document::document_ptr, const pqxx::row &)>;
using postgres_to_otterbrix_doc_impl =
    std::function<void(
        components::document::document_ptr,
        const std::string &,
        const pqxx::row &,
        size_t)>;

namespace {
    std::vector<std::string> parse_string(const std::string& input) {
        std::vector<std::string> result;

        std::string cleanedInput = input;
        if (cleanedInput.front() == '{') {
            cleanedInput.erase(cleanedInput.begin());
        }
        if (cleanedInput.back() == '}') {
            cleanedInput.erase(cleanedInput.end() - 1);
        }

        std::stringstream ss(cleanedInput);
        std::string item;

        while (std::getline(ss, item, ',')) {
            result.push_back(item);
        }

        return result;
    }

    void
    set_int8(components::document::document_ptr doc,
             const std::string &name,
             const std::vector<std::string> &result,
             const int16_t &index)
    {
        if (result[index] == emptyValue) {
            doc->set(name, nullptr);
            return;
        }
        int64_t int_value = std::stoll(result[index]);
        doc->set<int8_t>(name, int_value);
    }

    void
    set_int16(components::document::document_ptr doc,
              const std::string &name,
              const std::vector<std::string> &result,
              const int16_t &index)
    {
        if (result[index] == emptyValue) {
            doc->set(name, nullptr);
            return;
        }
        int64_t int_value = std::stoll(result[index]);
        doc->set<int16_t>(name, int_value);
    }

    void
    set_int32(components::document::document_ptr doc,
              const std::string &name,
              const std::vector<std::string> &result,
              const int16_t &index)
    {
        if (result[index] == emptyValue) {
            doc->set(name, nullptr);
            return;
        }
        int64_t int_value = std::stoll(result[index]);
        doc->set<int32_t>(name, int_value);
    }

    void
    set_int64(components::document::document_ptr doc,
              const std::string &name,
              const std::vector<std::string> &result,
              const int16_t &index)
    {
        if (result[index] == emptyValue) {
            doc->set(name, nullptr);
            return;
        }
        int64_t int_value = std::stoll(result[index]);
        doc->set<int64_t>(name, int_value);
    }

    void
    set_float(components::document::document_ptr doc,
              const std::string &name,
              const std::vector<std::string> &result,
              const int16_t &index)
    {
        if (result[index] == emptyValue) {
            doc->set(name, nullptr);
            return;
        }
        float float_value = std::stof(result[index]);
        doc->set<float>(name, float_value);
    }

    void
    set_double(components::document::document_ptr doc,
               const std::string &name,
               const std::vector<std::string> &result,
               const int16_t &index) {
        if (result[index] == emptyValue) {
            doc->set(name, nullptr);
            return;
        }
        double double_value = std::stod(result[index]);
        doc->set<double>(name, double_value);
    }

    void
    set_bit(components::document::document_ptr doc,
            const std::string &name,
            const std::vector<std::string> &result,
            const int16_t &index) {
        if (result[index] == emptyValue) {
            doc->set(name, nullptr);
            return;
        }
        if (result[index] == "1" || result[index] == "true") {
            doc->set<bool>(name, true);
        } else {
            doc->set<bool>(name, false);
        }
    }

    void
    set_string(components::document::document_ptr doc,
               const std::string &name,
               const std::vector<std::string> &result,
               const int16_t &index) {
        if (result[index] == emptyValue) {
            doc->set(name, nullptr);
            return;
        }
        doc->set<std::string>(name, result[index]);
    }

    logical_replication_to_otterbrix_doc_impl type_to_translator_array(const int32_t& type) {
        switch (type) {
            case static_cast<int32_t>(postgres_array_types::BIT):
            case static_cast<int32_t>(postgres_array_types::BOOL):
                return set_bit;
            case static_cast<int32_t>(postgres_array_types::INT8):
            case static_cast<int32_t>(postgres_array_types::INT2):
            case static_cast<int32_t>(postgres_array_types::INT4):
                return set_int8;
            case static_cast<int32_t>(postgres_array_types::CHAR):
            case static_cast<int32_t>(postgres_array_types::TEXT):
            case static_cast<int32_t>(postgres_array_types::VARCHAR):
            case static_cast<int32_t>(postgres_array_types::UUID):
                return set_string;
            case static_cast<int32_t>(postgres_array_types::FLOAT):
                return set_float;
            case static_cast<int32_t>(postgres_array_types::DOUBLE):
                return set_double;
            case static_cast<int32_t>(postgres_array_types::NUMERIC):
                return set_int64;
            default:
            {
                std::stringstream oss;
                oss << "Cant find row to doc array translator for type: " << type;
                std::cerr << oss.str() << std::endl;
                throw std::runtime_error(oss.str());
            }
        }

    }

    void
    set_array(components::document::document_ptr doc,
               const std::string &name,
               const std::vector<std::string> &result,
               const int16_t &index,
               const int32_t& type) {
        if (result[index] == emptyValue) {
            doc->set(name, nullptr);
            return;
        }

        std::vector<std::string> values = parse_string(result[index]);
        doc->set_array(name);
        auto translator = type_to_translator_array(type);

        for (int i = 0; i < values.size(); i++) {
            translator(doc->get_array(name), std::to_string(i), std::vector{values[i]}, 0);
        }
    }

    void
    set_int8_postgres(components::document::document_ptr doc,
                      const std::string &name,
                      const pqxx::row &result,
                      const int16_t &index) {
        if (result[index].is_null()) {
            doc->set(name, nullptr);
            return;
        }
        int64_t int_value = result[index].get<int64_t>().value();
        doc->set<int8_t>(name, int_value);
    }

    void
    set_int16_postgres(components::document::document_ptr doc,
                       const std::string &name,
                       const pqxx::row &result,
                       const int16_t &index) {
        if (result[index].is_null()) {
            doc->set(name, nullptr);
            return;
        }
        int64_t int_value = result[index].get<int64_t>().value();
        doc->set<int16_t>(name, int_value);
    }

    void
    set_int32_postgres(components::document::document_ptr doc,
                       const std::string &name,
                       const pqxx::row &result,
                       const int16_t &index) {
        if (result[index].is_null()) {
            doc->set(name, nullptr);
            return;
        }
        int64_t int_value = result[index].get<int64_t>().value();
        doc->set<int32_t>(name, int_value);
    }

    void
    set_int64_postgres(components::document::document_ptr doc,
                       const std::string &name,
                       const pqxx::row &result,
                       const int16_t &index) {
        if (result[index].is_null()) {
            doc->set(name, nullptr);
            return;
        }
        int64_t int_value = result[index].get<int64_t>().value();
        doc->set<int64_t>(name, int_value);
    }

    void
    set_float_postgres(components::document::document_ptr doc,
                       const std::string &name,
                       const pqxx::row &result,
                       const int16_t &index)
    {
        if (result[index].is_null()) {
            doc->set(name, nullptr);
            return;
        }
        float float_value = result[index].get<float>().value();
        doc->set<float>(name, float_value);
    }

    void
    set_double_postgres(components::document::document_ptr doc,
                        const std::string &name,
                        const pqxx::row &result,
                        const int16_t &index) {
        if (result[index].is_null()) {
            doc->set(name, nullptr);
            return;
        }
        double double_value = result[index].get<double>().value();
        doc->set<double>(name, double_value);
    }

    void
    set_bit_postgres(components::document::document_ptr doc,
                     const std::string &name,
                     const pqxx::row &result,
                     const int16_t &index) {
        if (result[index].is_null()) {
            doc->set(name, nullptr);
            return;
        }
        bool bool_value = result[index].get<bool>().value();
        doc->set<bool>(name, false);
    }

    void
    set_string_postgres(components::document::document_ptr doc,
                        const std::string &name,
                        const pqxx::row &result,
                        const int16_t &index) {
        if (result[index].is_null()) {
            doc->set(name, nullptr);
            return;
        }
        std::string string_value = result[index].c_str();
        doc->set<std::string>(name, string_value);
    }

    void
    set_array_postgres(components::document::document_ptr doc,
                       const std::string &name,
                       const pqxx::row &result,
                       const int16_t &index,
                       const int32_t &type) {
        if (result[index].is_null()) {
            doc->set(name, nullptr);
            return;
        }

        std::string string_value = result[index].c_str();
        std::vector<std::string> values = parse_string(string_value);
        doc->set_array(name);
        auto translator = type_to_translator_array(type);

        for (int i = 0; i < values.size(); i++) {
            translator(doc->get_array(name), std::to_string(i), std::vector{values[i]}, 0);
        }
    }

    struct logical_replication_to_doc_setter {
        document_types type;
        logical_replication_to_otterbrix_doc_impl setter;
    };

    struct postgres_to_doc_setter {
        document_types type;
        postgres_to_otterbrix_doc_impl setter;
    };

    logical_replication_to_doc_setter logical_replication_to_doc(const int32_t& type) {
        postgres_types postgres_types = get_enum(type);
        switch (postgres_types) {
            case postgres_types::INT2:
            case postgres_types::INT4:
            case postgres_types::INT8:
            {
                return {document_types::INT8, set_int8};
            }
            case postgres_types::NUMERIC:
            {
                return {document_types::INT64, set_int64};
            }
            case postgres_types::BIT:
            {
                return {document_types::BOOL, set_bit};
            }
            case postgres_types::FLOAT:
            {
                return {document_types::FLOAT, set_float};
            }
            case postgres_types::DOUBLE:
            {
                return {document_types::DOUBLE, set_double};
            }
            case postgres_types::TEXT:
            case postgres_types::CHAR:
            case postgres_types::VARCHAR:
            case postgres_types::UUID:
            {
                return {document_types::STRING, set_string};
            }
            case postgres_types::ARRAY: {
                auto setter = [type_element = type](components::document::document_ptr doc,
                                                    const std::string &name,
                                                    const std::vector<std::string> &result,
                                                    const int16_t &index) ->
                    void { set_array(doc, name, result, index, type_element); };
                logical_replication_to_doc_setter row_to_doc_setter;
                row_to_doc_setter.setter = std::move(setter);
                row_to_doc_setter.type = document_types::ARRAY;
                return row_to_doc_setter;
            }
            default:
            {
                std::stringstream oss;
                oss << "Cant find row to doc translator for type: " << type;
                std::cerr << oss.str() << std::endl;
                throw std::runtime_error(oss.str());
            }
        }
    }

    postgres_to_doc_setter postgres_to_doc(const int32_t& type) {
        postgres_types postgres_types = get_enum(type);
        switch (postgres_types) {
            case postgres_types::INT2:
            case postgres_types::INT4:
            case postgres_types::INT8:
            {
                return {document_types::INT8, set_int8_postgres};
            }
            case postgres_types::NUMERIC:
            {
                return {document_types::INT64, set_int64_postgres};
            }
            case postgres_types::BIT:
            {
                return {document_types::BOOL, set_bit_postgres};
            }
            case postgres_types::FLOAT:
            {
                return {document_types::FLOAT, set_float_postgres};
            }
            case postgres_types::DOUBLE:
            {
                return {document_types::DOUBLE, set_double_postgres};
            }
            case postgres_types::TEXT:
            case postgres_types::CHAR:
            case postgres_types::VARCHAR:
            case postgres_types::UUID:
            {
                return {document_types::STRING, set_string_postgres};
            }
            case postgres_types::ARRAY: {
                auto setter = [type_element = type](components::document::document_ptr doc,
                                                    const std::string &name,
                                                    const pqxx::row &result,
                                                    const int16_t &index) ->
                    void { set_array_postgres(doc, name, result, index, type_element); };
                postgres_to_doc_setter row_to_doc_setter;
                row_to_doc_setter.setter = std::move(setter);
                row_to_doc_setter.type = document_types::ARRAY;
                return row_to_doc_setter;
            }
            default:
            {
                std::stringstream oss;
                oss << "Cant find row to doc translator for type: " << type;
                std::cerr << oss.str() << std::endl;
                throw std::runtime_error(oss.str());
            }
        }
    }
} // namespace

namespace tsl {
    docs_result postgres_to_docs(std::pmr::memory_resource *res, const pqxx::result &result) {
        const auto ncolumns = result.columns();
        const auto nrows = result.size();

        std::vector<postgres_to_otterbrix_doc> postgres_row_to_doc_translators;
        postgres_row_to_doc_translators.reserve(ncolumns);
        std::vector<column_info> schema;
        schema.reserve(ncolumns);

        size_t counter = 0;
        for (const auto &column: result[0]) {
            auto translator = postgres_to_doc(column.type());
            schema.emplace_back(translator.type, column.name());

            auto wrapper = [translator = translator.setter, index = counter++, name = column.name()](
                components::document::document_ptr doc,
                const pqxx::row &row) -> void {
                translator(doc, name, row, index);
            };
            postgres_row_to_doc_translators.push_back(std::move(wrapper));
        }

        assert(postgres_row_to_doc_translators.size() == ncolumns);
        if (postgres_row_to_doc_translators.size() != ncolumns) {
            std::stringstream oss;
            oss << "Invalid number of translators: " << postgres_row_to_doc_translators.size() << " expected: " <<
                    ncolumns;
            std::cerr << oss.str() << std::endl;
            throw std::runtime_error(oss.str());
        }

        std::vector<components::document::document_ptr> docs;
        docs.reserve(nrows);

        for (const auto &row: result) {
             components::document::document_ptr doc =  components::document::make_document(res);
            for (const auto &translator: postgres_row_to_doc_translators) {
                translator(doc, row);
            }
            docs.push_back(std::move(doc));
        }

        return {std::move(schema), std::move(docs)};
    }

    doc_result logical_replication_to_docs(std::pmr::memory_resource *res, int16_t num_columns,
                                           const std::vector<std::pair<std::string, int32_t> > &columns,
                                           const std::vector<std::string> &result) {
        std::vector<logical_replication_to_otterbrix_doc> postgres_row_to_doc_translators;
        postgres_row_to_doc_translators.reserve(num_columns);
        std::vector<column_info> schema;
        schema.reserve(num_columns);

        for (int16_t i = 0; i < num_columns; i++) {

            auto translator = logical_replication_to_doc(columns[i].second);
            schema.emplace_back(translator.type, columns[i].first);

            auto wrapper = [translator = translator.setter, index = i, name = columns[i].first](
                               components::document::document_ptr doc,
                               const std::vector<std::string> &result) -> void { translator(doc, name, result, index); };
            postgres_row_to_doc_translators.push_back(std::move(wrapper));
        }

        assert(postgres_row_to_doc_translators.size() == num_columns);
        if (postgres_row_to_doc_translators.size() != num_columns) {
            std::stringstream oss;
            oss << "Invalid number of translator: " << postgres_row_to_doc_translators.size() << " expected: " << num_columns;
            std::cerr << oss.str() << std::endl;
            throw std::runtime_error(oss.str());
        }

        components::document::document_ptr doc =  components::document::make_document(res);
        for (const auto &translator: postgres_row_to_doc_translators) {
            translator(doc, result);
        }

        return {std::move(schema), std::move(doc)};
    }

    std::optional<std::vector<column_info>> merge_schemas(const std::vector<std::vector<column_info>>& schemas) {
        if (schemas.empty()) {
            return std::nullopt;
        }
        std::unordered_map<std::string, column_info> merged;
        for (const auto& schema : schemas) {
            for (const auto& column : schema) {
                merged.insert({column.name, column});
            }
        }
        std::vector<column_info> merged_vector;
        merged_vector.reserve(merged.size());
        for (const auto& [_, column] : merged) {
            merged_vector.push_back(column);
        }
        return merged_vector;
    }
} // namespace tsl
