#include <otterbrix/otterbrix_layer.h>
#include <components/logical_plan/node_insert.hpp>
#include <logical_replication//logical_replication_handler.h>

#include <lock_free_queue/readerwriterqueue.h>
#include <integration/cpp/otterbrix.hpp>

using namespace moodycamel;

namespace layer {

    void create_otterbrix(const char *path, otterbrix::otterbrix_ptr &otterbrix_service) {
        // const char* path = "/tmp/test_collection_sql/base";
        auto config = configuration::config::default_config();
        config.log.path = path;
        config.log.level = log_t::level::warn;
        config.disk.path = path;
        config.wal.path = path;
        otterbrix_service = otterbrix::make_otterbrix(config);
    }

    void create_database(const std::string &postgres_database, std::vector<std::string> &tables_array,
                         otterbrix::otterbrix_ptr &otterbrix_service) {
        otterbrix_service->dispatcher()->create_database(otterbrix::session_id_t(), postgres_database);

        for (auto &table: tables_array) {
            otterbrix_service->dispatcher()->create_collection(otterbrix::session_id_t(), postgres_database, table);
        }
        std::cout << "success create database" << std::endl;
    }

    void consumer(const std::string &postgres_database,
                  std::vector<std::string> &tables_array,
                  const std::string &file_name,
                  otterbrix::otterbrix_ptr &otterbrix_service,
                  ReaderWriterQueue<result_node> &queue) {
        std::cout << "start consumer queue" << std::endl;

        //     auto doc = components::document::make_document(resource);
        //
        //     doc->set("/_id", gen_id(1, resource));
        //     doc->set<std::string>("/name", "Heruvimo");
        //
        //     auto insert_node = components::logical_plan::make_node_insert(resource,
        //                                                   {"postgres", "students"},
        //                                                   doc);
        //
        //     otterbrix_service->dispatcher()->execute_plan(otterbrix::session_id_t(), insert_node);
        // std::cout << "add node success" << std::endl;
        logger log = logger(file_name, "");
        int counter = 0;
        while (true) {
            if (counter == 10) {
                counter = 0;
                for (auto &table : tables_array) {
                    auto cursor_p = otterbrix_service->dispatcher()->execute_sql(
                    otterbrix::session_id_t(),
                    fmt::format("SELECT * FROM {}.\"{}\";", postgres_database, table)
                    );

                    for (const auto& buf : *cursor_p) {
                        for (const auto& doc : buf->data()) {
                            log.log_to_file(log_level::INFO, doc->to_json().c_str());
                        }
                    }
                }
                continue;
            }

            result_node result_node;
            bool succeeded = queue.try_dequeue(result_node);
            if (succeeded) {
                std::cout << "add node" << std::endl;
                if (!result_node.has_parameter) {
                    otterbrix_service->dispatcher()->execute_plan(otterbrix::session_id_t(), result_node.node);
                } else {
                    otterbrix_service->dispatcher()->execute_plan(otterbrix::session_id_t(), result_node.node, result_node.parameter);
                }
                std::cout << "add node success" << std::endl;
                counter++;
            }
        }
    }
}