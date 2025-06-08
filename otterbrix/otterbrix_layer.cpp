#include <otterbrix/otterbrix_layer.h>
#include <logical_replication//logical_replication_handler.h>

#include <lock_free_queue/readerwriterqueue.h>
#include <integration/cpp/otterbrix.hpp>

using namespace moodycamel;

namespace layer {

    void create_otterbrix(const char *path, otterbrix::otterbrix_ptr &otterbrix_service) {
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
    }

    void consumer(const std::string &postgres_database,
                  std::vector<std::string> &tables_array,
                  const std::string &file_name,
                  otterbrix::otterbrix_ptr &otterbrix_service,
                  ReaderWriterQueue<result_node> &queue) {
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
                            log.log_to_console(log_level::INFO, doc->to_json().c_str());
                        }
                    }
                }
                continue;
            }

            result_node result_node;
            bool succeeded = queue.try_dequeue(result_node);
            if (succeeded) {
                if (!result_node.has_parameter) {
                    otterbrix_service->dispatcher()->execute_plan(otterbrix::session_id_t(), result_node.node);
                } else {
                    otterbrix_service->dispatcher()->execute_plan(otterbrix::session_id_t(), result_node.node, result_node.parameter);
                }
                counter++;
            }
        }
    }
}