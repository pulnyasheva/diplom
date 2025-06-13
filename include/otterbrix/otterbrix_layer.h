#pragma once

#include <components/logical_plan/node_insert.hpp>
#include <logical_replication//logical_replication_handler.h>
#include <otterbrix/otterbrix_service.h>

#include <lock_free_queue/readerwriterqueue.h>
#include <integration/cpp/otterbrix.hpp>

namespace layer {
    void create_otterbrix(const char* path, otterbrix::otterbrix_ptr &otterbrix_service);

    void create_database(const std::string &postgres_database, std::vector<std::string> &tables_array,
                         otterbrix::otterbrix_ptr &otterbrix_service);

    // void producer(logical_replication_handler &re);

    void consumer(const std::string &postgres_database,
                  std::vector<std::string> &tables_array,
                  const std::string &file_name,
                  otterbrix::otterbrix_ptr &otterbrix_service,
                  ReaderWriterQueue<result_node> &queue);
}
