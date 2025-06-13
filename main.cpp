#include <string>
#include <vector>
#include <iostream>

#include <logical_replication/logical_replication_handler.h>
#include <otterbrix/otterbrix_layer.h>
#include <otterbrix/otterbrix_service.h>

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <integration/cpp/wrapper_dispatcher.hpp>

int main() {
    std::string conninfo = "postgresql://postgres:postgres@postgres:5432/postgres";
    std::string postgres_database = "postgres";
    std::string postgres_name = "example";
    std::vector<std::string> tables = {"public.example1", "public.example2"};
    std::string file_name = "";
    std::string url_log = "";
    const char* path = "/tmp/test_collection_sql/base";

    ReaderWriterQueue<result_node> queue(100);
    otterbrix::otterbrix_ptr otterbrix_service;
    layer::create_otterbrix(path, otterbrix_service);
    std::pmr::memory_resource* resource = resource = otterbrix_service->dispatcher()->resource();
    layer::create_database(postgres_database, tables, otterbrix_service);

    logical_replication_handler replication_handler = logical_replication_handler(postgres_database,
        postgres_name,
        conninfo,
        file_name,
        url_log,
        tables,
        queue,
        resource,
        100);

    replication_handler.start_synchronization();
    std::thread producer_thread(&logical_replication_handler::run_consumer, &replication_handler);

    layer::consumer(postgres_database, tables, file_name, otterbrix_service, queue);

    if (producer_thread.joinable()) {
        producer_thread.join();
    }
}
