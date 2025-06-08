cmake_minimum_required(VERSION 3.22.1)
project(diplom)

set(CMAKE_CXX_STANDARD 20)

find_package(libpqxx REQUIRED)
find_package(Boost REQUIRED)
find_package(CURL REQUIRED)
find_package(otterbrix REQUIRED)

include_directories(include)

add_executable(diplom postgres/сonnection.cpp
        include/common/logger.h
        common/logger.cpp
        include/logical_replication/logical_replication_handler.h
        include/postgres/сonnection.h
        logical_replication/logical_replication_handler.cpp
        include/logical_replication/logical_replication_consumer.h
        logical_replication/logical_replication_consumer.cpp
        include/common/exception.h
        common/exception.cpp
        include/common/scheduler.h
        common/scheduler.cpp
        include/logical_replication/logical_replication_parser.h
        logical_replication/logical_replication_parser.cpp
        include/otterbrix/otterbrix_converter.h
        otterbrix/otterbrix_converter.cpp
        include/postgres/postgres_types.h
        include/postgres/postgres_types.h
        include/otterbrix/otterbrix_service.h
        otterbrix/otterbrix_service.cpp
        include/postgres/postgres_settings.h
        postgres/postgres_settings.cpp
        include/otterbrix/document_types.h
        otterbrix/otterbrix_layer.cpp
        include/lock_free_queue/readerwriterqueue.h
        include/lock_free_queue/atomicops.h
        main.cpp
        include/otterbrix/otterbrix_layer.h
)

target_link_libraries(diplom PUBLIC libpqxx::pqxx fmt::fmt otterbrix::otterbrix CURL::libcurl spdlog::spdlog)
