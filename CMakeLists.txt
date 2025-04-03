cmake_minimum_required(VERSION 3.22.1)
project(diplom)

set(CMAKE_CXX_STANDARD 20)

find_library(PQXX_LIB pqxx)
find_library(PQ_LIB pq)
find_package(Boost REQUIRED)
find_package(CURL REQUIRED)
find_package(SOCI REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(Qt5 REQUIRED COMPONENTS Core Sql)

include_directories(${LIBPQXX_INCLUDE_DIRS} include ${SOCI_INCLUDE_DIRS} ${PostgreSQL_INCLUDE_DIRS})

add_executable(diplom сonnection.cpp
        include/Logger.h
        Logger.cpp
        include/logical_replication_handler.h
        include/сonnection.h
        example.cpp
        logical_replication_handler.cpp
        include/logical_replication_consumer.h
        logical_replication_consumer.cpp
        include/Exception.h
        Exception.cpp
        include/Scheduler.h
        Scheduler.cpp
        include/logical_replication_parser.h
        logical_replication_parser.cpp
        test.cpp
        include/otterbrix_converter.h
        otterbrix_converter.cpp
        include/postgres_types.h
        include/postgres_types.h
        include/otterbrix_service.h
        otterbrix_service.cpp
        include/postgres_settings.h
        postgres_settings.cpp
        include/document_types.h
)

add_subdirectory(fmt)

target_link_libraries(diplom ${PQXX_LIB} ${PQ_LIB} fmt soci_core soci_postgresql ${CURL_LIBRARIES} ${Boost_LIBRARIES} ${PostgreSQL_LIBRARIES} Qt5::Core Qt5::Sql)
