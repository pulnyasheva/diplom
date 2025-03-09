cmake_minimum_required(VERSION 3.22.1)
project(diplom)

set(CMAKE_CXX_STANDARD 20)

find_library(PQXX_LIB pqxx)
find_library(PQ_LIB pq)
find_package(Boost REQUIRED)

include_directories(${LIBPQXX_INCLUDE_DIRS} include)

add_executable(diplom Connection.cpp
        include/Logger.h
        Logger.cpp
        include/LogicalReplicationHandler.h
        include/Connection.h
        example.cpp
        LogicalReplicationHandler.cpp
        include/LogicalReplicationConsumer.h
        LogicalReplicationConsumer.cpp
        include/Exception.h
        Exception.cpp
        include/Scheduler.h
        Scheduler.cpp
        include/LogicalReplicationParser.h
        LogicalReplicationParser.cpp
)

add_subdirectory(fmt)

target_link_libraries(diplom ${PQXX_LIB} ${PQ_LIB} fmt)
