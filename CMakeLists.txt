cmake_minimum_required(VERSION 3.22.1)
project(diplom)

set(CMAKE_CXX_STANDARD 20)

find_library(PQXX_LIB pqxx)
find_library(PQ_LIB pq)
find_package(libpqxx REQUIRED)
find_package(Boost REQUIRED)

include_directories(
        ${PostgreSQL_INCLUDE_DIR} # PostgreSQL headers (e.g., libpq-fe.h)
        ${LIBPQXX_INCLUDE_DIRS}    # libpqxx headers
        include                    # Your project's include directory
)

add_executable(diplom Connection.cpp
        include/Logger.h
        Logger.cpp
        include/LogicalReplicationHandler.h
        include/Connection.h
        example.cpp
        LogicalReplicationHandler.cpp
)

add_subdirectory(fmt)

target_link_libraries(diplom libpqxx::pqxx ${PQXX_LIB} ${PQ_LIB} fmt)
