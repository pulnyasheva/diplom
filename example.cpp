#include <iostream>
#include <string>

#include <common/logger.h>
#include <logical_replication/logical_replication_handler.h>

bool exampleDatabaseQuery(const std::string conninfo) {
    try {
        pqxx::connection C(conninfo);
        if (!C.is_open()) {
            std::cerr << "Can't open database" << std::endl;
            return false;
        }

        pqxx::work tx(C);

        tx.exec("INSERT INTO students (first_name, last_name, age, email) "
                "VALUES ('John', 'Doe', 21, 'john.doe2@example.com'), "
                "('Lui', 'Doe', 21, 'lui.doe2@example.com'), "
                "('Lui', 'Brown', 27, 'lui.brown2@example.com');");

        tx.exec("UPDATE students SET age = 50 WHERE email = 'john.doe@example.com';");

        pqxx::result R = tx.exec("SELECT * FROM students");
        for (size_t i = 0; i < R.columns(); ++i) {
            std::cout << R.column_name(i) << "\t";
        }
        std::cout << std::endl;

        for (const auto& row : R) {
            for (const auto& field : row) {
                std::cout << field.c_str() << "\t";
            }
            std::cout << std::endl;
        }

        tx.exec("UPDATE students SET id = 25 WHERE email = 'john.doe2@example.com';");

        tx.exec("DELETE FROM students WHERE first_name = 'Lui';");

        tx.exec("DELETE FROM students WHERE email = 'john.doe2@example.com';");

        tx.commit();
    } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
        return false;
    }

    return true;
}

int main() {
    const std::string conninfo = "postgresql://postgres:postgres@172.29.190.7:5432/postgres";

    std::vector<std::string> tables = {"students"};
    logical_replication_handler replication_handler = logical_replication_handler(
        "postgres",
        "students",
        conninfo,
        "log.txt",
        "",
        tables,
        100);

    replication_handler.start_synchronization();

    if (!exampleDatabaseQuery(conninfo)) {
        return 1;
    }

    std::cout << replication_handler.run_consumer() << std::endl;

    return 0;
}
