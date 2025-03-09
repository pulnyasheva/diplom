#include <iostream>
#include <string>

#include <Logger.h>
#include <LogicalReplicationHandler.h>

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
                "('Bob', 'Brown', 27, 'bob.brown2@example.com');");

        tx.exec("UPDATE students SET age = 50 WHERE email = 'bob.brown2@example.com';");

        tx.exec("UPDATE students SET age = 12 WHERE id = 2;");

        tx.exec("DELETE FROM students WHERE email = 'bob.brown2@example.com';");

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
    LogicalReplicationHandler replication_handler = LogicalReplicationHandler(
        "postgres",
        "students",
        conninfo,
        "log.txt",
        tables,
        100);

    replication_handler.startSynchronization();

    if (!exampleDatabaseQuery(conninfo)) {
        return 1;
    }

    std::cout << replication_handler.runConsumer() << std::endl;

    return 0;
}
