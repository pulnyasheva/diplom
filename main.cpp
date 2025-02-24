//#include <iostream>
//#include <string>
//#include <libpq-fe.h>
//
//class PostgresQuery {
//public:
//    PostgresQuery(const std::string &conninfo) {
//        conn = PQconnectdb(conninfo.c_str());
//        std::cout << "Conn info: " << conninfo << std::endl;
//        if (PQstatus(conn) != CONNECTION_OK) {
//            std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
//            PQfinish(conn);
//            exit(1);
//        }
//    }
//
//    ~PostgresQuery() {
//        PQfinish(conn);
//    }
//
//    // void createTable() {
//    //     const std::string createTableQuery =
//    //         "CREATE TABLE IF NOT EXISTS public.students ("
//    //         "id SERIAL PRIMARY KEY, "
//    //         "name VARCHAR(100), "
//    //         "age INT);";
//    //
//    //     PGresult *res = PQexec(conn, createTableQuery.c_str());
//    //     if (PQresultStatus(res) != PGRES_COMMAND_OK) {
//    //         std::cerr << "Create table failed: " << PQerrorMessage(conn) << std::endl;
//    //     } else {
//    //         std::cout << "Table created successfully." << std::endl;
//    //     }
//    //     PQclear(res);
//    // }
//    //
//    // void insertData(const std::string &name, int age) {
//    //     const std::string insertQuery =
//    //         "INSERT INTO public.students (name, age) VALUES ('" + name + "', " + std::to_string(age) + ");";
//    //
//    //     PGresult *res = PQexec(conn, insertQuery.c_str());
//    //     if (PQresultStatus(res) != PGRES_COMMAND_OK) {
//    //         std::cerr << "Insert data failed: " << PQerrorMessage(conn) << std::endl;
//    //     } else {
//    //         std::cout << "Data inserted successfully." << std::endl;
//    //     }
//    //     PQclear(res);
//    // }
//
//    void executeQuery(const std::string &query) {
//        PGresult *res = PQexec(conn, query.c_str());
//        if (PQresultStatus(res) != PGRES_TUPLES_OK && PQresultStatus(res) != PGRES_COMMAND_OK) {
//            std::cerr << "Query execution failed: " << PQerrorMessage(conn) << std::endl;
//            PQclear(res);
//            return;
//        }
//
//        int nrows = PQntuples(res);
//        for (int i = 0; i < nrows; i++) {
//            for (int j = 0; j < PQnfields(res); j++) {
//                std::cout << PQgetvalue(res, i, j) << "\t";
//            }
//            std::cout << std::endl;
//        }
//        PQclear(res);
//    }
//
//private:
//    PGconn *conn;
//};
//
//class PostgresReplication {
//public:
//    PostgresReplication(const std::string &conninfo) {
//        conn = PQconnectdb(conninfo.c_str());
//        if (PQstatus(conn) != CONNECTION_OK) {
//            std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
//            PQfinish(conn);
//            exit(1);
//        }
//    }
//
//    ~PostgresReplication() {
//        PQfinish(conn);
//    }
//
//    void createSubscription() {
//        const char *query = "CREATE SUBSCRIPTION my_subscription "
//                            "CONNECTION 'host=localhost dbname=postgres user=postgres password=postgres' "
//                            "PUBLICATION my_publication;";
//
//        PGresult *res = PQexec(conn, query);
//
//        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
//            std::cerr << "Error create subscription: " << PQerrorMessage(conn) << std::endl;
//            PQclear(res);
//            return;
//        }
//
//        std::cout << "Subscription create is ok" << std::endl;
//        PQclear(res);
//    }
//
//    void startReplication() {
//        const char *replicationCommand = "START_REPLICATION SLOT my_slot LOGICAL 0/0 (proto_version '1', publication 'my_publication')";
//
//        PGresult *res = PQexec(conn, replicationCommand);
//        if (PQresultStatus(res) != PGRES_COPY_BOTH) {
//            std::cerr << "Error starting replication: " << PQerrorMessage(conn) << std::endl;
//            PQclear(res);
//            return;
//        }
//
//        std::cout << "Replication started successfully." << std::endl;
//
//        while (true) {
//            char *buffer = nullptr;
//            int bytesRead = PQgetCopyData(conn, &buffer, 0);
//            if (bytesRead > 0) {
//                std::cout << "Received data: " << std::string(buffer, bytesRead) << std::endl;
//            } else if (bytesRead == 0) {
//                continue;
//            } else {
//                std::cerr << "Error reading data: " << PQerrorMessage(conn) << std::endl;
//                break;
//            }
//        }
//
//        PQclear(res);
//    }
//
//private:
//    PGconn *conn;
//};
//
//int main() {
//    const std::string conninfo = "host=localhost port=5432 dbname=postgres user=postgres password=postgres";
//
//    PostgresQuery query(conninfo);
//
//    query.executeQuery("SELECT * FROM public.students;");
//
//    PostgresReplication replication(conninfo);
//    replication.createSubscription();
//    replication.startReplication();
//
//    return 0;
//}


