// #include <soci/soci.h>
// #include <soci/postgresql/soci-postgresql.h>
// #include <iostream>
// #include <chrono>
// #include <QCoreApplication>
// #include <QSqlDatabase>
// #include <QSqlQuery>
// #include <QSqlError>
// #include <QDebug>
// #include <pqxx/pqxx>
//
// namespace pqxx {
//     class connection;
// }
//
// int main(int argc, char *argv[]) {
//     QCoreApplication a(argc, argv);
//
//     // Параметры подключения
//     const QString dbName = "postgres";      // Имя вашей базы данных
//     const QString user = "postgres";      // Имя пользователя
//     const QString password = "postgres"; // Пароль
//     const QString host = "172.29.190.7";    // Хост (обычно localhost)
//     const int port = 5432;               // Порт (стандартный порт PostgreSQL)
//
//     // QSqlDatabase db = QSqlDatabase::addDatabase("QPSQL", "ps");
//     // db.setHostName(host);
//     // db.setDatabaseName(dbName);
//     // db.setUserName(user);
//     // db.setPassword(password);
//     // db.setPort(port);
//
//     float sum_connection = 0, sum_query = 0;
//     for (int i = 0; i < 10000; i++) {
//         if (i % 100 == 0) {
//             std::cout << i << std::endl;
//         }
//         try {
//
//             auto start = std::chrono::high_resolution_clock::now();
//             pqxx::connection* c = new pqxx::connection("postgresql://postgres:postgres@172.29.190.7:5432/postgres");
//             auto end = std::chrono::high_resolution_clock::now();
//
//             pqxx::work W(*c);
//             auto start_query = std::chrono::high_resolution_clock::now();
//             // Выполняем запрос в рамках транзакции
//            // Разыменовываем указатель c
//             pqxx::result R = W.exec("SELECT 1");
//             W.commit();
//             auto end_query = std::chrono::high_resolution_clock::now();
//
//             // Закрываем соединение
//             delete c; // Освобождаем память
//
//
//             // // Замер времени подключения
//             // auto start = std::chrono::high_resolution_clock::now();
//             //
//             // db.open();
//             // // Замер времени завершения подключения
//             // auto end = std::chrono::high_resolution_clock::now();
//             //
//             // // Замер времени выполнения запроса
//             // auto start_query = std::chrono::high_resolution_clock::now();
//             //
//             // QSqlQuery query(db); // Передаем объект базы данных в конструктор запроса
//             // query.prepare("SELECT 1");
//             //
//             // query.exec();
//             //
//             // while (query.next()) {
//             //     // Обработка результатов запроса
//             //     QString value = query.value(0).toString(); // Замените индекс на нужный
//             //     // qDebug() << value;
//             // }
//             //
//             // auto end_query = std::chrono::high_resolution_clock::now();
//
//             // auto start = std::chrono::high_resolution_clock::now();
//             // // Подключение к базе данных
//             // soci::session sql(soci::postgresql, "dbname=postgres user=postgres password=postgres host=172.29.190.7 port=5432");
//             //
//             // // Замер времени подключения
//             // auto end = std::chrono::high_resolution_clock::now();
//             //
//             // auto start_query = std::chrono::high_resolution_clock::now();
//             // // Пример выполнения запроса
//             // std::string query = "SELECT 1";
//             // soci::rowset<soci::row> rs = (sql.prepare << query);
//             //
//             // // Замер времени выполнения запроса
//             // auto end_query = std::chrono::high_resolution_clock::now();
//             auto query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_query - start_query).count();
//             auto connect_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
//             sum_connection += connect_duration;
//             sum_query += query_duration;
//         } catch (const std::exception &e) {
//             std::cerr << "Error: " << e.what() << std::endl;
//         }
//     }
//
//     std::cout << sum_connection / 1000 << std::endl;
//     std::cout << sum_query / 1000 << std::endl;
//
//     return 0;
// }
