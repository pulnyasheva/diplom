#include <string>
#include <iostream>

#include <Connection.h>
#include <Logger.h>

#include "LogicalReplicationHandler.h"


int main() {
    const std::string conninfo = "postgresql://postgres:postgres@172.29.190.7:5432/postgres";

    Logger logger("log.txt");
    postgres::Connection conn(conninfo, &logger, true);
    conn.connect();
    std::cout <<  conn.isConnected() << std::endl;

    pqxx::nontransaction tx(conn.getRef());
    std::cout << LogicalReplicationHandler::isPublicationExist(tx, "student_publication") << std::endl;

    LogicalReplicationHandler::createPublicationIfNeeded(tx, "student_publication", "students");

    std::cout << LogicalReplicationHandler::isPublicationExist(tx, "student_publication") << std::endl;

    return 0;
}
