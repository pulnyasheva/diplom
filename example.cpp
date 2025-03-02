#include <string>

#include <Logger.h>
#include <LogicalReplicationHandler.h>


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

    return 0;
}
