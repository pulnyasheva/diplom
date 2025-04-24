#include <iostream>
#include <string>
#include <vector>
#include <exception>
#include <boost/program_options.hpp>

#include "logical_replication/logical_replication_handler.h"

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
    std::string conninfo;
    std::string database;
    std::string table;
    std::vector<std::string> tables;
    std::string logfile = "";
    std::string url_log = "";
    std::string user_snapshot = "";
    bool user_managed_slot = false;
    int batch_size = 100;

    po::options_description desc("Allowed options for Logical Replication Handler");
    desc.add_options()
        ("help,h", "Show help message")
        ("conninfo,c", po::value<std::string>(&conninfo),
            "PostgreSQL connection string (REQUIRED, e.g., postgresql://user:pass@host:port/db)")
        ("tables,t", po::value<std::vector<std::string>>(&tables),
            "List of tables to replicate (schema.table format recommended)")
        ("database,d", po::value<std::string>(&database),
            "Name of the table in PostgreSQL")
        ("table_name,tn", po::value<std::string>(&table),
            "Name of the table in PostgreSQL")
        ("logfile,l", po::value<std::string>(&logfile)->default_value(logfile),
            "Path to the log file (leave empty to disable file logging)")
        ("url_log", po::value<std::string>(&url_log)->default_value(url_log),
            "Url to the log (leave empty to disable file logging)")
        ("user_managed_slot", po::value<bool>(&user_managed_slot)->default_value(user_managed_slot),
            "User managed slot")
        ("user_snapshot", po::value<std::string>(&user_snapshot)->default_value(user_snapshot),
            "User snapshot name")
        ("batchsize,b", po::value<int>(&batch_size)->default_value(batch_size),
            "Batch size for processing changes");

    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
    } catch (const po::error& e) {
        std::cerr << "Error parsing arguments: " << e.what() << "\n\n";
        std::cerr << desc << "\n";
        return 1;
    }

    bool missing_arg = false;
    if (!vm.count("conninfo")) { std::cerr << "Error: --conninfo is required.\n"; missing_arg = true; }
    if (!vm.count("publication")) { std::cerr << "Error: --publication is required.\n"; missing_arg = true; }
    if (!vm.count("slot")) { std::cerr << "Error: --slot is required.\n"; missing_arg = true; }
    if (!vm.count("tables")) { std::cerr << "Error: --tables is required.\n"; missing_arg = true; }

    if (missing_arg) {
        std::cerr << "\n" << desc << "\n";
        return 1;
    }

    try {
        po::notify(vm);
    } catch (const std::exception& e) {
        std::cerr << "Error processing arguments: " << e.what() << "\n\n";
        std::cerr << desc << "\n";
        return 1;
    }


    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 0;
    }

    try {
        logical_replication_handler logical_replication_handler(
        database,
        table,
        conninfo,
        logfile,
        url_log,
        tables,
        100,
        user_managed_slot,
        user_snapshot);

        logical_replication_handler.start_synchronization();

        logical_replication_handler.run_consumer();
    } catch (const std::exception& e) {
        std::cerr << "An error occurred during replication: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
