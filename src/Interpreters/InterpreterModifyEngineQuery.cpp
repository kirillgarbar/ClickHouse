#include <Interpreters/InterpreterModifyEngineQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/MutationsNonDeterministicHelpers.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTModifyEngineQuery.h>
#include <Parsers/ASTAssignment.h>
#include "Core/Field.h"
#include "Formats/FormatSettings.h"
#include "IO/ReadBufferFromString.h"
#include "IO/WriteBufferFromString.h"
#include "Parsers/ASTCreateQuery.h"
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/queryToString.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/MutationCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/StorageKeeperMap.h>
#include <Common/typeid_cast.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>

#include <boost/range/algorithm_ext/push_back.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <sstream>
#include <string>

#include <chrono>
#include <thread>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
    extern const int TABLE_IS_READ_ONLY;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
}


InterpreterModifyEngineQuery::InterpreterModifyEngineQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterModifyEngineQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    const auto & query = query_ptr->as<ASTModifyEngineQuery &>();
    const auto & storage = query.storage->as<ASTStorage &>();

    BlockIO res;

    if (!UserDefinedSQLFunctionFactory::instance().empty())
        UserDefinedSQLFunctionVisitor::visit(query_ptr);

    auto table_id = getContext()->resolveStorageID(query, Context::ResolveOrdinary);
    query_ptr->as<ASTModifyEngineQuery &>().setDatabase(table_id.database_name);
    StoragePtr table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());

    //getContext()->checkAccess(getRequiredAccess());

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);

    if (!table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Could not find table: {}", table_id.table_name);

    checkStorageSupportsTransactionsIfNeeded(table, getContext());
    if (table->isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is read-only");
    auto table_lock = table->lockForShare(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);

    /// Add default database to table identifiers that we can encounter in e.g. default expressions, mutation expression, etc.
    AddDefaultDatabaseVisitor visitor(getContext(), table_id.getDatabaseName());
    visitor.visit(query_ptr);

    String name = query.getTable();
    String new_name = fmt::format("{0}_new", name);
    String old_name = fmt::format("{0}_old", name);
    String engine_name = storage.engine->name;

    WriteBufferFromOwnString buffer;
    IAST::FormatSettings format_settings{buffer, true};
    storage.format(format_settings);
    String storage_formatted = buffer.str();

    //Start queries
    auto query_context = Context::createCopy(context);

    String query1;
    if (query.cluster.empty())
        query1 = fmt::format("CREATE TABLE {0} AS {1} {2}", new_name, name, storage_formatted);
    else
        query1 = fmt::format("CREATE TABLE {0} ON CLUSTER '{1}' AS {2} {3}", new_name, query.cluster, name, storage_formatted);

    executeQuery(query1, query_context, true);

    //Wait until table is created
    auto select_query_context1 = Context::createCopy(context);
    select_query_context1->makeQueryContext();
    select_query_context1->setCurrentQueryId("");


    std::chrono::milliseconds sleep_time_ms(50);
    while (true)
    {
        String check_table_exists_query = fmt::format("SELECT name FROM system.tables WHERE name = '{0}';", new_name);
        ReadBufferFromOwnString buffer_read(std::move(check_table_exists_query));
        WriteBufferFromOwnString buffer_write;
        executeQuery(buffer_read, buffer_write, false, select_query_context1, {});
        if (!buffer_write.str().empty()) break;
        std::this_thread::sleep_for(sleep_time_ms);
    }

    //Stop merges
    String query2 = fmt::format("SYSTEM STOP MERGES;");
    executeQuery(query2, query_context, true);String check_table_exists_query = fmt::format("SELECT name FROM system.tables WHERE name = '{0}';", new_name);

    //Get partition ids
    String get_attach_queries_query = fmt::format("SELECT DISTINCT partition_id FROM system.parts WHERE table = '{0}' AND active;", name);
    WriteBufferFromOwnString buffer2;
    ReadBufferFromOwnString buffer3 {std::move(get_attach_queries_query)};
    auto select_query_context2 = Context::createCopy(context);
    select_query_context2->makeQueryContext();
    select_query_context2->setCurrentQueryId("");

    executeQuery(buffer3, buffer2, false, select_query_context2, {});

    std::stringstream partition_ids_string{buffer2.str()};
    std::string line;

    while (std::getline(partition_ids_string, line, '\n'))
    {
        String query3 = fmt::format("ALTER TABLE {0} ATTACH PARTITION ID '{1}' FROM {2};", new_name, line, name);
        executeQuery(query3, query_context, true);
    }

    //Execute
    String query4 = fmt::format("SYSTEM START MERGES;");

    //RENAME ON CLUSTER???
    
    String query5 = fmt::format("RENAME TABLE {0} TO {1};", name, old_name);
    String query6 = fmt::format("RENAME TABLE {0} TO {1};", new_name, name);
    
    executeQuery(query4, query_context, true);
    executeQuery(query5, query_context, true);
    executeQuery(query6, query_context, true);

    return res;
}

}
