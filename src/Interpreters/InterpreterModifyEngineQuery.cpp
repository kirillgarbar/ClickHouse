#include <Interpreters/InterpreterModifyEngineQuery.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/TableEngineModifier.h>
#include <Parsers/ASTModifyEngineQuery.h>
#include "Access/Common/AuthenticationType.h"
#include "IO/ReadBufferFromString.h"
#include "IO/WriteBufferFromString.h"
#include "Parsers/ASTCreateQuery.h"
#include "Parsers/ParserRenameQuery.h"
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/queryToString.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Storages/IStorage.h>
#include "Common/Exception.h"
#include <Common/typeid_cast.h>

#include <fmt/core.h>

#include <memory>
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

    auto table_id = getContext()->resolveStorageID(query, Context::ResolveOrdinary);
    query_ptr->as<ASTModifyEngineQuery &>().setDatabase(table_id.database_name);
    StoragePtr table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);

    //TODO: Check access rights

    if (!table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Could not find table: {}", table_id.table_name);

    /// Add default database to table identifiers that we can encounter in e.g. default expressions, mutation expression, etc.
    AddDefaultDatabaseVisitor visitor(getContext(), table_id.getDatabaseName());
    visitor.visit(query_ptr);

    //Entity names
    String database_name = (database) ? database->getDatabaseName() : "default";
    String table_name = query.getTable();
    //TODO: Make sure name is unique
    String table_name_new = fmt::format("{0}_new", table_name);
    String table_name_old = fmt::format("{0}_old", table_name);

    auto query_context = Context::createCopy(context);

    DDLGuardPtr ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, table_name);
    DDLGuardPtr ddl_guard_new = DatabaseCatalog::instance().getDDLGuard(database_name, table_name_new);
    DDLGuardPtr ddl_guard_old = DatabaseCatalog::instance().getDDLGuard(database_name, table_name_old);

    //Create table
    String storage_string = queryToString(storage);
    String query1 = fmt::format("CREATE TABLE {0}.{1} AS {0}.{2} {3}", database_name, table_name_new, table_name, storage_string);
    ParserCreateQuery p_create_query;
    auto parsed_query = parseQuery(p_create_query, query1, "", 0, 0);
    auto engine_modifier = std::make_unique<TableEngineModifier>();
    engine_modifier->createTable(parsed_query, query_context);

    //Rename tables
    ParserRenameQuery p_rename_query;
    String rename_query_old = fmt::format("RENAME TABLE {0}.{1} TO {0}.{2};", database_name, table_name, table_name_old);
    String rename_query_new = fmt::format("RENAME TABLE {0}.{1} TO {0}.{2};", database_name, table_name_new, table_name);
    auto parsed_rename_query_old = parseQuery(p_rename_query, rename_query_old, "", 0, 0);
    auto parsed_rename_query_new = parseQuery(p_rename_query, rename_query_new, "", 0, 0);
    engine_modifier->renameTable(parsed_rename_query_old, query_context);
    engine_modifier->renameTable(parsed_rename_query_new, query_context);

    /// Transfer data
    String query2 = fmt::format("SYSTEM STOP MERGES;");
    executeQuery(query2, query_context, true);

    //Get partition ids
    String get_attach_queries_query = fmt::format("SELECT DISTINCT partition_id FROM system.parts WHERE table = '{0}' AND active;", table_name_old);
    WriteBufferFromOwnString buffer2;
    ReadBufferFromOwnString buffer3 {std::move(get_attach_queries_query)};
    auto select_query_context2 = Context::createCopy(context);
    select_query_context2->makeQueryContext();
    select_query_context2->setCurrentQueryId("");

    executeQuery(buffer3, buffer2, false, select_query_context2, {});

    std::stringstream partition_ids_string{buffer2.str()};
    std::string line;

    //Attach partitions
    while (std::getline(partition_ids_string, line, '\n'))
    {
        String query3 = fmt::format("ALTER TABLE {0}.{1} ATTACH PARTITION ID '{2}' FROM {0}.{3};", database_name, table_name, line, table_name_old);
        executeQuery(query3, query_context, true);
    }

    String query4 = fmt::format("SYSTEM START MERGES;");
    executeQuery(query4, query_context, true);

    ddl_guard.reset();
    ddl_guard_new.reset();
    ddl_guard_old.reset();

    return {};
}

}
