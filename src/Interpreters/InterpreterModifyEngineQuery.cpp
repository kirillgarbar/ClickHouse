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
#include "Formats/FormatSettings.h"
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
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    /// Add default database to table identifiers that we can encounter in e.g. default expressions, mutation expression, etc.
    AddDefaultDatabaseVisitor visitor(getContext(), table_id.getDatabaseName());
    visitor.visit(query_ptr);

    auto alter_lock = table->lockForAlter(getContext()->getSettingsRef().lock_acquire_timeout);

    String name = query.getTable();
    String new_name = fmt::format("{0}_new", name);
    String old_name = fmt::format("{0}_old", name);
    String engine_name = storage.engine->name;

    WriteBufferFromOwnString buffer;
    IAST::FormatSettings format_settings{buffer, true};
    storage.format(format_settings);
    String storage_formatted = buffer.str();

    String query1 = fmt::format("CREATE TABLE {0} AS {1} {2}", new_name, name, storage_formatted);
    String query2 = fmt::format("SYSTEM STOP MERGES;");
    String query3 = fmt::format("ALTER TABLE {0} ATTACH PARTITION ID 'all' FROM {1};", new_name, name);
    String query4 = fmt::format("SYSTEM START MERGES;");
    String query5 = fmt::format("RENAME TABLE {0} TO {1};", name, old_name);
    String query6 = fmt::format("RENAME TABLE {0} TO {1};", new_name, name);
    auto query_context = Context::createCopy(context);
    
    executeQuery(query1, query_context, true);
    executeQuery(query2, query_context, true);
    executeQuery(query3, query_context, true);
    executeQuery(query4, query_context, true);
    executeQuery(query5, query_context, true);
    executeQuery(query6, query_context, true);

    return res;
}

}
