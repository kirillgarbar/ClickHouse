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
#include <Parsers/ASTModifyEngineQuery.h>
#include <Parsers/ASTAssignment.h>
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
    const auto & alter = query_ptr->as<ASTModifyEngineQuery &>();

    BlockIO res;

    if (!UserDefinedSQLFunctionFactory::instance().empty())
        UserDefinedSQLFunctionVisitor::visit(query_ptr);

    auto table_id = getContext()->resolveStorageID(alter, Context::ResolveOrdinary);
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

    return res;
}

}
