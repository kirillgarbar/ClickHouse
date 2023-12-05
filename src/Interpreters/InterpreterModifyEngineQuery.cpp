#include <Interpreters/InterpreterModifyEngineQuery.h>

#include <Databases/IDatabase.h>
#include <Parsers/ASTModifyEngineQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/TableEngineModifier.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Access/Common/AccessType.h>

#include <fmt/core.h>


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

    if (query.cluster.empty())
    {
        auto table_id = getContext()->resolveStorageID(query, Context::ResolveOrdinary);
        query_ptr->as<ASTModifyEngineQuery &>().setDatabase(table_id.database_name);
        StoragePtr table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
        DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);

        getContext()->checkAccess(getRequiredAccess());

        if (!table)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Could not find table: {}", table_id.table_name);

        TableEngineModifier::setReadonly(table, true);

        try {
            if (auto table_merge_tree = dynamic_pointer_cast<StorageMergeTree>(table))
            {
                auto unfinished_mutations = table_merge_tree->getUnfinishedMutationCommands();
                if (!unfinished_mutations.empty())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Modify engine while there are unfinished mutations left is prohibited.");
            }

            /// Add default database to table identifiers that we can encounter in e.g. default expressions, mutation expression, etc.
            AddDefaultDatabaseVisitor visitor(getContext(), table_id.getDatabaseName());
            visitor.visit(query_ptr);

            //Entity names
            String database_name = (database) ? database->getDatabaseName() : "default";
            String table_name = query.getTable();
            //TODO: Make sure name is unique
            String table_name_temp = fmt::format("{0}_temp", table_name);

            auto query_context = Context::createCopy(context);

            DDLGuardPtr ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, table_name);
            DDLGuardPtr ddl_guard_temp = DatabaseCatalog::instance().getDDLGuard(database_name, table_name_temp);

            //Create table
            auto engine_modifier = std::make_unique<TableEngineModifier>(table_name, database_name);
            engine_modifier->createTable(query_ptr, query_context);

            //Rename tables
            engine_modifier->renameTable(query_context);

            //Attach partitions
            engine_modifier->attachAllPartitionsToTable(query_context);

            TableEngineModifier::setReadonly(table, false);
            
            //StoragePtr table_new = DatabaseCatalog::instance().tryGetTable(table_id, query_context);
            //table_new->startup();
            
            ddl_guard.reset();
            ddl_guard_temp.reset();
        } catch (...) {
            TableEngineModifier::setReadonly(table, false);
            throw;
        }
    } 
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Modify engine on cluster is not implemented yet.");
        //Doesn't work correctly if converting to replicated while tables on other hosts aren't empty
        //TODO: Attach only on one host?
        //return executeDDLQueryOnCluster(query_ptr, getContext());
    }

    return {};
}

AccessRightsElements InterpreterModifyEngineQuery::getRequiredAccess() const
{
    /// Internal queries (initiated by the server itself) always have access to everything.
    if (internal)
        return {};

    AccessRightsElements required_access;
    const auto & query = query_ptr->as<const ASTModifyEngineQuery &>();

    String temp_name = query.getTable() + "_temp";

    required_access.emplace_back(AccessType::SELECT | AccessType::DROP_TABLE, query.getDatabase(), query.getTable());
    required_access.emplace_back(AccessType::SELECT | AccessType::DROP_TABLE, query.getDatabase(), temp_name);
    required_access.emplace_back(AccessType::CREATE_TABLE | AccessType::INSERT, query.getDatabase(), query.getTable());
    required_access.emplace_back(AccessType::CREATE_TABLE | AccessType::INSERT, query.getDatabase(), temp_name);

    if (query.storage)
    {
        auto & storage = query.storage->as<ASTStorage &>();
        auto source_access_type = StorageFactory::instance().getSourceAccessType(storage.engine->name);
        if (source_access_type != AccessType::NONE)
            required_access.emplace_back(source_access_type);
    }

    return required_access;
}

}
