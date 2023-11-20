#include <memory>

#include <filesystem>

#include "Common/Exception.h"
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/Macros.h>
#include <Common/randomSeed.h>
#include <Common/atomicRename.h>
#include <Common/logger_useful.h>
#include "Parsers/IAST_fwd.h"
#include <base/hex.h>

#include <Core/Defines.h>
#include <Core/SettingsEnums.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/WindowView/StorageWindowView.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/BlockNumberColumn.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TableEngineModifier.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/GinFilter.h>

#include <Access/Common/AccessRightsElement.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/hasNullable.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/TablesLoader.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/NormalizeAndEvaluateConstantsVisitor.h>

#include <Compression/CompressionFactory.h>

#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <DataTypes/DataTypeFixedString.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int DICTIONARY_ALREADY_EXISTS;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int DUPLICATE_COLUMN;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int BAD_ARGUMENTS;
    extern const int BAD_DATABASE_FOR_TEMPORARY_TABLE;
    extern const int SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY;
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_INDEX;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_DATABASE;
    extern const int PATH_ACCESS_DENIED;
    extern const int NOT_IMPLEMENTED;
    extern const int ENGINE_REQUIRED;
    extern const int UNKNOWN_STORAGE;
    extern const int SYNTAX_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace fs = std::filesystem;

ASTPtr TableEngineModifier::formatIndices(const IndicesDescription & indices)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & index : indices)
        res->children.push_back(index.definition_ast->clone());

    return res;
}

ASTPtr TableEngineModifier::formatConstraints(const ConstraintsDescription & constraints)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & constraint : constraints.getConstraints())
        res->children.push_back(constraint->clone());

    return res;
}

ASTPtr TableEngineModifier::formatProjections(const ProjectionsDescription & projections)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & projection : projections)
        res->children.push_back(projection.definition_ast->clone());

    return res;
}

ASTPtr TableEngineModifier::formatColumns(const ColumnsDescription & columns)
{
    auto columns_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : columns)
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        ASTPtr column_declaration_ptr{column_declaration};

        column_declaration->name = column.name;

        ParserDataType type_parser;
        String type_name = column.type->getName();
        const char * type_name_pos = type_name.data();
        const char * type_name_end = type_name_pos + type_name.size();
        column_declaration->type = parseQuery(type_parser, type_name_pos, type_name_end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        if (column.default_desc.expression)
        {
            column_declaration->default_specifier = toString(column.default_desc.kind);
            column_declaration->default_expression = column.default_desc.expression->clone();
            column_declaration->children.push_back(column_declaration->default_expression);
        }

        column_declaration->ephemeral_default = column.default_desc.ephemeral_default;

        if (!column.comment.empty())
        {
            column_declaration->comment = std::make_shared<ASTLiteral>(Field(column.comment));
            column_declaration->children.push_back(column_declaration->comment);
        }

        if (column.codec)
        {
            column_declaration->codec = column.codec;
            column_declaration->children.push_back(column_declaration->codec);
        }

        if (column.ttl)
        {
            column_declaration->ttl = column.ttl;
            column_declaration->children.push_back(column_declaration->ttl);
        }

        columns_list->children.push_back(column_declaration_ptr);
    }

    return columns_list;
}

TableEngineModifier::TableProperties TableEngineModifier::getTablePropertiesAndNormalizeCreateQuery(ASTCreateQuery & create, ContextMutablePtr context) const
{
    /// We have to check access rights again (in case engine was changed).
    if (create.storage)
    {
        auto source_access_type = StorageFactory::instance().getSourceAccessType(create.storage->engine->name);
        if (source_access_type != AccessType::NONE)
            context->checkAccess(source_access_type);
    }

    TableProperties properties;
    TableLockHolder as_storage_lock;

    if (!create.as_table.empty())
    {
        String as_database_name = context->resolveDatabase(create.as_database);
        StoragePtr as_storage = DatabaseCatalog::instance().getTable({as_database_name, create.as_table}, context);

        /// as_storage->getColumns() and setEngine(...) must be called under structure lock of other_table for CREATE ... AS other_table.
        as_storage_lock = as_storage->lockForShare(context->getCurrentQueryId(), context->getSettingsRef().lock_acquire_timeout);
        auto as_storage_metadata = as_storage->getInMemoryMetadataPtr();
        properties.columns = as_storage_metadata->getColumns();

        /// Secondary indices and projections make sense only for MergeTree family of storage engines.
        /// We should not copy them for other storages.
        if (create.storage && endsWith(create.storage->engine->name, "MergeTree"))
        {
            properties.indices = as_storage_metadata->getSecondaryIndices();
            properties.projections = as_storage_metadata->getProjections().clone();
        }
        else
        {
            /// Only MergeTree support TTL
            properties.columns.resetColumnTTLs();
        }

        properties.constraints = as_storage_metadata->getConstraints();
    }
    else
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Only \"AS table\" queries should reach this code.");

    if (!create.columns_list)
        create.set(create.columns_list, std::make_shared<ASTColumns>());

    ASTPtr new_columns = formatColumns(properties.columns);
    ASTPtr new_indices = formatIndices(properties.indices);
    ASTPtr new_constraints = formatConstraints(properties.constraints);
    ASTPtr new_projections = formatProjections(properties.projections);

    create.columns_list->setOrReplace(create.columns_list->columns, new_columns);
    create.columns_list->setOrReplace(create.columns_list->indices, new_indices);
    create.columns_list->setOrReplace(create.columns_list->constraints, new_constraints);
    create.columns_list->setOrReplace(create.columns_list->projections, new_projections);

    assert(as_database_saved.empty() && as_table_saved.empty());
    std::swap(create.as_database, as_database_saved);
    std::swap(create.as_table, as_table_saved);

    return properties;
}

void TableEngineModifier::assertOrSetUUID(ASTCreateQuery & create, const DatabasePtr & database, ContextMutablePtr context) const
{
    const auto * kind = "Table";

    if (database->getEngineName() == "Replicated" && context->getClientInfo().is_replicated_database_internal
        && !internal)
    {
        if (create.uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table UUID is not specified in DDL log");
    }

    if (database->getUUID() != UUIDHelpers::Nil)
    {
        create.generateRandomUUID();
    }
    else
    {
        bool is_on_cluster = context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
        bool has_uuid = create.uuid != UUIDHelpers::Nil || create.to_inner_uuid != UUIDHelpers::Nil;
        if (has_uuid && !is_on_cluster && !internal)
        {
            /// We don't show the following error message either
            /// 1) if it's a secondary query (an initiator of a CREATE TABLE ON CLUSTER query
            /// doesn't know the exact database engines on replicas and generates an UUID, and then the replicas are free to ignore that UUID); or
            /// 2) if it's an internal query (for example RESTORE uses internal queries to create tables and it generates an UUID
            /// before creating a table to be possibly ignored if the database engine doesn't need it).
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "{} UUID specified, but engine of database {} is not Atomic", kind, create.getDatabase());
        }

        /// The database doesn't support UUID so we'll ignore it. The UUID could be set here because of either
        /// a) the initiator of `ON CLUSTER` query generated it to ensure the same UUIDs are used on different hosts; or
        /// b) `RESTORE from backup` query generated it to ensure the same UUIDs are used on different hosts.
        create.uuid = UUIDHelpers::Nil;
        create.to_inner_uuid = UUIDHelpers::Nil;
    }
}

bool TableEngineModifier::doCreateTable(ASTPtr & query_ptr,
                                           const TableEngineModifier::TableProperties & properties,
                                           ContextMutablePtr context)
{

    auto & create = query_ptr->as<ASTCreateQuery &>();

    String data_path;
    DatabasePtr database;

    database = DatabaseCatalog::instance().getDatabase(create.getDatabase());
    assertOrSetUUID(create, database, context);

    /// DELETE?
    /// Table can be created before or it can be created concurrently in another thread, while we were waiting in DDLGuard.
    if (database->isTableExist(create.getTable(), context))
    {
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS,
            "{} {}.{} already exists", "Table", backQuoteIfNeed(create.getDatabase()), backQuoteIfNeed(create.getTable()));
    }
    else
    {
        /// Checking that table may exists in detached/detached permanently state
        try
        {
            database->checkMetadataFilenameAvailability(create.getTable());
        }
        catch (const Exception &)
        {
            throw;
        }
    }

    data_path = database->getTableDataPath(create);
    auto full_data_path = fs::path{context->getPath()} / data_path;

    //DELETE?
    if (!data_path.empty() && fs::exists(full_data_path))
    {
        if (context->getZooKeeperMetadataTransaction() &&
            !context->getZooKeeperMetadataTransaction()->isInitialQuery() &&
            !DatabaseCatalog::instance().hasUUIDMapping(create.uuid) &&
            Context::getGlobalContextInstance()->isServerCompletelyStarted() &&
            Context::getGlobalContextInstance()->getConfigRef().getBool("allow_moving_table_directory_to_trash", false))
        {
            /// This is a secondary query from a Replicated database. It cannot be retried with another UUID, we must execute it as is.
            /// We don't have a table with this UUID (and all metadata is loaded),
            /// so the existing directory probably contains some leftovers from previous unsuccessful attempts to create the table

            fs::path trash_path = fs::path{context->getPath()} / "trash" / data_path / getHexUIntLowercase(thread_local_rng());
            LOG_WARNING(&Poco::Logger::get("TableEngineModifier"), "Directory for {} data {} already exists. Will move it to {}",
                        "table", String(data_path), trash_path);
            fs::create_directories(trash_path.parent_path());
            renameNoReplace(full_data_path, trash_path);
        }
        else
        {
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS,
                "Directory for {} data {} already exists", "table", String(data_path));
        }
    }

    /// We should lock UUID on CREATE query (because for ATTACH it must be already locked previously).
    /// But ATTACH without create.attach_short_syntax flag works like CREATE actually, that's why we check it.
    bool need_lock_uuid = !create.attach_short_syntax;
    TemporaryLockForUUIDDirectory uuid_lock;
    if (need_lock_uuid)
        uuid_lock = TemporaryLockForUUIDDirectory{create.uuid};
    else if (create.uuid != UUIDHelpers::Nil && !DatabaseCatalog::instance().hasUUIDMapping(create.uuid))
    {
        /// FIXME MaterializedPostgreSQL works with UUIDs incorrectly and breaks invariants
        if (database->getEngineName() != "MaterializedPostgreSQL")
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find UUID mapping for {}, it's a bug", create.uuid);
    }

    StoragePtr res;
    res = StorageFactory::instance().get(create,
        data_path,
        context,
        context->getGlobalContext(),
        properties.columns,
        properties.constraints,
        false);

    database->createTable(context, create.getTable(), res, query_ptr);

    res->startup();
    return true;
}


void TableEngineModifier::createTable(ASTPtr & query_ptr, ContextMutablePtr context)
{
    auto & create = query_ptr->as<ASTCreateQuery &>();

    String current_database = context->getCurrentDatabase();
    auto database_name = create.database ? create.getDatabase() : current_database;
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(database_name);

    if (!create.temporary && !create.database)
        create.setDatabase(current_database);

    /// Set and retrieve list of columns, indices and constraints. Set table engine if needed. Rewrite query in canonical way.
    TableProperties properties = getTablePropertiesAndNormalizeCreateQuery(create, context);

    /// Actually creates table
    doCreateTable(query_ptr, properties, context);

    /// If table has dependencies - add them to the graph
    QualifiedTableName qualified_name{database_name, create.getTable()};
    auto ref_dependencies = getDependenciesFromCreateQuery(context->getGlobalContext(), qualified_name, query_ptr);
    auto loading_dependencies = getLoadingDependenciesFromCreateQuery(context->getGlobalContext(), qualified_name, query_ptr);
    DatabaseCatalog::instance().addDependencies(qualified_name, ref_dependencies, loading_dependencies);
}

void TableEngineModifier::renameTable(ASTPtr & query_ptr, ContextMutablePtr context)
{
    const auto & rename = query_ptr->as<const ASTRenameQuery &>();

    String path = context->getPath();
    String current_database = context->getCurrentDatabase();

    /** In case of error while renaming, it is possible that only part of tables was renamed
      *  or we will be in inconsistent state. (It is worth to be fixed.)
      */

    RenameDescriptions descriptions;
    descriptions.reserve(rename.elements.size());

    for (const auto & elem : rename.elements)
    {
        descriptions.emplace_back(elem, current_database);
    }

    auto & database_catalog = DatabaseCatalog::instance();

    assert(!rename.rename_if_cannot_exchange || descriptions.size() == 1);
    assert(!(rename.rename_if_cannot_exchange && rename.exchange));

    for (const auto & elem : descriptions)
    {
        if (elem.if_exists)
        {
            assert(!rename.exchange);
            if (!database_catalog.isTableExist(StorageID(elem.from_database_name, elem.from_table_name), context))
                continue;
        }

        database_catalog.assertTableDoesntExist(StorageID(elem.to_database_name, elem.to_table_name), context);

        DatabasePtr database = database_catalog.getDatabase(elem.from_database_name);
        
        StorageID from_table_id{elem.from_database_name, elem.from_table_name};
        StorageID to_table_id{elem.to_database_name, elem.to_table_name};
        std::vector<StorageID> ref_dependencies;
        std::vector<StorageID> loading_dependencies;

        bool check_ref_deps = context->getSettingsRef().check_referential_table_dependencies;
        bool check_loading_deps = !check_ref_deps && context->getSettingsRef().check_table_dependencies;
        std::tie(ref_dependencies, loading_dependencies) = database_catalog.removeDependencies(from_table_id, check_ref_deps, check_loading_deps);

        try
        {
            database->renameTable(
                context,
                elem.from_table_name,
                *database_catalog.getDatabase(elem.to_database_name),
                elem.to_table_name,
                false,
                rename.dictionary);

            DatabaseCatalog::instance().addDependencies(to_table_id, ref_dependencies, loading_dependencies);
        }
        catch (...)
        {
            /// Restore dependencies if RENAME fails
            DatabaseCatalog::instance().addDependencies(from_table_id, ref_dependencies, loading_dependencies);
            throw;
        }
    }
}
}
