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
#include <Interpreters/InterpreterModifyEngineCreateQuery.h>
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

InterpreterModifyEngineCreateQuery::InterpreterModifyEngineCreateQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}

ConstraintsDescription InterpreterModifyEngineCreateQuery::getConstraintsDescription(const ASTExpressionList * constraints)
{
    ASTs constraints_data;
    if (constraints)
        for (const auto & constraint : constraints->children)
            constraints_data.push_back(constraint->clone());

    return ConstraintsDescription{constraints_data};
}

ASTPtr InterpreterModifyEngineCreateQuery::formatIndices(const IndicesDescription & indices)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & index : indices)
        res->children.push_back(index.definition_ast->clone());

    return res;
}

ASTPtr InterpreterModifyEngineCreateQuery::formatConstraints(const ConstraintsDescription & constraints)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & constraint : constraints.getConstraints())
        res->children.push_back(constraint->clone());

    return res;
}

ASTPtr InterpreterModifyEngineCreateQuery::formatProjections(const ProjectionsDescription & projections)
{
    auto res = std::make_shared<ASTExpressionList>();

    for (const auto & projection : projections)
        res->children.push_back(projection.definition_ast->clone());

    return res;
}

ASTPtr InterpreterModifyEngineCreateQuery::formatColumns(const NamesAndTypesList & columns)
{
    auto columns_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : columns)
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column.name;

        ParserDataType type_parser;
        String type_name = column.type->getName();
        const char * pos = type_name.data();
        const char * end = pos + type_name.size();
        column_declaration->type = parseQuery(type_parser, pos, end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        columns_list->children.emplace_back(column_declaration);
    }

    return columns_list;
}

ASTPtr InterpreterModifyEngineCreateQuery::formatColumns(const NamesAndTypesList & columns, const NamesAndAliases & alias_columns)
{
    std::shared_ptr<ASTExpressionList> columns_list = std::static_pointer_cast<ASTExpressionList>(formatColumns(columns));

    for (const auto & alias_column : alias_columns)
    {
        const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = alias_column.name;

        ParserDataType type_parser;
        String type_name = alias_column.type->getName();
        const char * type_pos = type_name.data();
        const char * type_end = type_pos + type_name.size();
        column_declaration->type = parseQuery(type_parser, type_pos, type_end, "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        column_declaration->default_specifier = "ALIAS";

        const auto & alias = alias_column.expression;
        const char * alias_pos = alias.data();
        const char * alias_end = alias_pos + alias.size();
        ParserExpression expression_parser;
        column_declaration->default_expression = parseQuery(expression_parser, alias_pos, alias_end, "expression", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        column_declaration->children.push_back(column_declaration->default_expression);

        columns_list->children.emplace_back(column_declaration);
    }

    return columns_list;
}

ASTPtr InterpreterModifyEngineCreateQuery::formatColumns(const ColumnsDescription & columns)
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


ColumnsDescription InterpreterModifyEngineCreateQuery::getColumnsDescription(
    const ASTExpressionList & columns_ast, ContextPtr context_, bool attach)
{
    /// First, deduce implicit types.

    /** all default_expressions as a single expression list,
     *  mixed with conversion-columns for each explicitly specified type */

    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    NamesAndTypesList column_names_and_types;
    bool make_columns_nullable = !attach && context_->getSettingsRef().data_type_default_nullable;

    for (const auto & ast : columns_ast.children)
    {
        const auto & col_decl = ast->as<ASTColumnDeclaration &>();

        if (col_decl.collation && !context_->getSettingsRef().compatibility_ignore_collation_in_create_table)
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot support collation, please set compatibility_ignore_collation_in_create_table=true");
        }

        DataTypePtr column_type = nullptr;
        if (col_decl.type)
        {
            column_type = DataTypeFactory::instance().get(col_decl.type);

            if (attach)
                setVersionToAggregateFunctions(column_type, true);

            if (col_decl.null_modifier)
            {
                if (column_type->isNullable())
                    throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Can't use [NOT] NULL modifier with Nullable type");
                if (*col_decl.null_modifier)
                    column_type = makeNullable(column_type);
            }
            else if (make_columns_nullable)
            {
                column_type = makeNullable(column_type);
            }
            else if (!hasNullable(column_type) &&
                     col_decl.default_specifier == "DEFAULT" &&
                     col_decl.default_expression &&
                     col_decl.default_expression->as<ASTLiteral>() &&
                     col_decl.default_expression->as<ASTLiteral>()->value.isNull())
            {
                if (column_type->lowCardinality())
                {
                    const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(column_type.get());
                    assert(low_cardinality_type);
                    column_type = std::make_shared<DataTypeLowCardinality>(makeNullable(low_cardinality_type->getDictionaryType()));
                }
                else
                    column_type = makeNullable(column_type);
            }

            column_names_and_types.emplace_back(col_decl.name, column_type);
        }
        else
        {
            /// we're creating dummy DataTypeUInt8 in order to prevent the NullPointerException in ExpressionActions
            column_names_and_types.emplace_back(col_decl.name, std::make_shared<DataTypeUInt8>());
        }

        /// add column to postprocessing if there is a default_expression specified
        if (col_decl.default_expression)
        {
            /** For columns with explicitly-specified type create two expressions:
              * 1. default_expression aliased as column name with _tmp suffix
              * 2. conversion of expression (1) to explicitly-specified type alias as column name
              */
            if (col_decl.type)
            {
                const auto & final_column_name = col_decl.name;
                const auto tmp_column_name = final_column_name + "_tmp_alter" + toString(randomSeed());
                const auto * data_type_ptr = column_names_and_types.back().type.get();

                default_expr_list->children.emplace_back(
                    setAlias(addTypeConversionToAST(std::make_shared<ASTIdentifier>(tmp_column_name), data_type_ptr->getName()),
                        final_column_name));

                default_expr_list->children.emplace_back(
                    setAlias(col_decl.default_expression->clone(), tmp_column_name));
            }
            else
                default_expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), col_decl.name));
        }
    }

    Block defaults_sample_block;
    /// set missing types and wrap default_expression's in a conversion-function if necessary
    if (!default_expr_list->children.empty())
        defaults_sample_block = validateColumnsDefaultsAndGetSampleBlock(default_expr_list, column_names_and_types, context_);

    bool sanity_check_compression_codecs = !attach && !context_->getSettingsRef().allow_suspicious_codecs;
    bool allow_experimental_codecs = attach || context_->getSettingsRef().allow_experimental_codecs;
    bool enable_deflate_qpl_codec = attach || context_->getSettingsRef().enable_deflate_qpl_codec;

    ColumnsDescription res;
    auto name_type_it = column_names_and_types.begin();
    for (const auto * ast_it = columns_ast.children.begin(); ast_it != columns_ast.children.end(); ++ast_it, ++name_type_it)
    {
        ColumnDescription column;

        auto & col_decl = (*ast_it)->as<ASTColumnDeclaration &>();

        column.name = col_decl.name;

        /// ignore or not other database extensions depending on compatibility settings
        if (col_decl.default_specifier == "AUTO_INCREMENT"
            && !context_->getSettingsRef().compatibility_ignore_auto_increment_in_create_table)
        {
            throw Exception(ErrorCodes::SYNTAX_ERROR,
                            "AUTO_INCREMENT is not supported. To ignore the keyword "
                            "in column declaration, set `compatibility_ignore_auto_increment_in_create_table` to true");
        }

        if (col_decl.default_expression)
        {
            if (context_->hasQueryContext() && context_->getQueryContext().get() == context_.get())
            {
                /// Normalize query only for original CREATE query, not on metadata loading.
                /// And for CREATE query we can pass local context, because result will not change after restart.
                NormalizeAndEvaluateConstantsVisitor::Data visitor_data{context_};
                NormalizeAndEvaluateConstantsVisitor visitor(visitor_data);
                visitor.visit(col_decl.default_expression);
            }

            ASTPtr default_expr = col_decl.default_expression->clone();

            if (col_decl.type)
                column.type = name_type_it->type;
            else
            {
                column.type = defaults_sample_block.getByName(column.name).type;
                /// set nullability for case of column declaration w/o type but with default expression
                if ((col_decl.null_modifier && *col_decl.null_modifier) || make_columns_nullable)
                    column.type = makeNullable(column.type);
            }

            column.default_desc.kind = columnDefaultKindFromString(col_decl.default_specifier);
            column.default_desc.expression = default_expr;
            column.default_desc.ephemeral_default = col_decl.ephemeral_default;
        }
        else if (col_decl.type)
            column.type = name_type_it->type;
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Neither default value expression nor type is provided for a column");

        if (col_decl.comment)
            column.comment = col_decl.comment->as<ASTLiteral &>().value.get<String>();

        if (col_decl.codec)
        {
            if (col_decl.default_specifier == "ALIAS")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot specify codec for column type ALIAS");
            column.codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                col_decl.codec, column.type, sanity_check_compression_codecs, allow_experimental_codecs, enable_deflate_qpl_codec);
        }

        if (col_decl.ttl)
            column.ttl = col_decl.ttl;

        res.add(std::move(column));
    }

    if (!attach && context_->getSettingsRef().flatten_nested)
        res.flattenNested();

    if (res.getAllPhysical().empty())
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED, "Cannot CREATE table without physical columns");

    return res;
}

InterpreterModifyEngineCreateQuery::TableProperties InterpreterModifyEngineCreateQuery::getTablePropertiesAndNormalizeCreateQuery(ASTCreateQuery & create) const
{
    /// Set the table engine if it was not specified explicitly.
    setEngine(create);

    /// We have to check access rights again (in case engine was changed).
    if (create.storage)
    {
        auto source_access_type = StorageFactory::instance().getSourceAccessType(create.storage->engine->name);
        if (source_access_type != AccessType::NONE)
            getContext()->checkAccess(source_access_type);
    }

    TableProperties properties;
    TableLockHolder as_storage_lock;

    if (create.columns_list)
    {
        if (create.as_table_function && (create.columns_list->indices || create.columns_list->constraints))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Indexes and constraints are not supported for table functions");

        /// Dictionaries have dictionary_attributes_list instead of columns_list
        assert(!create.is_dictionary);

        if (create.columns_list->columns)
        {
            properties.columns = getColumnsDescription(*create.columns_list->columns, getContext(), create.attach);
        }

        if (create.columns_list->indices)
            for (const auto & index : create.columns_list->indices->children)
            {
                IndexDescription index_desc = IndexDescription::getIndexFromAST(index->clone(), properties.columns, getContext());
                if (properties.indices.has(index_desc.name))
                    throw Exception(ErrorCodes::ILLEGAL_INDEX, "Duplicated index name {} is not allowed. Please use different index names.", backQuoteIfNeed(index_desc.name));
                const auto & settings = getContext()->getSettingsRef();
                if (index_desc.type == INVERTED_INDEX_NAME && !settings.allow_experimental_inverted_index)
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                            "Experimental Inverted Index feature is not enabled (the setting 'allow_experimental_inverted_index')");
                }
                if (index_desc.type == "annoy" && !settings.allow_experimental_annoy_index)
                    throw Exception(ErrorCodes::INCORRECT_QUERY, "Annoy index is disabled. Turn on allow_experimental_annoy_index");

                if (index_desc.type == "usearch" && !settings.allow_experimental_usearch_index)
                    throw Exception(ErrorCodes::INCORRECT_QUERY, "USearch index is disabled. Turn on allow_experimental_usearch_index");

                properties.indices.push_back(index_desc);
            }

        if (create.columns_list->projections)
            for (const auto & projection_ast : create.columns_list->projections->children)
            {
                auto projection = ProjectionDescription::getProjectionFromAST(projection_ast, properties.columns, getContext());
                properties.projections.add(std::move(projection));
            }

        properties.constraints = getConstraintsDescription(create.columns_list->constraints);
    }
    else if (!create.as_table.empty())
    {
        String as_database_name = getContext()->resolveDatabase(create.as_database);
        StoragePtr as_storage = DatabaseCatalog::instance().getTable({as_database_name, create.as_table}, getContext());

        /// as_storage->getColumns() and setEngine(...) must be called under structure lock of other_table for CREATE ... AS other_table.
        as_storage_lock = as_storage->lockForShare(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
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
    else if (create.select)
    {

        Block as_select_sample;

        if (getContext()->getSettingsRef().allow_experimental_analyzer)
        {
            as_select_sample = InterpreterSelectQueryAnalyzer::getSampleBlock(create.select->clone(), getContext());
        }
        else
        {
            as_select_sample = InterpreterSelectWithUnionQuery::getSampleBlock(create.select->clone(),
                getContext(),
                false /* is_subquery */,
                create.isParameterizedView());
        }

        properties.columns = ColumnsDescription(as_select_sample.getNamesAndTypesList());
    }
    else if (create.as_table_function)
    {
        /// Table function without columns list.
        auto table_function_ast = create.as_table_function->ptr();
        auto table_function = TableFunctionFactory::instance().get(table_function_ast, getContext());
        properties.columns = table_function->getActualTableStructure(getContext(), /*is_insert_query*/ true);
    }
    else if (create.is_dictionary)
    {
        if (!create.dictionary || !create.dictionary->source)
            return {};

        /// Evaluate expressions (like currentDatabase() or tcpPort()) in dictionary source definition.
        NormalizeAndEvaluateConstantsVisitor::Data visitor_data{getContext()};
        NormalizeAndEvaluateConstantsVisitor visitor(visitor_data);
        visitor.visit(create.dictionary->source->ptr());

        return {};
    }
    else if (!create.storage || !create.storage->engine)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected application state. CREATE query is missing either its storage or engine.");
    /// We can have queries like "CREATE TABLE <table> ENGINE=<engine>" if <engine>
    /// supports schema inference (will determine table structure in it's constructor).
    else if (!StorageFactory::instance().checkIfStorageSupportsSchemaInterface(create.storage->engine->name))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Incorrect CREATE query: required list of column descriptions or AS section or SELECT.");

    /// Even if query has list of columns, canonicalize it (unfold Nested columns).
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

void InterpreterModifyEngineCreateQuery::setEngine(ASTCreateQuery & create) const
{
    if (create.as_table_function)
        return;

    if (create.is_dictionary || create.is_ordinary_view || create.is_live_view || create.is_window_view)
        return;

    if (create.is_materialized_view && create.to_table_id)
        return;

    if (create.temporary)
    {
        /// Some part of storage definition is specified, but ENGINE is not: just set the one from default_temporary_table_engine setting.

        if (!create.cluster.empty())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Temporary tables cannot be created with ON CLUSTER clause");

        if (!create.storage)
        {
            auto storage_ast = std::make_shared<ASTStorage>();
            create.set(create.storage, storage_ast);
        }

        return;
    }

    if (!create.as_table.empty())
    {
        /// NOTE Getting the structure from the table specified in the AS is done not atomically with the creation of the table.

        String as_database_name = getContext()->resolveDatabase(create.as_database);
        String as_table_name = create.as_table;

        ASTPtr as_create_ptr = DatabaseCatalog::instance().getDatabase(as_database_name)->getCreateTableQuery(as_table_name, getContext());
        const auto & as_create = as_create_ptr->as<ASTCreateQuery &>();

        const String qualified_name = backQuoteIfNeed(as_database_name) + "." + backQuoteIfNeed(as_table_name);

        if (as_create.is_ordinary_view)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot CREATE a table AS {}, it is a View", qualified_name);

        if (as_create.is_live_view)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot CREATE a table AS {}, it is a Live View", qualified_name);

        if (as_create.is_window_view)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot CREATE a table AS {}, it is a Window View", qualified_name);

        if (as_create.is_dictionary)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot CREATE a table AS {}, it is a Dictionary", qualified_name);

        if (as_create.storage)
            create.set(create.storage, as_create.storage->ptr());
        else if (as_create.as_table_function)
            create.set(create.as_table_function, as_create.as_table_function->ptr());
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot set engine, it's a bug.");

        return;
    }

    create.set(create.storage, std::make_shared<ASTStorage>());
}

void InterpreterModifyEngineCreateQuery::assertOrSetUUID(ASTCreateQuery & create, const DatabasePtr & database) const
{
    const auto * kind = create.is_dictionary ? "Dictionary" : "Table";
    const auto * kind_upper = create.is_dictionary ? "DICTIONARY" : "TABLE";

    if (database->getEngineName() == "Replicated" && getContext()->getClientInfo().is_replicated_database_internal
        && !internal)
    {
        if (create.uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table UUID is not specified in DDL log");
    }

    bool from_path = create.attach_from_path.has_value();

    if (database->getUUID() != UUIDHelpers::Nil)
    {
        if (create.attach && !from_path && create.uuid == UUIDHelpers::Nil)
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "Incorrect ATTACH {} query for Atomic database engine. "
                            "Use one of the following queries instead:\n"
                            "1. ATTACH {} {};\n"
                            "2. CREATE {} {} <table definition>;\n"
                            "3. ATTACH {} {} FROM '/path/to/data/' <table definition>;\n"
                            "4. ATTACH {} {} UUID '<uuid>' <table definition>;",
                            kind_upper,
                            kind_upper, create.table,
                            kind_upper, create.table,
                            kind_upper, create.table,
                            kind_upper, create.table);
        }

        create.generateRandomUUID();
    }
    else
    {
        bool is_on_cluster = getContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
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


BlockIO InterpreterModifyEngineCreateQuery::createTable(ASTCreateQuery & create)
{
    String current_database = getContext()->getCurrentDatabase();
    auto database_name = create.database ? create.getDatabase() : current_database;

    DDLGuardPtr ddl_guard;

    if (!create.temporary && !create.database)
        create.setDatabase(current_database);
    if (create.to_table_id && create.to_table_id.database_name.empty())
        create.to_table_id.database_name = current_database;

    //DELETE
    if (create.columns_list)
    {
        AddDefaultDatabaseVisitor visitor(getContext(), current_database);
        visitor.visit(*create.columns_list);
    }

    /// Set and retrieve list of columns, indices and constraints. Set table engine if needed. Rewrite query in canonical way.
    TableProperties properties = getTablePropertiesAndNormalizeCreateQuery(create);

    DatabasePtr database;
    bool need_add_to_database = !create.temporary;
    if (need_add_to_database)
        database = DatabaseCatalog::instance().getDatabase(database_name);

    /// Actually creates table
    doCreateTable(create, properties, ddl_guard);
    ddl_guard.reset();

    /// If table has dependencies - add them to the graph
    QualifiedTableName qualified_name{database_name, create.getTable()};
    auto ref_dependencies = getDependenciesFromCreateQuery(getContext()->getGlobalContext(), qualified_name, query_ptr);
    auto loading_dependencies = getLoadingDependenciesFromCreateQuery(getContext()->getGlobalContext(), qualified_name, query_ptr);
    DatabaseCatalog::instance().addDependencies(qualified_name, ref_dependencies, loading_dependencies);

    return {};
}

bool InterpreterModifyEngineCreateQuery::doCreateTable(ASTCreateQuery & create,
                                           const InterpreterModifyEngineCreateQuery::TableProperties & properties,
                                           DDLGuardPtr & ddl_guard)
{

    if (!ddl_guard && likely(need_ddl_guard))
        ddl_guard = DatabaseCatalog::instance().getDDLGuard(create.getDatabase(), create.getTable());

    String data_path;
    DatabasePtr database;

    database = DatabaseCatalog::instance().getDatabase(create.getDatabase());
    assertOrSetUUID(create, database);

    String storage_name = "Table";
    auto storage_already_exists_error_code = ErrorCodes::TABLE_ALREADY_EXISTS;

    /// Table can be created before or it can be created concurrently in another thread, while we were waiting in DDLGuard.
    if (database->isTableExist(create.getTable(), getContext()))
    {
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS,
            "{} {}.{} already exists", storage_name, backQuoteIfNeed(create.getDatabase()), backQuoteIfNeed(create.getTable()));
    }
    else if (!create.attach)
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
    auto full_data_path = fs::path{getContext()->getPath()} / data_path;

    //DELETE
    if (!create.attach && !data_path.empty() && fs::exists(full_data_path))
    {
        if (getContext()->getZooKeeperMetadataTransaction() &&
            !getContext()->getZooKeeperMetadataTransaction()->isInitialQuery() &&
            !DatabaseCatalog::instance().hasUUIDMapping(create.uuid) &&
            Context::getGlobalContextInstance()->isServerCompletelyStarted() &&
            Context::getGlobalContextInstance()->getConfigRef().getBool("allow_moving_table_directory_to_trash", false))
        {
            /// This is a secondary query from a Replicated database. It cannot be retried with another UUID, we must execute it as is.
            /// We don't have a table with this UUID (and all metadata is loaded),
            /// so the existing directory probably contains some leftovers from previous unsuccessful attempts to create the table

            fs::path trash_path = fs::path{getContext()->getPath()} / "trash" / data_path / getHexUIntLowercase(thread_local_rng());
            LOG_WARNING(&Poco::Logger::get("InterpreterModifyEngineCreateQuery"), "Directory for {} data {} already exists. Will move it to {}",
                        Poco::toLower(storage_name), String(data_path), trash_path);
            fs::create_directories(trash_path.parent_path());
            renameNoReplace(full_data_path, trash_path);
        }
        else
        {
            throw Exception(storage_already_exists_error_code,
                "Directory for {} data {} already exists", Poco::toLower(storage_name), String(data_path));
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
        getContext(),
        getContext()->getGlobalContext(),
        properties.columns,
        properties.constraints,
        false);

    /// If schema wes inferred while storage creation, add columns description to create query.
    addColumnsDescriptionToCreateQueryIfNecessary(query_ptr->as<ASTCreateQuery &>(), res);

    database->createTable(getContext(), create.getTable(), res, query_ptr);

    /// We must call "startup" and "shutdown" while holding DDLGuard.
    /// Because otherwise method "shutdown" (from InterpreterDropQuery) can be called before startup
    /// (in case when table was created and instantly dropped before started up)
    ///
    /// Method "startup" may create background tasks and method "shutdown" will wait for them.
    /// But if "shutdown" is called before "startup", it will exit early, because there are no background tasks to wait.
    /// Then background task is created by "startup" method. And when destructor of a table object is called, background task is still active,
    /// and the task will use references to freed data.

    /// Also note that "startup" method is exception-safe. If exception is thrown from "startup",
    /// we can safely destroy the object without a call to "shutdown", because there is guarantee
    /// that no background threads/similar resources remain after exception from "startup".

    if (!res->supportsDynamicSubcolumns() && hasDynamicSubcolumns(res->getInMemoryMetadataPtr()->getColumns()))
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "Cannot create table with column of type Object, "
            "because storage {} doesn't support dynamic subcolumns",
            res->getName());
    }

    res->startup();
    return true;
}


BlockIO InterpreterModifyEngineCreateQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    auto & create = query_ptr->as<ASTCreateQuery &>();

    return createTable(create);
}

void InterpreterModifyEngineCreateQuery::addColumnsDescriptionToCreateQueryIfNecessary(ASTCreateQuery & create, const StoragePtr & storage)
{
    if (create.is_dictionary || (create.columns_list && create.columns_list->columns && !create.columns_list->columns->children.empty()))
        return;

    auto ast_storage = std::make_shared<ASTStorage>();
    unsigned max_parser_depth = static_cast<unsigned>(getContext()->getSettingsRef().max_parser_depth);
    auto query_from_storage = DB::getCreateQueryFromStorage(storage,
                                                            ast_storage,
                                                            false,
                                                            max_parser_depth,
                                                            true);
    auto & create_query_from_storage = query_from_storage->as<ASTCreateQuery &>();

    if (!create.columns_list)
    {
        ASTPtr columns_list = std::make_shared<ASTColumns>(*create_query_from_storage.columns_list);
        create.set(create.columns_list, columns_list);
    }
    else
    {
        ASTPtr columns = std::make_shared<ASTExpressionList>(*create_query_from_storage.columns_list->columns);
        create.columns_list->set(create.columns_list->columns, columns);
    }
}

}
