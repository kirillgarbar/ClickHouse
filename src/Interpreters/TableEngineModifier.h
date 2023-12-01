#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/SettingsEnums.h>
#include <Access/Common/AccessRightsElement.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>
#include "Parsers/IAST_fwd.h"


namespace DB
{

class ASTCreateQuery;
class ASTExpressionList;
class ASTConstraintDeclaration;
class ASTStorage;
class IDatabase;
class DDLGuard;
using DatabasePtr = std::shared_ptr<IDatabase>;
using DDLGuardPtr = std::unique_ptr<DDLGuard>;


/** Allows to create new table or database,
  *  or create an object for existing table or database.
  */
class TableEngineModifier
{
public:
    TableEngineModifier(String & table_name_, String & database_name_);

    void createTable(ASTPtr & modify_query_ptr, ContextMutablePtr context);
    void renameTable(ContextMutablePtr context);
    void attachAllPartitionsToTable(ContextMutablePtr query_context);

    static void setReadonly(StoragePtr table, bool value);

    void prepareOnClusterQuery(ASTCreateQuery & create, ContextPtr context, const String & cluster_name);
private:
    String table_name;
    String table_name_temp;
    String database_name;

    struct TableProperties
    {
        ColumnsDescription columns;
        IndicesDescription indices;
        ConstraintsDescription constraints;
        ProjectionsDescription projections;
    };

    /// List of columns and their types in AST.
    static ASTPtr formatColumns(const ColumnsDescription & columns);
    static ASTPtr formatIndices(const IndicesDescription & indices);
    static ASTPtr formatConstraints(const ConstraintsDescription & constraints);
    static ASTPtr formatProjections(const ProjectionsDescription & projections);

    /// Calculate list of columns, constraints, indices, etc... of table. Rewrite query in canonical way.
    TableProperties getTablePropertiesAndNormalizeCreateQuery(ASTCreateQuery & create, ContextMutablePtr context) const;

    /// Create IStorage and add it to database. If table already exists and IF NOT EXISTS specified, do nothing and return false.
    bool doCreateTable(ASTPtr & query_ptr, const TableProperties & properties, ContextMutablePtr context);

    void assertOrSetUUID(ASTCreateQuery & create, const DatabasePtr & database, ContextMutablePtr context) const;
};
}
