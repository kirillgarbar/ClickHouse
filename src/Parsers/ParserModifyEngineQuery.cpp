#include <Common/typeid_cast.h>
#include <Parsers/ParserModifyEngineQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTModifyEngineQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{

bool ParserEngine::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_partition_by("PARTITION BY");
    ParserKeyword s_primary_key("PRIMARY KEY");
    ParserKeyword s_order_by("ORDER BY");
    ParserKeyword s_sample_by("SAMPLE BY");
    ParserKeyword s_ttl("TTL");
    ParserKeyword s_settings("SETTINGS");

    ParserIdentifierWithOptionalParameters ident_with_optional_params_p;
    ParserExpression expression_p;
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);
    ParserTTLExpressionList parser_ttl_list;
    ParserStringLiteral string_literal_parser;

    ASTPtr engine;
    ASTPtr partition_by;
    ASTPtr primary_key;
    ASTPtr order_by;
    ASTPtr sample_by;
    ASTPtr ttl_table;
    ASTPtr settings;

    bool storage_like = false;

    if (!ident_with_optional_params_p.parse(pos, engine, expected))
        return false;
    storage_like = true;

    while (true)
    {
        if (!partition_by && s_partition_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, partition_by, expected))
            {
                storage_like = true;
                continue;
            }
            else
                return false;
        }

        if (!primary_key && s_primary_key.ignore(pos, expected))
        {
            if (expression_p.parse(pos, primary_key, expected))
            {
                storage_like = true;
                continue;
            }
            else
                return false;
        }

        if (!order_by && s_order_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, order_by, expected))
            {
                storage_like = true;
                continue;
            }
            else
                return false;
        }

        if (!sample_by && s_sample_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, sample_by, expected))
            {
                storage_like = true;
                continue;
            }
            else
                return false;
        }

        if (!ttl_table && s_ttl.ignore(pos, expected))
        {
            if (parser_ttl_list.parse(pos, ttl_table, expected))
            {
                storage_like = true;
                continue;
            }
            else
                return false;
        }

        if (s_settings.ignore(pos, expected))
        {
            if (!settings_p.parse(pos, settings, expected))
                return false;
            storage_like = true;
        }

        break;
    }

    // If any part of storage definition is found create storage node
    if (!storage_like)
        return false;
    
    engine->as<ASTFunction &>().kind = ASTFunction::Kind::TABLE_ENGINE;

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, engine);
    storage->set(storage->partition_by, partition_by);
    storage->set(storage->primary_key, primary_key);
    storage->set(storage->order_by, order_by);
    storage->set(storage->sample_by, sample_by);
    storage->set(storage->ttl_table, ttl_table);
    storage->set(storage->settings, settings);

    node = storage;
    return true;
}

bool ParserModifyEngineQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTModifyEngineQuery>();
    node = query;

    ParserKeyword s_alter_table("ALTER TABLE");

    if (!s_alter_table.ignore(pos, expected))
        return false;

    
    if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
        return false;

    String cluster_str;
    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }
    query->cluster = cluster_str;

    /////////////

    ParserKeyword s_modify_engine("MODIFY ENGINE");
    ParserEngine storage_p;

    if (s_modify_engine.ignore(pos, expected))
    {
        if (!storage_p.parse(pos, query->storage, expected))
            return false;
    }
    else
        return false;

    if (query->storage)
        query->children.push_back(query->storage);

    /////////////

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    return true;
}

}
