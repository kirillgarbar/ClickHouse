#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTModifyEngineQuery.h>

namespace DB
{

/** Query like this:
  * ALTER TABLE [db.]name [ON CLUSTER cluster]
  *     [ADD COLUMN [IF NOT EXISTS] col_name type [AFTER col_after],]
  *     [DROP COLUMN [IF EXISTS] col_to_drop, ...]
  *     [CLEAR COLUMN [IF EXISTS] col_to_clear[ IN PARTITION partition],]
  *     [MODIFY COLUMN [IF EXISTS] col_to_modify type, ...]
  *     [RENAME COLUMN [IF EXISTS] col_name TO col_name]
  *     [MODIFY PRIMARY KEY (a, b, c...)]
  *     [MODIFY SETTING setting_name=setting_value, ...]
  *     [RESET SETTING setting_name, ...]
  *     [COMMENT COLUMN [IF EXISTS] col_name string]
  *     [MODIFY COMMENT string]
  *     [DROP|DETACH|ATTACH PARTITION|PART partition, ...]
  *     [FETCH PARTITION partition FROM ...]
  *     [FREEZE [PARTITION] [WITH NAME name]]
  *     [DELETE[ IN PARTITION partition] WHERE ...]
  *     [UPDATE col_name = expr, ...[ IN PARTITION partition] WHERE ...]
  *     [ADD INDEX [IF NOT EXISTS] index_name [AFTER index_name]]
  *     [DROP INDEX [IF EXISTS] index_name]
  *     [CLEAR INDEX [IF EXISTS] index_name IN PARTITION partition]
  *     [MATERIALIZE INDEX [IF EXISTS] index_name [IN PARTITION partition]]
  *     [MODIFY ENGINE] engine_name
  * ALTER LIVE VIEW [db.name]
  *     [REFRESH]
  */

class ParserModifyEngineQuery : public IParserBase
{
protected:
    const char * getName() const  override{ return "ALTER TABLE MODIFY ENGINE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
