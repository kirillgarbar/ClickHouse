#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context.h>


namespace DB
{

class AccessRightsElements;
class ASTModifyEngineQuery;

/** Allows to change table's engine.
  * Creates a new table with desired engine,
  * exchanges it's name with the old table and 
  * attaches all parts from old table.
  * Currently implemented only between MergeTree and
  * it's replicated version.
  */
class InterpreterModifyEngineQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterModifyEngineQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    bool supportsTransactions() const override { return false; }

private:
    void checkEngineChangeIsPossible(StoragePtr table);

    AccessRightsElements getRequiredAccess() const;

    ASTPtr query_ptr;

    bool internal = false;
};

}
