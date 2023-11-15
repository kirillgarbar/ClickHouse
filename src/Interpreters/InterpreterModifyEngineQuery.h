#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class AccessRightsElements;
class ASTModifyEngoneQuery;


class InterpreterModifyEngineQuery : public IInterpreter, WithContext
{
public:
    InterpreterModifyEngineQuery(const ASTPtr & query_ptr_, ContextPtr context_);

    BlockIO execute() override;

    bool supportsTransactions() const override { return true; }

private:
    ASTPtr query_ptr;
};

}
