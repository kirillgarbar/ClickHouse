#pragma once

#include <Interpreters/SquashingTransform.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/IInflatingTransform.h>
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

class SquashingChunksTransform : public ExceptionKeepingTransform
{
public:
    explicit SquashingChunksTransform(
        const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes);

    String getName() const override { return "SquashingTransform"; }

    void work() override;

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;
    void onFinish() override;

private:
    SquashingTransform squashing;
    Chunk cur_chunk;
    Chunk finish_chunk;
};

/// Doesn't care about propagating exceptions and thus doesn't throw LOGICAL_ERROR if the following transform closes its input port.
class SimpleSquashingChunksTransform : public IInflatingTransform
{
public:
    explicit SimpleSquashingChunksTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes);

    String getName() const override { return "SimpleSquashingTransform"; }

protected:
    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;
    Chunk getRemaining() override;

private:
    SquashingTransform squashing;
    Chunk squashed_chunk;
};

}
