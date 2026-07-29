/*
 * Unit tests for InnerJoinOperator.h
 * Covers: InnerJoinOperator, SemiAntiJoinOperator, LeftOuterJoinOperator,
 *         RightOuterJoinOperator, FullOuterJoinOperator
 *
 * Focus: constructor branches (key vs no-key, isAntiJoin).
 * Note: join() cannot be called without open() (segfaults due to uninitialized
 * SVE buffers and internal state). SemiAntiJoinOperator::join() is empty {}.
 */
#include <gtest/gtest.h>
#include "table/runtime/operators/join/window/WindowJoinOperator.h"
#include "table/runtime/operators/join/window/InnerJoinOperator.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "test/core/operators/OutputTest.h"
#include "core/typeutils/LongSerializer.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"

using namespace omnistream;

// ============================================================================
// JSON configuration for tests — minimal equi-join config
// ============================================================================
static std::string testConfig = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["INT", "BIGINT"],
  "rightInputTypes": ["INT", "BIGINT"],
  "outputTypes": ["INT", "BIGINT", "INT", "BIGINT"],
  "leftJoinKey": [0],
  "rightJoinKey": [0],
  "leftWindowEndIndex": 1,
  "rightWindowEndIndex": 1,
  "nonEquiCondition": null,
  "joinType": "InnerJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

// No-key config (empty joinKey arrays)
static std::string testConfigNoKey = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["BIGINT"],
  "rightInputTypes": ["BIGINT"],
  "outputTypes": ["BIGINT", "BIGINT"],
  "leftJoinKey": [],
  "rightJoinKey": [],
  "leftWindowEndIndex": 0,
  "rightWindowEndIndex": 0,
  "nonEquiCondition": null,
  "joinType": "InnerJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

// ============================================================================
// InnerJoinOperator Tests
// ============================================================================

TEST(InnerJoinOperatorTest, Construction)
{
    OutputTest out;
    auto config = nlohmann::json::parse(testConfig);
    auto *leftSer = new LongSerializer();
    auto *rightSer = new LongSerializer();

    auto *op = new InnerJoinOperator<int64_t>(config, &out, leftSer, rightSer);
    ASSERT_NE(op, nullptr);
    delete op;
}

TEST(InnerJoinOperatorTest, ConstructionNoKey)
{
    OutputTest out;
    auto config = nlohmann::json::parse(testConfigNoKey);
    auto *leftSer = new LongSerializer();
    auto *rightSer = new LongSerializer();

    auto *op = new InnerJoinOperator<int64_t>(config, &out, leftSer, rightSer);
    ASSERT_NE(op, nullptr);
    delete op;
}

// ============================================================================
// SemiAntiJoinOperator Tests
// ============================================================================

TEST(SemiAntiJoinOperatorTest, ConstructionSemiJoin)
{
    OutputTest out;
    auto config = nlohmann::json::parse(testConfig);
    auto *leftSer = new LongSerializer();
    auto *rightSer = new LongSerializer();

    auto *op = new SemiAntiJoinOperator<int64_t>(config, &out, leftSer, rightSer, false);
    ASSERT_NE(op, nullptr);
    delete op;
}

TEST(SemiAntiJoinOperatorTest, ConstructionAntiJoin)
{
    OutputTest out;
    auto config = nlohmann::json::parse(testConfig);
    auto *leftSer = new LongSerializer();
    auto *rightSer = new LongSerializer();

    auto *op = new SemiAntiJoinOperator<int64_t>(config, &out, leftSer, rightSer, true);
    ASSERT_NE(op, nullptr);
    delete op;
}

TEST(SemiAntiJoinOperatorTest, JoinEmptyImpl)
{
    OutputTest out;
    auto config = nlohmann::json::parse(testConfig);
    auto *leftSer = new LongSerializer();
    auto *rightSer = new LongSerializer();

    auto *op = new SemiAntiJoinOperator<int64_t>(config, &out, leftSer, rightSer, false);
    std::vector<VectorBatchId> left;
    std::vector<VectorBatchId> right;
    // join() is empty implementation — safe to call
    op->join(&left, &right);
    op->join(nullptr, nullptr);
    delete op;
}

// ============================================================================
// LeftOuterJoinOperator Tests
// ============================================================================

TEST(LeftOuterJoinOperatorTest, Construction)
{
    OutputTest out;
    auto config = nlohmann::json::parse(testConfig);
    auto *leftSer = new LongSerializer();
    auto *rightSer = new LongSerializer();

    auto *op = new LeftOuterJoinOperator<int64_t>(config, &out, leftSer, rightSer);
    ASSERT_NE(op, nullptr);
    delete op;
}

// ============================================================================
// RightOuterJoinOperator Tests
// ============================================================================

TEST(RightOuterJoinOperatorTest, Construction)
{
    OutputTest out;
    auto config = nlohmann::json::parse(testConfig);
    auto *leftSer = new LongSerializer();
    auto *rightSer = new LongSerializer();

    auto *op = new RightOuterJoinOperator<int64_t>(config, &out, leftSer, rightSer);
    ASSERT_NE(op, nullptr);
    delete op;
}

// ============================================================================
// FullOuterJoinOperator Tests
// ============================================================================

TEST(FullOuterJoinOperatorTest, Construction)
{
    OutputTest out;
    auto config = nlohmann::json::parse(testConfig);
    auto *leftSer = new LongSerializer();
    auto *rightSer = new LongSerializer();

    auto *op = new FullOuterJoinOperator<int64_t>(config, &out, leftSer, rightSer);
    ASSERT_NE(op, nullptr);
    delete op;
}

// ============================================================================
// AbstractOuterJoinOperator — construction test (via LeftOuter)
// ============================================================================
TEST(AbstractOuterJoinOperatorTest, Construction)
{
    OutputTest out;
    auto config = nlohmann::json::parse(testConfig);
    auto *leftSer = new LongSerializer();
    auto *rightSer = new LongSerializer();

    auto *op = new LeftOuterJoinOperator<int64_t>(config, &out, leftSer, rightSer);
    ASSERT_NE(op, nullptr);
    delete op;
}

// ============================================================================
// Paths that cannot be covered without full runtime (open() + SVE):
//
// 1. All join() methods on InnerJoin/LeftOuter/RightOuter/FullOuter:
//    Calling join() without open() segfaults because internal state
//    (SVE buffers, output batch, collector) is uninitialized.
//    open() requires full OmniStreamTask runtime environment.
//
// 2. Non-equi condition paths (filter):
//    Requires LLVM JIT codegen via generateJoinCondition().
//
// 3. ProcessWatermark / onProcessingTime:
//    Tested in WindowJoinOperatorCoverageTest.cpp
// ============================================================================
