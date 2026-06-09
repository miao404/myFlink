#include <gtest/gtest.h>
#include "table/runtime/operators/join/window/WindowJoinOperator.h"
#include "table/runtime/operators/join/window/InnerJoinOperator.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "test/core/operators/OutputTest.h"
#include "core/typeutils/LongSerializer.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"

using namespace omnistream;

// ============================================================================
// JSON configurations for various test scenarios
// ============================================================================

// Standard equi-join with INT keys
static std::string configEqui = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["INT", "BIGINT", "INT"],
  "rightInputTypes": ["INT", "BIGINT", "INT"],
  "outputTypes": ["INT", "BIGINT", "INT", "INT", "BIGINT", "INT"],
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

// No-key join (leftJoinKey is empty)
static std::string configNoKey = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["BIGINT", "BIGINT"],
  "rightInputTypes": ["BIGINT", "BIGINT"],
  "outputTypes": ["BIGINT", "BIGINT", "BIGINT", "BIGINT"],
  "leftJoinKey": [],
  "rightJoinKey": [],
  "leftWindowEndIndex": 1,
  "rightWindowEndIndex": 1,
  "nonEquiCondition": null,
  "joinType": "InnerJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

// Non-equi condition join (to cover filter, getAllColRefs, generateJoinCondition)
static std::string configNonEqui = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["INT", "BIGINT", "INT"],
  "rightInputTypes": ["INT", "BIGINT", "INT"],
  "outputTypes": ["INT", "BIGINT", "INT", "INT", "BIGINT", "INT"],
  "leftJoinKey": [],
  "rightJoinKey": [],
  "leftWindowEndIndex": 1,
  "rightWindowEndIndex": 1,
  "nonEquiCondition": {"exprType":"BINARY","returnType": 4,"operator":"GREATER_THAN_OR_EQUAL","left":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":0},"right":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":3}},
  "joinType": "InnerJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

// DOUBLE type columns
static std::string configDouble = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["INT", "BIGINT", "DOUBLE"],
  "rightInputTypes": ["INT", "BIGINT", "DOUBLE"],
  "outputTypes": ["INT", "BIGINT", "DOUBLE", "INT", "BIGINT", "DOUBLE"],
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

// Mismatched key types (left INT key, right BIGINT key)
static std::string configMismatchedKeys = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["INT", "BIGINT", "INT"],
  "rightInputTypes": ["BIGINT", "BIGINT", "INT"],
  "outputTypes": ["INT", "BIGINT", "INT", "BIGINT", "BIGINT", "INT"],
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

// ============================================================================
// Helper: create an initialized operator
// ============================================================================
template <typename OpType, typename KeyType>
OpType* createInitializedOp(const std::string& jsonConfig, OutputTestVectorBatch* out, TypeSerializer* keySerializer)
{
    nlohmann::json parsed = nlohmann::json::parse(jsonConfig);
    auto op = new OpType(parsed, out, new LongSerializer(), new LongSerializer());
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    op->setup();
    op->initializeState(initializer, keySerializer);
    return op;
}

// ============================================================================
// Test: processElement1 and processElement2 (lines 49-50, empty bodies)
// Covers: processElement1, processElement2
// ============================================================================
TEST(WindowJoinCoverageTest, ProcessElementEmptyMethods)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configEqui, out, new LongSerializer());
    op->open();

    // These are empty methods but calling them covers lines 49-50
    StreamRecord record(nullptr);
    op->processElement1(&record);
    op->processElement2(&record);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: ProcessWatermark1 and ProcessWatermark2 (lines 52-71)
// Covers: combinedWatermark->UpdateWatermark, timeServiceManager check, output->emitWatermark
// ============================================================================
TEST(WindowJoinCoverageTest, ProcessWatermark1And2)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configEqui, out, new LongSerializer());
    op->open();

    // ProcessWatermark1 - sets input 0 watermark. Combined stays at INT64_MIN
    // because input 1 is still at INT64_MIN, so no emit yet.
    Watermark wm1(1000);
    op->ProcessWatermark1(&wm1);

    // ProcessWatermark2 - sets input 1 watermark. Now combined = min(1000, 2000) = 1000
    // which is > INT64_MIN, so combined watermark advances and emits.
    Watermark wm2(2000);
    op->ProcessWatermark2(&wm2);
    // After both inputs advance, watermark should be emitted
    EXPECT_NE(out->getWatermark(), nullptr);
    EXPECT_EQ(out->getWatermark()->getTimestamp(), 1000);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: getTypeName (line 76)
// Covers: return "WindowJoinOperator"
// ============================================================================
TEST(WindowJoinCoverageTest, GetTypeName)
{
    auto *out = new OutputTestVectorBatch();
    nlohmann::json parsed = nlohmann::json::parse(configEqui);
    auto op = new InnerJoinOperator<int32_t>(parsed, out, new LongSerializer(), new LongSerializer());

    EXPECT_EQ(op->getTypeName(), "WindowJoinOperator");

    delete op;
    delete out;
}

// ============================================================================
// Test: GetMectrics (lines 81-85)
// Covers: return this->metrics (nullptr when no task set)
// ============================================================================
TEST(WindowJoinCoverageTest, GetMectrics)
{
    auto *out = new OutputTestVectorBatch();
    nlohmann::json parsed = nlohmann::json::parse(configEqui);
    auto op = new InnerJoinOperator<int32_t>(parsed, out, new LongSerializer(), new LongSerializer());
    op->setup();

    auto metrics = op->GetMectrics();
    EXPECT_EQ(metrics, nullptr);

    delete op;
    delete out;
}

// ============================================================================
// Test: onProcessingTime throws exception (lines 315-317)
// Covers: THROW_LOGIC_EXCEPTION("Window Join only support event-time now")
// ============================================================================
TEST(WindowJoinCoverageTest, OnProcessingTimeThrows)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configEqui, out, new LongSerializer());
    op->open();

    // Constructor: TimerHeapInternalTimer(int64_t timestamp, K key, N nameSpace)
    TimerHeapInternalTimer<int32_t, int64_t> timer(1000, 0, 0);
    EXPECT_THROW(op->onProcessingTime(&timer), std::exception);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: Key type mismatch in open() (line 214)
// Covers: throw std::runtime_error("Left key types do not match right key types")
// ============================================================================
TEST(WindowJoinCoverageTest, MismatchedKeyTypesThrows)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configMismatchedKeys, out, new LongSerializer());

    EXPECT_THROW(op->open(), std::runtime_error);

    delete op;
    delete out;
}

// ============================================================================
// Test: No-key join (lines 220-226 in build env)
// Covers: hasKey==false branch in open(), setCurrentKey(0) for int64_t
// ============================================================================
TEST(WindowJoinCoverageTest, NoKeyJoin)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int64_t>, int64_t>(configNoKey, out, new LongSerializer());
    op->open();

    // Left batch: 1 row [value=1, windowEnd=1000]
    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto v1 = new omniruntime::vec::Vector<int64_t>(1);
    auto v2 = new omniruntime::vec::Vector<int64_t>(1);
    v1->SetValue(0, 1);
    v2->SetValue(0, 1000);
    vbatchLeft->Append(v1);
    vbatchLeft->Append(v2);

    // Right batch: 2 rows [value=2,3, windowEnd=1000,1000]
    auto vbatchRight = new omnistream::VectorBatch(2);
    auto v3 = new omniruntime::vec::Vector<int64_t>(2);
    auto v4 = new omniruntime::vec::Vector<int64_t>(2);
    v3->SetValue(0, 2);
    v3->SetValue(1, 3);
    v4->SetValue(0, 1000);
    v4->SetValue(1, 1000);
    vbatchRight->Append(v3);
    vbatchRight->Append(v4);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    // Should have output: cross product of left x right
    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: LeftOuterJoin with no matching right records (covers buildRightNull, lines 428-466)
// Also covers: onEventTime null rightRecords branch (line 300)
// Also covers: insertLeft with isInner=false (lines 561-569)
// ============================================================================
TEST(WindowJoinCoverageTest, LeftOuterJoinBuildRightNull)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<LeftOuterJoinOperator<int32_t>, int32_t>(configEqui, out, new LongSerializer());
    op->open();

    // Left batch: key=99 (no matching right)
    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 99);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(1);
    vTimeLeft->SetValue(0, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<int32_t>(1);
    vValLeft->SetValue(0, 42);
    vbatchLeft->Append(vValLeft);

    op->processBatch1(new StreamRecord(vbatchLeft));
    // No processBatch2 for key=99, so rightRecords will be nullptr for that window/key

    op->getInternalTimerService()->advanceWatermark(100000);

    // Should have output with right side nulls
    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: RightOuterJoin with no matching left records (covers buildLeftNull, lines 469-507)
// Also covers: onEventTime null leftRecords branch (line 295)
// ============================================================================
TEST(WindowJoinCoverageTest, RightOuterJoinBuildLeftNull)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<RightOuterJoinOperator<int32_t>, int32_t>(configEqui, out, new LongSerializer());
    op->open();

    // Right batch: key=99 (no matching left)
    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 99);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(1);
    vTimeRight->SetValue(0, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new omniruntime::vec::Vector<int32_t>(1);
    vValRight->SetValue(0, 77);
    vbatchRight->Append(vValRight);

    op->processBatch2(new StreamRecord(vbatchRight));
    // No processBatch1 for key=99, so leftRecords will be nullptr for that window/key

    op->getInternalTimerService()->advanceWatermark(100000);

    // Should have output with left side nulls
    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: InnerJoin with DOUBLE type columns (covers OMNI_DOUBLE branches in
//       BuildInnerLeft lines 361-363, BuildInnerRight lines 400-402)
// ============================================================================
TEST(WindowJoinCoverageTest, InnerJoinDoubleType)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configDouble, out, new LongSerializer());
    op->open();

    // Left: [key=0, windowEnd=1000, value=3.14]
    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 0);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(1);
    vTimeLeft->SetValue(0, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<double>(1);
    vValLeft->SetValue(0, 3.14);
    vbatchLeft->Append(vValLeft);

    // Right: [key=0, windowEnd=1000, value=2.71]
    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 0);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(1);
    vTimeRight->SetValue(0, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new omniruntime::vec::Vector<double>(1);
    vValRight->SetValue(0, 2.71);
    vbatchRight->Append(vValRight);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);
    // Verify double values are in the output
    auto outBatch = out->getAll()[0];
    auto leftDoubleCol = reinterpret_cast<omniruntime::vec::Vector<double>*>(outBatch->Get(2));
    auto rightDoubleCol = reinterpret_cast<omniruntime::vec::Vector<double>*>(outBatch->Get(5));
    EXPECT_DOUBLE_EQ(leftDoubleCol->GetValue(0), 3.14);
    EXPECT_DOUBLE_EQ(rightDoubleCol->GetValue(0), 2.71);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: NonEquiCondition join (covers lines 236-251, 738-744, 753-761, 766-797)
// This covers: generateJoinCondition with isNonEquiCondition=true,
//              getAllColRefs with FIELD_REFERENCE and recursive left/right,
//              filterFuncPtrs population,
//              filter() function,
//              insertLeft/insertRight with isNonEquiCondition branch (lines 561-569, 661-669)
// ============================================================================
TEST(WindowJoinCoverageTest, NonEquiConditionJoin)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configNonEqui, out, new VoidNamespaceSerializer());
    op->open();

    // Left batch: 3 rows with keys [0,1,2], windowEnd=1000, values [12,24,36]
    auto vbatchLeft = new omnistream::VectorBatch(3);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(3);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetValue(1, 1);
    vKeyLeft->SetValue(2, 2);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(3);
    for (int i = 0; i < 3; i++) vTimeLeft->SetValue(i, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<int32_t>(3);
    vValLeft->SetValue(0, 12);
    vValLeft->SetValue(1, 24);
    vValLeft->SetValue(2, 36);
    vbatchLeft->Append(vValLeft);

    // Right batch: 4 rows with keys [0,1,1,3], windowEnd=1000, values [100,200,300,400]
    auto vbatchRight = new omnistream::VectorBatch(4);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(4);
    vKeyRight->SetValue(0, 0);
    vKeyRight->SetValue(1, 1);
    vKeyRight->SetValue(2, 1);
    vKeyRight->SetValue(3, 3);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(4);
    for (int i = 0; i < 4; i++) vTimeRight->SetValue(i, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new omniruntime::vec::Vector<int32_t>(4);
    vValRight->SetValue(0, 100);
    vValRight->SetValue(1, 200);
    vValRight->SetValue(2, 300);
    vValRight->SetValue(3, 400);
    vbatchRight->Append(vValRight);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    // The non-equi condition is left[0] >= right[3] (col 0 >= col 3)
    // Should produce filtered results
    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: FullOuterJoin to cover both buildRightNull and buildLeftNull paths
// in a single test - left only and right only keys
// ============================================================================
TEST(WindowJoinCoverageTest, FullOuterJoinBothNullPaths)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<FullOuterJoinOperator<int32_t>, int32_t>(configEqui, out, new LongSerializer());
    op->open();

    // Left batch: key=10 (no matching right)
    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 10);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(1);
    vTimeLeft->SetValue(0, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<int32_t>(1);
    vValLeft->SetValue(0, 111);
    vbatchLeft->Append(vValLeft);

    // Right batch: key=20 (no matching left)
    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 20);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(1);
    vTimeRight->SetValue(0, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new omniruntime::vec::Vector<int32_t>(1);
    vValRight->SetValue(0, 222);
    vbatchRight->Append(vValRight);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    // Should have 2 output batches:
    // 1) key=10 with right-side nulls (buildRightNull)
    // 2) key=20 with left-side nulls (buildLeftNull)
    EXPECT_GE(out->getAll().size(), 2u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: ProcessWatermark1 with watermark not advancing (no emit)
// When watermark doesn't advance combined watermark, nothing is emitted
// ============================================================================
TEST(WindowJoinCoverageTest, ProcessWatermarkNoAdvance)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configEqui, out, new LongSerializer());
    op->open();

    // Call only ProcessWatermark1 - input 1 stays at INT64_MIN
    // Combined watermark = min(2000, INT64_MIN) = INT64_MIN → no advance, no emit
    Watermark wm1(2000);
    op->ProcessWatermark1(&wm1);
    // No watermark should be emitted (combined didn't advance)

    // Now advance input 1 - combined = min(2000, 1500) = 1500 > INT64_MIN → emits
    Watermark wm2(1500);
    op->ProcessWatermark2(&wm2);
    EXPECT_NE(out->getWatermark(), nullptr);
    EXPECT_EQ(out->getWatermark()->getTimestamp(), 1500);

    // Call ProcessWatermark1 again with same value - no advance
    Watermark wm3(2000);
    op->ProcessWatermark1(&wm3);
    // Combined = min(2000, 1500) = 1500, same as before, no new emit

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: getInternalTimerService accessor (line 77-80)
// ============================================================================
TEST(WindowJoinCoverageTest, GetInternalTimerService)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configEqui, out, new LongSerializer());
    op->open();

    auto timerService = op->getInternalTimerService();
    EXPECT_NE(timerService, nullptr);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: LeftOuterJoin with DOUBLE type - covers buildRightNull with OMNI_DOUBLE
// ============================================================================
TEST(WindowJoinCoverageTest, LeftOuterJoinDoubleType)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<LeftOuterJoinOperator<int32_t>, int32_t>(configDouble, out, new LongSerializer());
    op->open();

    // Left: [key=99, windowEnd=1000, value=9.99]
    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 99);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(1);
    vTimeLeft->SetValue(0, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<double>(1);
    vValLeft->SetValue(0, 9.99);
    vbatchLeft->Append(vValLeft);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->getInternalTimerService()->advanceWatermark(100000);

    // Output should have right side nulls
    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: RightOuterJoin with DOUBLE type - covers buildLeftNull with OMNI_DOUBLE
// ============================================================================
TEST(WindowJoinCoverageTest, RightOuterJoinDoubleType)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<RightOuterJoinOperator<int32_t>, int32_t>(configDouble, out, new LongSerializer());
    op->open();

    // Right: [key=99, windowEnd=1000, value=7.77]
    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 99);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(1);
    vTimeRight->SetValue(0, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new omniruntime::vec::Vector<double>(1);
    vValRight->SetValue(0, 7.77);
    vbatchRight->Append(vValRight);

    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: InnerJoin with SHORT (SMALLINT) type columns
// Covers: OMNI_SHORT branches in BuildInnerLeft (line 359-360) and BuildInnerRight (line 398-399)
// ============================================================================
static std::string configShort = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["INT", "BIGINT", "SMALLINT"],
  "rightInputTypes": ["INT", "BIGINT", "SMALLINT"],
  "outputTypes": ["INT", "BIGINT", "SMALLINT", "INT", "BIGINT", "SMALLINT"],
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

TEST(WindowJoinCoverageTest, InnerJoinShortType)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configShort, out, new LongSerializer());
    op->open();

    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 5);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(1);
    vTimeLeft->SetValue(0, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<int16_t>(1);
    vValLeft->SetValue(0, static_cast<int16_t>(42));
    vbatchLeft->Append(vValLeft);

    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 5);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(1);
    vTimeRight->SetValue(0, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new omniruntime::vec::Vector<int16_t>(1);
    vValRight->SetValue(0, static_cast<int16_t>(77));
    vbatchRight->Append(vValRight);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: LeftOuterJoin with SHORT type - covers buildRightNull OMNI_SHORT (line 443-444)
// and insertLeft with isInner=false for SHORT type
// ============================================================================
TEST(WindowJoinCoverageTest, LeftOuterJoinShortType)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<LeftOuterJoinOperator<int32_t>, int32_t>(configShort, out, new LongSerializer());
    op->open();

    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 99);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(1);
    vTimeLeft->SetValue(0, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<int16_t>(1);
    vValLeft->SetValue(0, static_cast<int16_t>(11));
    vbatchLeft->Append(vValLeft);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: RightOuterJoin with SHORT type - covers buildLeftNull OMNI_SHORT (line 484-485)
// ============================================================================
TEST(WindowJoinCoverageTest, RightOuterJoinShortType)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<RightOuterJoinOperator<int32_t>, int32_t>(configShort, out, new LongSerializer());
    op->open();

    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 99);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(1);
    vTimeRight->SetValue(0, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new omniruntime::vec::Vector<int16_t>(1);
    vValRight->SetValue(0, static_cast<int16_t>(22));
    vbatchRight->Append(vValRight);

    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: InnerJoin with BOOLEAN type columns
// Covers: OMNI_BOOLEAN branches in BuildInnerLeft (line 374-375) and BuildInnerRight (line 413-414)
// ============================================================================
static std::string configBoolean = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["INT", "BIGINT", "BOOLEAN"],
  "rightInputTypes": ["INT", "BIGINT", "BOOLEAN"],
  "outputTypes": ["INT", "BIGINT", "BOOLEAN", "INT", "BIGINT", "BOOLEAN"],
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

TEST(WindowJoinCoverageTest, InnerJoinBooleanType)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configBoolean, out, new LongSerializer());
    op->open();

    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 1);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(1);
    vTimeLeft->SetValue(0, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<bool>(1);
    vValLeft->SetValue(0, true);
    vbatchLeft->Append(vValLeft);

    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 1);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(1);
    vTimeRight->SetValue(0, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new omniruntime::vec::Vector<bool>(1);
    vValRight->SetValue(0, false);
    vbatchRight->Append(vValRight);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: LeftOuterJoin with BOOLEAN type - covers buildRightNull OMNI_BOOLEAN (line 455-456)
// ============================================================================
TEST(WindowJoinCoverageTest, LeftOuterJoinBooleanType)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<LeftOuterJoinOperator<int32_t>, int32_t>(configBoolean, out, new LongSerializer());
    op->open();

    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 99);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(1);
    vTimeLeft->SetValue(0, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<bool>(1);
    vValLeft->SetValue(0, true);
    vbatchLeft->Append(vValLeft);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: RightOuterJoin with BOOLEAN type - covers buildLeftNull OMNI_BOOLEAN (line 496-497)
// ============================================================================
TEST(WindowJoinCoverageTest, RightOuterJoinBooleanType)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<RightOuterJoinOperator<int32_t>, int32_t>(configBoolean, out, new LongSerializer());
    op->open();

    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 99);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(1);
    vTimeRight->SetValue(0, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new omniruntime::vec::Vector<bool>(1);
    vValRight->SetValue(0, false);
    vbatchRight->Append(vValRight);

    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: InnerJoin with VARCHAR (STRING) type columns
// Covers: OMNI_VARCHAR branches in BuildInnerLeft (line 380-382) insertLeftVarchar (lines 587-636)
//         and BuildInnerRight (line 419-421) insertRightVarchar (lines 688-734)
// ============================================================================
static std::string configVarchar = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["INT", "BIGINT", "STRING"],
  "rightInputTypes": ["INT", "BIGINT", "STRING"],
  "outputTypes": ["INT", "BIGINT", "STRING", "INT", "BIGINT", "STRING"],
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

TEST(WindowJoinCoverageTest, InnerJoinVarcharType)
{
    using varcharVecType = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;

    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configVarchar, out, new LongSerializer());
    op->open();

    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 1);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(1);
    vTimeLeft->SetValue(0, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new varcharVecType(1);
    std::string leftStr = "hello";
    std::string_view leftSv(leftStr);
    vValLeft->SetValue(0, leftSv);
    vbatchLeft->Append(vValLeft);

    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 1);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(1);
    vTimeRight->SetValue(0, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new varcharVecType(1);
    std::string rightStr = "world";
    std::string_view rightSv(rightStr);
    vValRight->SetValue(0, rightSv);
    vbatchRight->Append(vValRight);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: LeftOuterJoin with VARCHAR type - covers insertLeftVarchar with isInner=false
// and buildRightNull OMNI_CHAR branch (line 461-462)
// ============================================================================
static std::string configVarcharLeftOuter = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["INT", "BIGINT", "STRING"],
  "rightInputTypes": ["INT", "BIGINT", "STRING"],
  "outputTypes": ["INT", "BIGINT", "STRING", "INT", "BIGINT", "STRING"],
  "leftJoinKey": [0],
  "rightJoinKey": [0],
  "leftWindowEndIndex": 1,
  "rightWindowEndIndex": 1,
  "nonEquiCondition": null,
  "joinType": "LeftOuterJoin",
  "leftWindowing": "TUMBLE(size=[10 s])",
  "leftTimeAttributeType": 2,
  "rightWindowing": "TUMBLE(size=[10 s])",
  "rightTimeAttributeType": 2
})delimiter";

TEST(WindowJoinCoverageTest, LeftOuterJoinVarcharType)
{
    using varcharVecType = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;

    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<LeftOuterJoinOperator<int32_t>, int32_t>(configVarcharLeftOuter, out, new LongSerializer());
    op->open();

    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 99);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(1);
    vTimeLeft->SetValue(0, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new varcharVecType(1);
    std::string valStr = "test_left";
    std::string_view valSv(valStr);
    vValLeft->SetValue(0, valSv);
    vbatchLeft->Append(vValLeft);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: RightOuterJoin with VARCHAR type - covers insertRightVarchar with isInner=false
// and buildLeftNull OMNI_CHAR branch (line 502-503)
// ============================================================================
TEST(WindowJoinCoverageTest, RightOuterJoinVarcharType)
{
    using varcharVecType = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;

    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<RightOuterJoinOperator<int32_t>, int32_t>(configVarchar, out, new LongSerializer());
    op->open();

    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 99);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(1);
    vTimeRight->SetValue(0, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new varcharVecType(1);
    std::string valStr = "test_right";
    std::string_view valSv2(valStr);
    vValRight->SetValue(0, valSv2);
    vbatchRight->Append(vValRight);

    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: InnerJoin with DECIMAL128 type columns
// Covers: OMNI_DECIMAL128 branches in BuildInnerLeft (line 377-378) and BuildInnerRight (line 416-417)
// ============================================================================
static std::string configDecimal128 = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["INT", "BIGINT", "DECIMAL(20, 2)"],
  "rightInputTypes": ["INT", "BIGINT", "DECIMAL(20, 2)"],
  "outputTypes": ["INT", "BIGINT", "DECIMAL(20, 2)", "INT", "BIGINT", "DECIMAL(20, 2)"],
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

TEST(WindowJoinCoverageTest, InnerJoinDecimal128Type)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configDecimal128, out, new LongSerializer());
    op->open();

    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 1);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(1);
    vTimeLeft->SetValue(0, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(1);
    omniruntime::type::Decimal128 leftDec(12345);
    vValLeft->SetValue(0, leftDec);
    vbatchLeft->Append(vValLeft);

    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 1);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(1);
    vTimeRight->SetValue(0, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(1);
    omniruntime::type::Decimal128 rightDec(67890);
    vValRight->SetValue(0, rightDec);
    vbatchRight->Append(vValRight);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: LeftOuterJoin with DECIMAL128 type - covers buildRightNull OMNI_DECIMAL128 (line 458-459)
// ============================================================================
TEST(WindowJoinCoverageTest, LeftOuterJoinDecimal128Type)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<LeftOuterJoinOperator<int32_t>, int32_t>(configDecimal128, out, new LongSerializer());
    op->open();

    auto vbatchLeft = new omnistream::VectorBatch(1);
    auto vKeyLeft = new omniruntime::vec::Vector<int32_t>(1);
    vKeyLeft->SetValue(0, 99);
    vbatchLeft->Append(vKeyLeft);
    auto vTimeLeft = new omniruntime::vec::Vector<int64_t>(1);
    vTimeLeft->SetValue(0, 1000);
    vbatchLeft->Append(vTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(1);
    omniruntime::type::Decimal128 dec(55555);
    vValLeft->SetValue(0, dec);
    vbatchLeft->Append(vValLeft);

    op->processBatch1(new StreamRecord(vbatchLeft));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: RightOuterJoin with DECIMAL128 type - covers buildLeftNull OMNI_DECIMAL128 (line 499-500)
// ============================================================================
TEST(WindowJoinCoverageTest, RightOuterJoinDecimal128Type)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<RightOuterJoinOperator<int32_t>, int32_t>(configDecimal128, out, new LongSerializer());
    op->open();

    auto vbatchRight = new omnistream::VectorBatch(1);
    auto vKeyRight = new omniruntime::vec::Vector<int32_t>(1);
    vKeyRight->SetValue(0, 99);
    vbatchRight->Append(vKeyRight);
    auto vTimeRight = new omniruntime::vec::Vector<int64_t>(1);
    vTimeRight->SetValue(0, 1000);
    vbatchRight->Append(vTimeRight);
    auto vValRight = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(1);
    omniruntime::type::Decimal128 dec(77777);
    vValRight->SetValue(0, dec);
    vbatchRight->Append(vValRight);

    op->processBatch2(new StreamRecord(vbatchRight));
    op->getInternalTimerService()->advanceWatermark(100000);

    EXPECT_GT(out->getAll().size(), 0u);

    op->close();
    delete op;
    delete out;
}

// ============================================================================
// Test: Destructor coverage - operator that has been opened and closed
// Covers: ~WindowJoinOperator delete collector (line 171-172)
// ============================================================================
TEST(WindowJoinCoverageTest, DestructorDeletesCollector)
{
    auto *out = new OutputTestVectorBatch();
    auto op = createInitializedOp<InnerJoinOperator<int32_t>, int32_t>(configEqui, out, new LongSerializer());
    op->open();
    // Explicitly delete operator (not just close) to invoke destructor path
    delete op;
    delete out;
}

// ============================================================================
// Code paths that MAY NOT be coverable and reasons:
//
// 1. generateJoinCondition with isNonEquiCondition=true (lines 748-757):
//    Requires SimpleFilterCodeGen and LLVM JIT compilation to produce a native filter
//    function pointer at runtime. If the JIT infrastructure is not available in the
//    test build environment, this path cannot be covered. The NonEquiConditionJoin
//    test above attempts this - if it passes, these lines are covered.
//
// 2. filter() function (lines 778-808):
//    Depends on generatedFilter being a valid function pointer produced by JIT
//    compilation (see point 1 above). If JIT works, the NonEquiConditionJoin test
//    will cover this path.
//
// Note: All type branches (SHORT, BOOLEAN, DECIMAL128, VARCHAR) now have dedicated
// tests. The VARCHAR tests use LargeStringContainer<std::string_view> vectors directly.
// If the build environment's OmniRuntime supports creating these vectors in test code,
// the insertLeftVarchar/insertRightVarchar paths will be covered.
// ============================================================================
