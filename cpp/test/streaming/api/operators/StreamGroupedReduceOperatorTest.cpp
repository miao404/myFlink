/*
 * Unit tests for StreamGroupedReduceOperator<K>
 */
#include <gtest/gtest.h>
#include "streaming/api/operators/StreamGroupedReduceOperator.h"
#include "test/core/operators/OutputTest.h"
#include "test/core/operators/test_utils/Mocks.h"

using ReduceOp = omnistream::datastream::StreamGroupedReduceOperator<Object>;

// ---------- Test-only constructor ----------

TEST(StreamGroupedReduceOpTest, TestOnlyConstructor) {
    OutputTest out;
    ReduceOp op(&out, true);
    EXPECT_EQ(std::string(op.getName()), "StreamGroupedReduceOperator");
}

// ---------- getName ----------

TEST(StreamGroupedReduceOpTest, GetName) {
    OutputTest out;
    ReduceOp op(&out, false);
    EXPECT_STREQ(op.getName(), "StreamGroupedReduceOperator");
}

// ---------- canBeStreamOperator ----------

TEST(StreamGroupedReduceOpTest, CanBeStreamOperatorTrue) {
    OutputTest out;
    ReduceOp op(&out, true);
    EXPECT_TRUE(op.canBeStreamOperator());
}

TEST(StreamGroupedReduceOpTest, CanBeStreamOperatorFalse) {
    OutputTest out;
    ReduceOp op(&out, false);
    EXPECT_FALSE(op.canBeStreamOperator());
}

// ---------- isSetKeyContextElement ----------

TEST(StreamGroupedReduceOpTest, IsSetKeyContextElement) {
    OutputTest out;
    ReduceOp op(&out, true);
    EXPECT_TRUE(op.isSetKeyContextElement());
}

// ---------- ProcessWatermark ----------

TEST(StreamGroupedReduceOpTest, ProcessWatermarkForwarded) {
    OutputTest out;
    ReduceOp op(&out, true);
    op.setup();
    Watermark wm(54321);
    op.ProcessWatermark(&wm);
    ASSERT_NE(out.getWatermark(), nullptr);
    EXPECT_EQ(out.getWatermark()->getTimestamp(), 54321);
}

// ---------- processWatermarkStatus ----------

TEST(StreamGroupedReduceOpTest, ProcessWatermarkStatusForwarded) {
    OutputTest out;
    ReduceOp op(&out, true);
    op.setup();
    WatermarkStatus status(WatermarkStatus::idleStatus);
    EXPECT_NO_THROW(op.processWatermarkStatus(&status));
}

// ---------- close ----------

TEST(StreamGroupedReduceOpTest, CloseDoesNotCrash) {
    OutputTest out;
    ReduceOp op(&out, true);
    EXPECT_NO_THROW(op.close());
}

/*
 * Interfaces NOT tested and reasons:
 *
 * 1. StreamGroupedReduceOperator(Output*, json, bool, TypeSerializer*):
 *    Requires config with "udf_so", "hash_path", "key_so", "udf_obj" keys.
 *    The constructor calls loadUdf() which dynamically loads reduce function
 *    and key selector from shared libraries (.so files). Without actual .so
 *    files this will throw.
 *
 * 2. loadUdf(json):
 *    Same as above - requires UDFLoader::LoadReduceFunction and
 *    LoadKeySelectFunction which dlopen .so files.
 *
 * 3. processElement(StreamRecord*):
 *    Requires:
 *    - values (ValueState) to be initialized via open()
 *    - open() requires stateHandler from initializeState()
 *    - initializeState() requires full EnvironmentV2 + state backend
 *    - keySelector must be loaded from .so
 *    - BindCoreManager for CPU affinity
 *    The method accesses values->value(), calls userFunction->reduce(),
 *    and updates state. All these need the full state backend pipeline.
 *
 * 4. initializeState(StreamTaskStateInitializerImpl*, TypeSerializer*):
 *    Calls AbstractStreamOperator::initializeState which needs
 *    a fully configured EnvironmentV2 with TaskConfiguration,
 *    TaskStateManager, etc. Also accesses BindCoreManager.
 *
 * 5. open():
 *    Calls AbstractUdfStreamOperator::open(), creates ValueStateDescriptor,
 *    and gets state from stateHandler->getKeyedStateStore(). Requires
 *    stateHandler initialization (from initializeState).
 *
 * 6. setKeyContextElement(StreamRecord*):
 *    Requires keySelector (loaded from .so) and stateHandler to be valid.
 *    Extracts key from record value and sets it on stateHandler.
 *
 * 7. ~StreamGroupedReduceOperator():
 *    Deletes keySelector. Only relevant if constructor succeeded with
 *    UDF loading.
 */
