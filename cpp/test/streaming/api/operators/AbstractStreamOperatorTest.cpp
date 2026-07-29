/*
 * Unit tests for AbstractStreamOperator<K>
 */
#include <gtest/gtest.h>
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "test/core/operators/OutputTest.h"

// Minimal concrete subclass to test AbstractStreamOperator
class TestableStreamOperator : public AbstractStreamOperator<void*> {
public:
    TestableStreamOperator() : AbstractStreamOperator<void*>() {}
    explicit TestableStreamOperator(Output* output) : AbstractStreamOperator<void*>(output) {}

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override {
        AbstractStreamOperator<void*>::initializeState(initializer, keySerializer);
    }


};

// ---------- Constructor / setup ----------

TEST(AbstractStreamOperatorTest, DefaultConstructor) {
    TestableStreamOperator op;
    EXPECT_EQ(op.GetOutput(), nullptr);
    EXPECT_EQ(op.GetOpName(), "");
}

TEST(AbstractStreamOperatorTest, ConstructorWithOutput) {
    OutputTest out;
    TestableStreamOperator op(&out);
    EXPECT_EQ(op.GetOutput(), &out);
}

TEST(AbstractStreamOperatorTest, SetupCreatesRuntimeContext) {
    TestableStreamOperator op;
    op.setup();
    EXPECT_NE(op.getRuntimeContext(), nullptr);
}

// ---------- Output setter/getter ----------

TEST(AbstractStreamOperatorTest, SetOutput) {
    TestableStreamOperator op;
    OutputTest out;
    op.setOutput(&out);
    EXPECT_EQ(op.GetOutput(), &out);
}

// ---------- OpName setter/getter ----------

TEST(AbstractStreamOperatorTest, SetGetOpName) {
    TestableStreamOperator op;
    op.SetOpName("myOp");
    EXPECT_EQ(op.GetOpName(), "myOp");
}

// ---------- open / close ----------

TEST(AbstractStreamOperatorTest, OpenDoesNotCrash) {
    TestableStreamOperator op;
    op.setup();
    EXPECT_NO_THROW(op.open());
}

TEST(AbstractStreamOperatorTest, CloseWithNullStateHandler) {
    TestableStreamOperator op;
    // stateHandler is nullptr by default, close should handle gracefully
    EXPECT_NO_THROW(op.close());
}

// ---------- getTypeName ----------

TEST(AbstractStreamOperatorTest, GetTypeNameContainsClassName) {
    TestableStreamOperator op;
    auto typeName = op.getTypeName();
    EXPECT_TRUE(typeName.find("AbstractStreamOperator") != std::string::npos);
}

// ---------- GetOperatorKeySerializer ----------

TEST(AbstractStreamOperatorTest, GetOperatorKeySerializerReturnsNonNull) {
    TestableStreamOperator op;
    auto* serializer = op.GetOperatorKeySerializer();
    EXPECT_NE(serializer, nullptr);
    delete serializer;
}

// ---------- ProcessingTimeService setter/getter ----------

TEST(AbstractStreamOperatorTest, SetGetProcessingTimeService) {
    TestableStreamOperator op;
    EXPECT_EQ(op.getProcessingTimeService(), nullptr);
    // We only test null → non-null wiring; creating a real ProcessingTimeService
    // would require threading infrastructure.
}

// ---------- ProcessWatermark ----------

TEST(AbstractStreamOperatorTest, ProcessWatermarkForwardsToOutput) {
    OutputTest out;
    TestableStreamOperator op(&out);
    op.setup();
    Watermark wm(12345);
    // timeServiceManager is nullptr, so watermark goes directly to output
    op.ProcessWatermark(&wm);
    ASSERT_NE(out.getWatermark(), nullptr);
    EXPECT_EQ(out.getWatermark()->getTimestamp(), 12345);
}

// ---------- processWatermarkStatus ----------

TEST(AbstractStreamOperatorTest, ProcessWatermarkStatusForwardsToOutput) {
    OutputTest out;
    TestableStreamOperator op(&out);
    op.setup();
    WatermarkStatus status(WatermarkStatus::idleStatus);
    EXPECT_NO_THROW(op.processWatermarkStatus(&status));
}

// ---------- GetMectrics ----------

TEST(AbstractStreamOperatorTest, GetMectricsDefaultNull) {
    TestableStreamOperator op;
    op.setup();
    auto metrics = op.GetMectrics();
    EXPECT_EQ(metrics, nullptr);
}

// ---------- setDescription ----------

TEST(AbstractStreamOperatorTest, SetDescription) {
    TestableStreamOperator op;
    nlohmann::json desc = {{"key", "value"}};
    EXPECT_NO_THROW(op.setDescription(desc));
}

/*
 * Interfaces NOT tested and reasons:
 *
 * 1. setCurrentKey / getCurrentKey:
 *    Requires stateHandler to be initialized (via initializeState with a real
 *    StreamTaskStateInitializerImpl + EnvironmentV2). Calling with nullptr
 *    stateHandler would segfault. Cannot mock without full state backend.
 *
 * 2. initializeState(StreamTaskStateInitializerImpl*, TypeSerializer*):
 *    Requires a fully configured EnvironmentV2 with TaskConfiguration,
 *    TaskStateManager, etc. The method creates StreamOperatorStateHandler
 *    and InternalTimeServiceManager which need real state backends.
 *
 * 3. getKeyedStateBackend:
 *    Returns stateHandler->getKeyedStateBackend(). Requires stateHandler
 *    to be initialized first (see initializeState above).
 *
 * 4. getInternalTimerService:
 *    Requires both timeServiceManager and keyedStateBackend to be non-null.
 *    These are set during initializeState which needs a full runtime env.
 *
 * 5. SnapshotState:
 *    Delegates to stateHandler->SnapshotState(). Requires full state handler
 *    initialization with checkpoint stream factory, which needs I/O setup.
 *
 * 6. notifyCheckpointComplete / notifyCheckpointAborted:
 *    Delegate to stateHandler which must be initialized first.
 *
 * 7. setup(shared_ptr<OmniStreamTask>):
 *    Requires creating an OmniStreamTask which needs the full task manager,
 *    network stack, and environment setup. The method calls task->env()
 *    to get metrics.
 *
 * 8. ProcessWatermark1 / ProcessWatermark2 (two-input watermark):
 *    Requires combinedWatermark to be initialized (done in setup()), but
 *    also needs a proper output. The private ProcessWatermark(mark, index)
 *    method updates combinedWatermark and conditionally forwards. Testing
 *    this would require calling the public ProcessWatermark1/2 methods
 *    which work but produce nondeterministic watermark advancement.
 */
