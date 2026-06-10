/*
 * Unit tests for StreamOperatorStateHandler
 * Covers constructor branches, dispose, notify methods, and initializeOperatorState.
 */
#include <gtest/gtest.h>
#include "streaming/api/operators/StreamOperatorStateHandler.h"

using namespace omnistream;

// ============================================================================
// Test: Constructor with null keyedStateBackend — keyedStateStore should be null
// ============================================================================
TEST(StreamOperatorStateHandlerTest, ConstructorNullBackend)
{
    // Create context with null backends
    auto *context = new StreamOperatorStateContextImpl<int64_t>(
        std::nullopt, nullptr, nullptr, nullptr);

    StreamOperatorStateHandler<int64_t> handler(context);

    EXPECT_EQ(handler.getKeyedStateBackend(), nullptr);
    EXPECT_EQ(handler.getKeyedStateStore(), nullptr);
}

// ============================================================================
// Test: dispose() with null backends — should not crash
// ============================================================================
TEST(StreamOperatorStateHandlerTest, DisposeNullBackends)
{
    auto *context = new StreamOperatorStateContextImpl<int64_t>(
        std::nullopt, nullptr, nullptr, nullptr);

    StreamOperatorStateHandler<int64_t> handler(context);
    // Should not crash with null backends
    handler.dispose();

    EXPECT_EQ(handler.getKeyedStateBackend(), nullptr);
}

// ============================================================================
// Test: notifyCheckpointComplete with null backend — should not crash
// ============================================================================
TEST(StreamOperatorStateHandlerTest, NotifyCheckpointCompleteNullBackend)
{
    auto *context = new StreamOperatorStateContextImpl<int64_t>(
        std::nullopt, nullptr, nullptr, nullptr);

    StreamOperatorStateHandler<int64_t> handler(context);
    // Should not crash — dynamic_cast to RocksdbKeyedStateBackend returns nullptr
    handler.notifyCheckpointComplete(1L);
}

// ============================================================================
// Test: notifyCheckpointAborted with null backend — should not crash
// ============================================================================
TEST(StreamOperatorStateHandlerTest, NotifyCheckpointAbortedNullBackend)
{
    auto *context = new StreamOperatorStateContextImpl<int64_t>(
        std::nullopt, nullptr, nullptr, nullptr);

    StreamOperatorStateHandler<int64_t> handler(context);
    // Should not crash — dynamic_cast to RocksdbKeyedStateBackend returns nullptr
    handler.notifyCheckpointAborted(1L);
}

// ============================================================================
// Test: initializeOperatorState with null backends
// ============================================================================
TEST(StreamOperatorStateHandlerTest, InitializeOperatorStateNullBackends)
{
    auto *context = new StreamOperatorStateContextImpl<int64_t>(
        std::nullopt, nullptr, nullptr, nullptr);

    StreamOperatorStateHandler<int64_t> handler(context);

    // Simple mock implementation of CheckpointedStreamOperator
    class TestCheckpointedOp : public StreamOperatorStateHandler<int64_t>::CheckpointedStreamOperator {
    public:
        bool initCalled = false;
        void initializeState(StateInitializationContextImpl<int64_t> *ctx) override {
            initCalled = true;
        }
    };

    TestCheckpointedOp testOp;
    handler.initializeOperatorState(&testOp);
    EXPECT_TRUE(testOp.initCalled);
}

// ============================================================================
// Test: SnapshotState with null backends — covers minimal branch
// ============================================================================
TEST(StreamOperatorStateHandlerTest, SnapshotStateNullBackends)
{
    auto *context = new StreamOperatorStateContextImpl<int64_t>(
        std::nullopt, nullptr, nullptr, nullptr);

    StreamOperatorStateHandler<int64_t> handler(context);

    class TestCheckpointedOp : public StreamOperatorStateHandler<int64_t>::CheckpointedStreamOperator {
    public:
        void snapshotState(StateSnapshotContextSynchronousImpl *ctx) override {}
    };

    TestCheckpointedOp testOp;
    CheckpointOptions options(CheckpointType::CHECKPOINT);

    auto *result = handler.SnapshotState(
        &testOp,
        nullptr,  // timeServiceManager
        "test-operator",
        1L,       // checkpointId
        1000L,    // timestamp
        &options,
        nullptr,  // checkpointStreamFactory
        false,    // isUsingCustomRawKeyedState
        nullptr   // bridge
    );

    ASSERT_NE(result, nullptr);
    delete result;
}

// ============================================================================
// Test: isCanonicalSavepoint returns false for CHECKPOINT type
// (indirectly tested via SnapshotState — when keyedStateBackend is not null,
//  isCanonicalSavepoint check determines the branch. With CHECKPOINT type,
//  it should return false.)
// ============================================================================

// ============================================================================
// Paths that may not be fully coverable:
//
// 1. Constructor with non-null keyedStateBackend:
//    Requires a real HeapKeyedStateBackend or RocksdbKeyedStateBackend
//    which need KeyGroupRange, environment, etc.
//
// 2. setCurrentKey / getCurrentKey:
//    Require non-null keyedStateBackend
//
// 3. SnapshotState with timeServiceManager != nullptr:
//    Requires InternalTimeServiceManager which needs full runtime setup
//
// 4. SnapshotState canonical savepoint branch:
//    Requires a real keyed state backend with savepoint() support
//
// 5. SnapshotState exception handling branches:
//    Requires forcing an exception in snapshot operations
// ============================================================================
