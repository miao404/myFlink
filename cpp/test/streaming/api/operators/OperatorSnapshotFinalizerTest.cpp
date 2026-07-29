/*
 * Unit tests for OperatorSnapshotFinalizer
 * Covers constructor branches (null vs non-null futures) and accessor methods.
 */
#include <gtest/gtest.h>

// Build env has OperatorSubtaskState in omnistream namespace; bring it into scope
// before OperatorSnapshotFinalizer.h uses it unqualified.
#include "runtime/checkpoint/OperatorSubtaskState.h"
using omnistream::OperatorSubtaskState;

#include "streaming/api/operators/OperatorSnapshotFinalizer.h"

// ============================================================================
// Test: All futures are nullptr — covers all "else" branches in constructor
// ============================================================================
TEST(OperatorSnapshotFinalizerTest, AllNullFutures)
{
    auto *futures = new OperatorSnapshotFutures();
    // All futures are nullptr by default

    OperatorSnapshotFinalizer finalizer(futures);

    auto jobManagerState = finalizer.getJobManagerOwnedState();
    auto taskLocalState = finalizer.getTaskLocalState();

    ASSERT_NE(jobManagerState, nullptr);
    ASSERT_NE(taskLocalState, nullptr);

    delete futures;
}

// ============================================================================
// Test: KeyedStateManaged future is set (non-null) — covers "if (keyedStateManaged)" branch
// ============================================================================
TEST(OperatorSnapshotFinalizerTest, WithKeyedStateManagedFuture)
{
    auto *futures = new OperatorSnapshotFutures();

    // Create a packaged_task that returns a valid SnapshotResult
    auto task = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<KeyedStateHandle>> {
            return std::make_shared<SnapshotResult<KeyedStateHandle>>(nullptr, nullptr);
        });
    futures->setKeyedStateManagedFuture(task);

    OperatorSnapshotFinalizer finalizer(futures);

    ASSERT_NE(finalizer.getJobManagerOwnedState(), nullptr);
    ASSERT_NE(finalizer.getTaskLocalState(), nullptr);

    delete futures;
}

// ============================================================================
// Test: KeyedStateRaw future is set (non-null) — covers "if (KeyedStateRaw)" branch
// ============================================================================
TEST(OperatorSnapshotFinalizerTest, WithKeyedStateRawFuture)
{
    auto *futures = new OperatorSnapshotFutures();

    auto task = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<KeyedStateHandle>> {
            return std::make_shared<SnapshotResult<KeyedStateHandle>>(nullptr, nullptr);
        });
    futures->setKeyedStateRawFuture(task);

    OperatorSnapshotFinalizer finalizer(futures);

    ASSERT_NE(finalizer.getJobManagerOwnedState(), nullptr);
    ASSERT_NE(finalizer.getTaskLocalState(), nullptr);

    delete futures;
}

// ============================================================================
// Test: Both keyed state futures non-null — covers both "if" branches
// ============================================================================
TEST(OperatorSnapshotFinalizerTest, BothKeyedFuturesNonNull)
{
    auto *futures = new OperatorSnapshotFutures();

    auto keyedManaged = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<KeyedStateHandle>> {
            return std::make_shared<SnapshotResult<KeyedStateHandle>>(nullptr, nullptr);
        });
    auto keyedRaw = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<KeyedStateHandle>> {
            return std::make_shared<SnapshotResult<KeyedStateHandle>>(nullptr, nullptr);
        });

    futures->setKeyedStateManagedFuture(keyedManaged);
    futures->setKeyedStateRawFuture(keyedRaw);

    OperatorSnapshotFinalizer finalizer(futures);

    ASSERT_NE(finalizer.getJobManagerOwnedState(), nullptr);
    ASSERT_NE(finalizer.getTaskLocalState(), nullptr);

    delete futures;
}

// ============================================================================
// Test: OperatorSnapshotFutures semaphore operations
// ============================================================================
TEST(OperatorSnapshotFinalizerTest, FuturesSemaphoreOps)
{
    auto *futures = new OperatorSnapshotFutures();

    // Test semaphore init/post/wait lifecycle
    futures->OperatorSemInit();
    futures->OperatorSemPost();
    futures->OperatorSemWait();

    delete futures;
}

// ============================================================================
// Test: OperatorSnapshotFutures cancel returns pair
// ============================================================================
TEST(OperatorSnapshotFinalizerTest, FuturesCancel)
{
    auto *futures = new OperatorSnapshotFutures();

    auto result = futures->cancel();
    EXPECT_EQ(result.first, 0);
    EXPECT_EQ(result.second, 0);

    delete futures;
}

// ============================================================================
// Paths that may not be fully coverable:
//
// 1. operatorStateManaged / operatorStateRaw "if" branches:
//    Build env has different type signature for setOperatorState* methods
//    than the repo, cannot portably set these futures in test code.
//    The "else" branches are covered by the AllNullFutures test.
//
// 2. InputChannelState / ResultSubpartitionState futures:
//    These require real handles with non-null snapshots.
//    The "== nullptr" branches are covered by AllNullFutures.
//
// 3. SnapshotResult with real KeyedStateHandle data:
//    GetJobManagerOwnedSnapshot/GetTaskLocalSnapshot return nullptr
//    from SnapshotResult(nullptr, nullptr), so the SingletonOrEmpty
//    receives nullptr in the "if" branch.
// ============================================================================
