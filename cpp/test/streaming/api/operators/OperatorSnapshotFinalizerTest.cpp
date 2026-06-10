/*
 * Unit tests for OperatorSnapshotFinalizer
 * Covers constructor branches (null vs non-null futures) and accessor methods.
 */
#include <gtest/gtest.h>
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
// Test: OperatorStateManaged future is set — covers "if (operatorStateManaged)" branch
// ============================================================================
TEST(OperatorSnapshotFinalizerTest, WithOperatorStateManagedFuture)
{
    auto *futures = new OperatorSnapshotFutures();

    auto task = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<OperatorStateHandle>> {
            return std::make_shared<SnapshotResult<OperatorStateHandle>>(nullptr, nullptr);
        });
    futures->setOperatorStateManagedFuture(task);

    OperatorSnapshotFinalizer finalizer(futures);

    ASSERT_NE(finalizer.getJobManagerOwnedState(), nullptr);
    ASSERT_NE(finalizer.getTaskLocalState(), nullptr);

    delete futures;
}

// ============================================================================
// Test: OperatorStateRaw future is set — covers "if (operatorStateRaw)" branch
// ============================================================================
TEST(OperatorSnapshotFinalizerTest, WithOperatorStateRawFuture)
{
    auto *futures = new OperatorSnapshotFutures();

    auto task = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<OperatorStateHandle>> {
            return std::make_shared<SnapshotResult<OperatorStateHandle>>(nullptr, nullptr);
        });
    futures->setOperatorStateRawFuture(task);

    OperatorSnapshotFinalizer finalizer(futures);

    ASSERT_NE(finalizer.getJobManagerOwnedState(), nullptr);
    ASSERT_NE(finalizer.getTaskLocalState(), nullptr);

    delete futures;
}

// ============================================================================
// Test: All futures non-null — covers all "if" branches simultaneously
// ============================================================================
TEST(OperatorSnapshotFinalizerTest, AllFuturesNonNull)
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
    auto opManaged = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<OperatorStateHandle>> {
            return std::make_shared<SnapshotResult<OperatorStateHandle>>(nullptr, nullptr);
        });
    auto opRaw = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<OperatorStateHandle>> {
            return std::make_shared<SnapshotResult<OperatorStateHandle>>(nullptr, nullptr);
        });

    futures->setKeyedStateManagedFuture(keyedManaged);
    futures->setKeyedStateRawFuture(keyedRaw);
    futures->setOperatorStateManagedFuture(opManaged);
    futures->setOperatorStateRawFuture(opRaw);

    OperatorSnapshotFinalizer finalizer(futures);

    ASSERT_NE(finalizer.getJobManagerOwnedState(), nullptr);
    ASSERT_NE(finalizer.getTaskLocalState(), nullptr);

    delete futures;
}

// ============================================================================
// Paths that may not be fully coverable:
//
// 1. InputChannelState / ResultSubpartitionState futures:
//    These require real InputChannelStateHandle / ResultSubpartitionStateHandle
//    snapshot results with non-null JobManagerOwnedSnapshot. The current test
//    covers the "== nullptr" branches for these.
//
// 2. SnapshotResult with real KeyedStateHandle/OperatorStateHandle data:
//    The "if" branches are covered with SnapshotResult(nullptr, nullptr) which
//    still exercises the branch but GetJobManagerOwnedSnapshot returns nullptr.
// ============================================================================
