#include <gtest/gtest.h>
#include "streaming/api/operators/OperatorSnapshotFutures.h"

TEST(OperatorSnapshotFuturesTest, DefaultConstruction) {
    OperatorSnapshotFutures futures;
    EXPECT_EQ(futures.getKeyedStateManagedFuture(), nullptr);
    EXPECT_EQ(futures.getKeyedStateRawFuture(), nullptr);
    EXPECT_EQ(futures.getOperatorStateManagedFuture(), nullptr);
    EXPECT_EQ(futures.getOperatorStateRawFuture(), nullptr);
    EXPECT_EQ(futures.getInputChannelStateFuture(), nullptr);
    EXPECT_EQ(futures.getResultSubpartitionStateFuture(), nullptr);
}

TEST(OperatorSnapshotFuturesTest, SetAndGetKeyedStateManagedFuture) {
    OperatorSnapshotFutures futures;
    auto task = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<KeyedStateHandle>> { return nullptr; }
    );
    futures.setKeyedStateManagedFuture(task);
    EXPECT_EQ(futures.getKeyedStateManagedFuture(), task);
}

TEST(OperatorSnapshotFuturesTest, SetAndGetKeyedStateRawFuture) {
    OperatorSnapshotFutures futures;
    auto task = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<KeyedStateHandle>> { return nullptr; }
    );
    futures.setKeyedStateRawFuture(task);
    EXPECT_EQ(futures.getKeyedStateRawFuture(), task);
}

TEST(OperatorSnapshotFuturesTest, SetAndGetOperatorStateManagedFuture) {
    OperatorSnapshotFutures futures;
    auto task = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<OperatorStateHandle>> { return nullptr; }
    );
    futures.setOperatorStateManagedFuture(task);
    EXPECT_EQ(futures.getOperatorStateManagedFuture(), task);
}

TEST(OperatorSnapshotFuturesTest, SetAndGetOperatorStateRawFuture) {
    OperatorSnapshotFutures futures;
    auto task = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<OperatorStateHandle>> { return nullptr; }
    );
    futures.setOperatorStateRawFuture(task);
    EXPECT_EQ(futures.getOperatorStateRawFuture(), task);
}

TEST(OperatorSnapshotFuturesTest, SetAndGetInputChannelStateFuture) {
    OperatorSnapshotFutures futures;
    auto task = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>()>>(
        []() -> std::shared_ptr<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>> { return nullptr; }
    );
    futures.setInputChannelStateFuture(task);
    EXPECT_EQ(futures.getInputChannelStateFuture(), task);
}

TEST(OperatorSnapshotFuturesTest, SetAndGetResultSubpartitionStateFuture) {
    OperatorSnapshotFutures futures;
    auto task = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>()>>(
        []() -> std::shared_ptr<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>> { return nullptr; }
    );
    futures.setResultSubpartitionStateFuture(task);
    EXPECT_EQ(futures.getResultSubpartitionStateFuture(), task);
}

TEST(OperatorSnapshotFuturesTest, Cancel) {
    OperatorSnapshotFutures futures;
    auto result = futures.cancel();
    EXPECT_EQ(result.first, 0);
    EXPECT_EQ(result.second, 0);
}

TEST(OperatorSnapshotFuturesTest, SemaphoreNoWait) {
    OperatorSnapshotFutures futures;
    // With waitcount=0, OperatorSemWait should be a no-op
    futures.OperatorSemWait();
    SUCCEED();
}

TEST(OperatorSnapshotFuturesTest, SemaphoreInitPostWait) {
    OperatorSnapshotFutures futures;
    futures.OperatorSemInit();
    futures.OperatorSemPost();
    futures.OperatorSemWait();
    SUCCEED();
}

TEST(OperatorSnapshotFuturesTest, FullConstruction) {
    auto keyedManaged = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<KeyedStateHandle>> { return nullptr; }
    );
    auto keyedRaw = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<KeyedStateHandle>> { return nullptr; }
    );
    auto opManaged = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<OperatorStateHandle>> { return nullptr; }
    );
    auto opRaw = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>(
        []() -> std::shared_ptr<SnapshotResult<OperatorStateHandle>> { return nullptr; }
    );
    auto inputChannel = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>()>>(
        []() -> std::shared_ptr<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>> { return nullptr; }
    );
    auto resultSub = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>()>>(
        []() -> std::shared_ptr<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>> { return nullptr; }
    );

    OperatorSnapshotFutures futures(keyedManaged, keyedRaw, opManaged, opRaw, inputChannel, resultSub);

    EXPECT_EQ(futures.getKeyedStateManagedFuture(), keyedManaged);
    EXPECT_EQ(futures.getKeyedStateRawFuture(), keyedRaw);
    EXPECT_EQ(futures.getOperatorStateManagedFuture(), opManaged);
    EXPECT_EQ(futures.getOperatorStateRawFuture(), opRaw);
    EXPECT_EQ(futures.getInputChannelStateFuture(), inputChannel);
    EXPECT_EQ(futures.getResultSubpartitionStateFuture(), resultSub);
}
