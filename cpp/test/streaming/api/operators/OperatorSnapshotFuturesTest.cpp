#include <gtest/gtest.h>
#include "streaming/api/operators/OperatorSnapshotFutures.h"

// Derive types from the class getters to handle header variations
using KeyedManagedFutureType = decltype(std::declval<OperatorSnapshotFutures>().getKeyedStateManagedFuture());
using KeyedRawFutureType = decltype(std::declval<OperatorSnapshotFutures>().getKeyedStateRawFuture());
using OpManagedFutureType = decltype(std::declval<OperatorSnapshotFutures>().getOperatorStateManagedFuture());
using OpRawFutureType = decltype(std::declval<OperatorSnapshotFutures>().getOperatorStateRawFuture());
using InputChannelFutureType = decltype(std::declval<OperatorSnapshotFutures>().getInputChannelStateFuture());
using ResultSubFutureType = decltype(std::declval<OperatorSnapshotFutures>().getResultSubpartitionStateFuture());

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
    KeyedManagedFutureType task(new typename KeyedManagedFutureType::element_type());
    futures.setKeyedStateManagedFuture(task);
    EXPECT_EQ(futures.getKeyedStateManagedFuture(), task);
}

TEST(OperatorSnapshotFuturesTest, SetAndGetKeyedStateRawFuture) {
    OperatorSnapshotFutures futures;
    KeyedRawFutureType task(new typename KeyedRawFutureType::element_type());
    futures.setKeyedStateRawFuture(task);
    EXPECT_EQ(futures.getKeyedStateRawFuture(), task);
}

TEST(OperatorSnapshotFuturesTest, SetAndGetOperatorStateManagedFuture) {
    OperatorSnapshotFutures futures;
    OpManagedFutureType task(new typename OpManagedFutureType::element_type());
    futures.setOperatorStateManagedFuture(task);
    EXPECT_EQ(futures.getOperatorStateManagedFuture(), task);
}

TEST(OperatorSnapshotFuturesTest, SetAndGetOperatorStateRawFuture) {
    OperatorSnapshotFutures futures;
    OpRawFutureType task(new typename OpRawFutureType::element_type());
    futures.setOperatorStateRawFuture(task);
    EXPECT_EQ(futures.getOperatorStateRawFuture(), task);
}

TEST(OperatorSnapshotFuturesTest, SetAndGetInputChannelStateFuture) {
    OperatorSnapshotFutures futures;
    InputChannelFutureType task(new typename InputChannelFutureType::element_type());
    futures.setInputChannelStateFuture(task);
    EXPECT_EQ(futures.getInputChannelStateFuture(), task);
}

TEST(OperatorSnapshotFuturesTest, SetAndGetResultSubpartitionStateFuture) {
    OperatorSnapshotFutures futures;
    ResultSubFutureType task(new typename ResultSubFutureType::element_type());
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
