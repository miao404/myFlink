/*
 * Unit tests for StreamOperatorStateHandler<K>
 *
 * Build environment note: StreamOperatorStateContextImpl only has 2-arg ctor:
 *   StreamOperatorStateContextImpl(AbstractKeyedStateBackend<K>*, InternalTimeServiceManager<K>*)
 */
#include <gtest/gtest.h>
#include "streaming/api/operators/StreamOperatorStateHandler.h"

// ============================================================================
// Test: Constructor with null keyedStateBackend → keyedStateStore = nullptr
// ============================================================================
TEST(StreamOperatorStateHandlerTest, ConstructorNullBackend)
{
    auto *context = new StreamOperatorStateContextImpl<int64_t>(nullptr, nullptr);
    StreamOperatorStateHandler<int64_t> handler(context);

    EXPECT_EQ(handler.getKeyedStateBackend(), nullptr);
    EXPECT_EQ(handler.getKeyedStateStore(), nullptr);
}

// ============================================================================
// Test: dispose with null backends (should not crash)
// ============================================================================
TEST(StreamOperatorStateHandlerTest, DisposeNullBackends)
{
    auto *context = new StreamOperatorStateContextImpl<int64_t>(nullptr, nullptr);
    StreamOperatorStateHandler<int64_t> handler(context);

    handler.dispose();  // Should not crash with null backends
}

// ============================================================================
// Test: notifyCheckpointComplete with null backend
// (dynamic_cast to RocksdbKeyedStateBackend returns nullptr)
// ============================================================================
TEST(StreamOperatorStateHandlerTest, NotifyCheckpointCompleteNullBackend)
{
    auto *context = new StreamOperatorStateContextImpl<int64_t>(nullptr, nullptr);
    StreamOperatorStateHandler<int64_t> handler(context);

    handler.notifyCheckpointComplete(1L);  // Should not crash
}

// ============================================================================
// Test: Destructor with null backends (covers delete branches)
// ============================================================================
TEST(StreamOperatorStateHandlerTest, DestructorNullBackends)
{
    auto *context = new StreamOperatorStateContextImpl<int64_t>(nullptr, nullptr);
    auto *handler = new StreamOperatorStateHandler<int64_t>(context);

    EXPECT_EQ(handler->getKeyedStateBackend(), nullptr);
    EXPECT_EQ(handler->getKeyedStateStore(), nullptr);

    delete handler;  // Should not crash, tests destructor with null members
}

// ============================================================================
// Paths that may not be fully coverable:
//
// 1. Constructor "if (keyedStateBackend != nullptr)" branch:
//    Requires a real AbstractKeyedStateBackend<K> instance (RocksDB backend),
//    which needs full runtime environment setup.
//
// 2. setCurrentKey / getCurrentKey:
//    Requires non-null keyedStateBackend (would NPE with nullptr).
//
// 3. SnapshotState:
//    Requires CheckpointOptions, CheckpointStreamFactory, OmniTaskBridge,
//    InternalTimeServiceManager — full runtime infrastructure.
//
// 4. notifyCheckpointAborted:
//    Build env may not have this method; dynamic_cast also requires real backend.
//
// 5. initializeOperatorState:
//    Build env may not have StateInitializationContextImpl with matching ctor.
// ============================================================================
