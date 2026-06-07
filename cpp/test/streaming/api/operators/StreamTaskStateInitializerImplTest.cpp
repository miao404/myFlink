/*
 * Unit tests for StreamTaskStateInitializerImpl and StreamOperatorStateContextImpl
 */
#include <gtest/gtest.h>
#include "streaming/api/operators/StreamTaskStateInitializerImpl.h"

// ===========================================================================
// StreamOperatorStateContextImpl tests
// ===========================================================================

TEST(StreamOperatorStateContextImplTest, ConstructWithNullptrs) {
    StreamOperatorStateContextImpl<void*> ctx(nullptr, nullptr);
    EXPECT_EQ(ctx.keyedStateBackend(), nullptr);
    EXPECT_EQ(ctx.getInternalTimeServiceManager(), nullptr);
}

TEST(StreamOperatorStateContextImplTest, KeyedStateBackendReturnsNull) {
    StreamOperatorStateContextImpl<void*> ctx(nullptr, nullptr);
    EXPECT_EQ(ctx.keyedStateBackend(), nullptr);
}

TEST(StreamOperatorStateContextImplTest, TimeServiceManagerReturnsNull) {
    StreamOperatorStateContextImpl<void*> ctx(nullptr, nullptr);
    EXPECT_EQ(ctx.getInternalTimeServiceManager(), nullptr);
}

// ===========================================================================
// StreamTaskStateInitializerImpl tests
// ===========================================================================

TEST(StreamTaskStateInitializerImplTest, ConstructWithNullEnv) {
    StreamTaskStateInitializerImpl initializer(static_cast<omnistream::EnvironmentV2*>(nullptr));
    EXPECT_EQ(initializer.getEnvironment(), nullptr);
}

/*
 * Interfaces NOT tested and reasons:
 *
 * 1. StreamOperatorStateContextImpl with real backends:
 *    Requires creating AbstractKeyedStateBackend and InternalTimeServiceManager
 *    instances which need full runtime env (KeyGroupRange, TypeSerializer, etc.)
 *
 * 2. StreamTaskStateInitializerImpl(StateBackend*, EnvironmentV2*):
 *    Requires a valid StateBackend subclass instance.
 *
 * 3. streamOperatorStateContext<K>(...):
 *    Requires a fully configured EnvironmentV2 with TaskConfiguration,
 *    TaskStateManager, etc. Calling env->taskConfiguration() segfaults with null env.
 *
 * 4. keyedStatedBackend<K>(...) (all overloads):
 *    Requires valid TypeSerializer and potentially state manager for restore.
 *
 * 5. operatorStateBackend(...):
 *    Requires env->getTaskStateManager() to be valid.
 *
 * 6. collectRawKeyedStateHandles(...):
 *    Private/protected method, cannot be called directly from tests.
 */
