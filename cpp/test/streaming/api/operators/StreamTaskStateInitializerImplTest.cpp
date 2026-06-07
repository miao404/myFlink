/*
 * Unit tests for StreamTaskStateInitializerImpl and StreamOperatorStateContextImpl
 */
#include <gtest/gtest.h>
#include "streaming/api/operators/StreamTaskStateInitializerImpl.h"

// ===========================================================================
// StreamOperatorStateContextImpl tests
// ===========================================================================

TEST(StreamOperatorStateContextImplTest, ConstructWithNullptrs) {
    StreamOperatorStateContextImpl<void*> ctx(std::nullopt, nullptr, nullptr, nullptr);
    EXPECT_EQ(ctx.keyedStateBackend(), nullptr);
    EXPECT_EQ(ctx.operatorStateBackend(), nullptr);
    EXPECT_EQ(ctx.getInternalTimeServiceManager(), nullptr);
    EXPECT_FALSE(ctx.getRestoredCheckpointId().has_value());
}

TEST(StreamOperatorStateContextImplTest, ConstructWithCheckpointId) {
    uint64_t cpId = 42;
    StreamOperatorStateContextImpl<void*> ctx(cpId, nullptr, nullptr, nullptr);
    ASSERT_TRUE(ctx.getRestoredCheckpointId().has_value());
    EXPECT_EQ(ctx.getRestoredCheckpointId().value(), 42u);
}

TEST(StreamOperatorStateContextImplTest, GettersReturnProvidedPointers) {
    // We cannot easily create real backends without full env, so test the
    // getter/setter wiring with nullptr - the important thing is that the
    // constructor stores and the getters return the same pointers.
    StreamOperatorStateContextImpl<void*> ctx(std::nullopt, nullptr, nullptr, nullptr);
    EXPECT_EQ(ctx.keyedStateBackend(), nullptr);
    EXPECT_EQ(ctx.operatorStateBackend(), nullptr);
    EXPECT_EQ(ctx.getInternalTimeServiceManager(), nullptr);
}

// ===========================================================================
// StreamTaskStateInitializerImpl tests
// ===========================================================================

TEST(StreamTaskStateInitializerImplTest, ConstructWithNullEnv) {
    // Constructing with nullptr env should not crash
    StreamTaskStateInitializerImpl initializer(static_cast<omnistream::EnvironmentV2*>(nullptr));
    EXPECT_EQ(initializer.getEnvironment(), nullptr);
}

/*
 * Interfaces NOT tested and reasons:
 *
 * 1. StreamTaskStateInitializerImpl(StateBackend*, EnvironmentV2*):
 *    Requires a valid StateBackend subclass instance. Testing would need a
 *    full environment setup with proper configuration. The constructor just
 *    stores pointers, so the wiring is trivial.
 *
 * 2. streamOperatorStateContext<K>(...):
 *    This template method requires a fully configured EnvironmentV2 with
 *    TaskConfiguration, TaskStateManager, etc. It calls env->taskConfiguration()
 *    which would segfault with a null env. Creating a valid EnvironmentV2
 *    requires the entire OmniStream runtime (task manager, network stack, etc.)
 *    which cannot be mocked in a unit test without a full integration setup.
 *
 * 3. keyedStatedBackend<K>(...) (3 overloads):
 *    The 4-parameter overload is testable only with a valid TypeSerializer.
 *    The 7-parameter overload depends on env and state backend configuration.
 *    The 4-parameter restore overload requires MetricGroup and OperatorID
 *    along with a proper state backend.
 *    All overloads create real state backends (HeapKeyedStateBackend,
 *    RocksdbKeyedStateBackend, etc.) that require complex initialization.
 *
 * 4. operatorStateBackend(...):
 *    Requires env->getTaskStateManager() to be valid. Returns a
 *    DefaultOperatorStateBackend which needs proper restoration context.
 *
 * 5. getPrioritizedOperatorSubtaskStates():
 *    Directly accesses env->taskConfiguration() and env->getTaskStateManager(),
 *    both of which require a fully initialized runtime environment.
 *
 * 6. collectRawKeyedStateHandles(...):
 *    Accesses env and env->getTaskStateManager(). The null-env path returns
 *    an empty vector, but the function is private (inline) and cannot be
 *    called directly from tests.
 */
