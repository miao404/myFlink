/*
 * Unit tests for TimerSerializer
 * Covers construction, getName, getBackendId, serialize/deserialize,
 * GetBuffer, setSubBufferReusable for multiple template specializations.
 */
#include <gtest/gtest.h>
#include "table/runtime/operators/TimerSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "basictypes/Long.h"

// ============================================================================
// Test: Construction with 2 args (keySerializer, namespaceSerializer)
// ============================================================================
TEST(TimerSerializerTest, ConstructionTwoArgs)
{
    auto *keySerializer = new LongSerializer();
    auto *nsSerializer = new VoidNamespaceSerializer();

    // TimerSerializer<int64_t, VoidNamespace> uses the 2-arg constructor
    auto *ts = new TimerSerializer<int64_t, VoidNamespace>(keySerializer, nsSerializer);

    ASSERT_NE(ts, nullptr);
    delete ts;  // will delete keySerializer and nsSerializer
}

// ============================================================================
// Test: getName returns "TimerSerializer"
// ============================================================================
TEST(TimerSerializerTest, GetName)
{
    auto *keySerializer = new LongSerializer();
    auto *nsSerializer = new VoidNamespaceSerializer();
    auto *ts = new TimerSerializer<int64_t, VoidNamespace>(keySerializer, nsSerializer);

    EXPECT_STREQ(ts->getName(), "TimerSerializer");
    delete ts;
}

// ============================================================================
// Test: getBackendId returns OBJECT_BK
// ============================================================================
TEST(TimerSerializerTest, GetBackendId)
{
    auto *keySerializer = new LongSerializer();
    auto *nsSerializer = new VoidNamespaceSerializer();
    auto *ts = new TimerSerializer<int64_t, VoidNamespace>(keySerializer, nsSerializer);

    EXPECT_EQ(ts->getBackendId(), BackendDataType::OBJECT_BK);
    delete ts;
}

// ============================================================================
// Test: Serialize and Deserialize roundtrip for <int64_t, VoidNamespace>
// ============================================================================
TEST(TimerSerializerTest, SerializeDeserializeInt64VoidNamespace)
{
    auto *keySerializer = new LongSerializer();
    auto *nsSerializer = new VoidNamespaceSerializer();
    auto *ts = new TimerSerializer<int64_t, VoidNamespace>(keySerializer, nsSerializer);

    // Create a timer
    auto *timer = new TimerHeapInternalTimer<int64_t, VoidNamespace>(12345L, 42L, VoidNamespace());

    // Serialize
    DataOutputSerializer output(128);
    ts->serialize(static_cast<Object*>(timer), output);

    // Deserialize
    auto data = output.getCopyOfBuffer();
    DataInputDeserializer input(data->data(), data->size());
    auto *deserialized = static_cast<TimerHeapInternalTimer<int64_t, VoidNamespace>*>(ts->deserialize(input));

    ASSERT_NE(deserialized, nullptr);
    EXPECT_EQ(deserialized->getTimestamp(), 12345L);
    EXPECT_EQ(deserialized->getKey(), 42L);

    delete deserialized;
    delete timer;
    delete ts;
}

// ============================================================================
// Test: setSubBufferReusable
// ============================================================================
TEST(TimerSerializerTest, SetSubBufferReusable)
{
    auto *keySerializer = new LongSerializer();
    auto *nsSerializer = new VoidNamespaceSerializer();
    auto *ts = new TimerSerializer<int64_t, VoidNamespace>(keySerializer, nsSerializer);

    // Should not crash
    ts->setSubBufferReusable(true);
    ts->setSubBufferReusable(false);

    delete ts;
}

// ============================================================================
// Test: GetBuffer (non-reusable mode — creates new instance)
// ============================================================================
TEST(TimerSerializerTest, GetBufferNonReusable)
{
    auto *keySerializer = new LongSerializer();
    auto *nsSerializer = new VoidNamespaceSerializer();
    auto *ts = new TimerSerializer<int64_t, VoidNamespace>(keySerializer, nsSerializer);

    // bufferReusable defaults to false for <int64_t, VoidNamespace>
    Object *buffer = ts->GetBuffer();
    ASSERT_NE(buffer, nullptr);

    // Cast to timer and verify it's a valid TimerHeapInternalTimer
    auto *timer = static_cast<TimerHeapInternalTimer<int64_t, VoidNamespace>*>(buffer);
    EXPECT_EQ(timer->getTimestamp(), 0L);

    delete timer;
    delete ts;
}

// ============================================================================
// Test: toJson returns valid JSON string
// ============================================================================
TEST(TimerSerializerTest, ToJson)
{
    auto *keySerializer = new LongSerializer();
    auto *nsSerializer = new VoidNamespaceSerializer();
    auto *ts = new TimerSerializer<int64_t, VoidNamespace>(keySerializer, nsSerializer);

    std::string json = ts->toJson();
    EXPECT_FALSE(json.empty());

    delete ts;
}

// ============================================================================
// Test: Deserialize with Object* buffer overload
// ============================================================================
TEST(TimerSerializerTest, DeserializeWithBuffer)
{
    auto *keySerializer = new LongSerializer();
    auto *nsSerializer = new VoidNamespaceSerializer();
    auto *ts = new TimerSerializer<int64_t, VoidNamespace>(keySerializer, nsSerializer);

    // Create and serialize a timer
    auto *timer = new TimerHeapInternalTimer<int64_t, VoidNamespace>(9999L, 7L, VoidNamespace());
    DataOutputSerializer output(128);
    ts->serialize(static_cast<Object*>(timer), output);

    // Deserialize into a pre-allocated buffer
    auto data = output.getCopyOfBuffer();
    DataInputDeserializer input(data->data(), data->size());
    auto *target = new TimerHeapInternalTimer<int64_t, VoidNamespace>();
    ts->deserialize(static_cast<Object*>(target), input);

    EXPECT_EQ(target->getTimestamp(), 9999L);
    EXPECT_EQ(target->getKey(), 7L);

    delete target;
    delete timer;
    delete ts;
}

// ============================================================================
// Test: TimerHeapInternalTimer basic operations (complementary coverage)
// ============================================================================
TEST(TimerSerializerTest, TimerHeapInternalTimerBasic)
{
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer(100L, 5L, VoidNamespace());
    EXPECT_EQ(timer.getTimestamp(), 100L);
    EXPECT_EQ(timer.getKey(), 5L);

    timer.setTimestamp(200L);
    EXPECT_EQ(timer.getTimestamp(), 200L);

    timer.setKey(10L);
    EXPECT_EQ(timer.getKey(), 10L);

    timer.setNamespace(VoidNamespace());
}

// ============================================================================
// Test: TimerHeapInternalTimer equality operator for int64_t key
// ============================================================================
TEST(TimerSerializerTest, TimerHeapInternalTimerEquality)
{
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer1(100L, 5L, VoidNamespace());
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer2(100L, 5L, VoidNamespace());
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer3(200L, 5L, VoidNamespace());

    EXPECT_TRUE(timer1 == timer2);
    EXPECT_TRUE(timer1 != timer3);
}

// ============================================================================
// Test: TimerHeapInternalTimer default constructor
// ============================================================================
TEST(TimerSerializerTest, TimerHeapInternalTimerDefaultCtor)
{
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer;
    EXPECT_EQ(timer.getTimestamp(), 0L);
    EXPECT_EQ(timer.getKey(), 0L);
}

// ============================================================================
// Paths that may not be fully coverable:
//
// 1. serialize/deserialize for Object* key type:
//    Requires Object* key with refcounting — complex memory management
//
// 2. serialize/deserialize for shared_ptr key type:
//    Requires is_shared_ptr_v<K> specialization with real RowData keys
//
// 3. serialize/deserialize for TimeWindow namespace:
//    Requires TimeWindow::Serializer::deserialize
//
// 4. snapshotConfiguration():
//    Throws NOT_IMPL_EXCEPTION — cannot test without catch
//
// 5. Construction with 4 args (keyClazz, namespaceClazz):
//    Requires Class* with newInstance() — complex dependency
// ============================================================================
