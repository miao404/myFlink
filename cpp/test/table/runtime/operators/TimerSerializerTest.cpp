/*
 * Unit tests for TimerSerializer and TimerHeapInternalTimer
 *
 * Build environment note: TimerSerializer is a non-template class inheriting
 * from TypeSerializerSingleton, with only the 4-arg constructor:
 *   TimerSerializer(TypeSerializer*, TypeSerializer*, Class*, Class*)
 */
#include <gtest/gtest.h>
#include "table/runtime/operators/TimerSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "basictypes/Long.h"
#include "basictypes/Class.h"

// ============================================================================
// Helper: create Class* for Long (int64_t key)
// ============================================================================
static Class* makeLongClass()
{
    return new Class([]() -> Object* { return new Long(0L); });
}

// ============================================================================
// Helper: create Class* for VoidNamespace
// ============================================================================
static Class* makeVoidNamespaceClass()
{
    return new Class([]() -> Object* { return new VoidNamespace(); });
}

// ============================================================================
// Test: Construction with 4 args
// ============================================================================
TEST(TimerSerializerTest, Construction)
{
    auto *ts = new TimerSerializer(
        new LongSerializer(), new VoidNamespaceSerializer(),
        makeLongClass(), makeVoidNamespaceClass());

    ASSERT_NE(ts, nullptr);
    delete ts;
}

// ============================================================================
// Test: getName returns "TimerSerializer"
// ============================================================================
TEST(TimerSerializerTest, GetName)
{
    auto *ts = new TimerSerializer(
        new LongSerializer(), new VoidNamespaceSerializer(),
        makeLongClass(), makeVoidNamespaceClass());

    EXPECT_STREQ(ts->getName(), "TimerSerializer");
    delete ts;
}

// ============================================================================
// Test: getBackendId returns OBJECT_BK
// ============================================================================
TEST(TimerSerializerTest, GetBackendId)
{
    auto *ts = new TimerSerializer(
        new LongSerializer(), new VoidNamespaceSerializer(),
        makeLongClass(), makeVoidNamespaceClass());

    EXPECT_EQ(ts->getBackendId(), BackendDataType::OBJECT_BK);
    delete ts;
}

// ============================================================================
// Test: Serialize and Deserialize roundtrip
// ============================================================================
TEST(TimerSerializerTest, SerializeDeserializeRoundtrip)
{
    auto *ts = new TimerSerializer(
        new LongSerializer(), new VoidNamespaceSerializer(),
        makeLongClass(), makeVoidNamespaceClass());

    // Create a timer using Object* key/namespace
    auto *keyObj = new Long(42L);
    auto *nsObj = new VoidNamespace();
    auto *timer = new TimerHeapInternalTimer<Object*, Object*>(
        12345L, static_cast<Object*>(keyObj), static_cast<Object*>(nsObj));

    // Serialize
    DataOutputSerializer output(256);
    ts->serialize(static_cast<Object*>(timer), output);

    EXPECT_GT(output.getPosition(), 0);

    // Deserialize
    DataInputDeserializer input(output.getData(), output.getPosition());
    auto *deserialized = static_cast<TimerHeapInternalTimer<Object*, Object*>*>(ts->deserialize(input));

    ASSERT_NE(deserialized, nullptr);
    EXPECT_EQ(deserialized->getTimestamp(), 12345L);

    delete deserialized;
    delete timer;
    delete ts;
}

// ============================================================================
// Test: Deserialize with Object* buffer overload
// ============================================================================
TEST(TimerSerializerTest, DeserializeWithBuffer)
{
    auto *ts = new TimerSerializer(
        new LongSerializer(), new VoidNamespaceSerializer(),
        makeLongClass(), makeVoidNamespaceClass());

    // Create and serialize
    auto *keyObj = new Long(7L);
    auto *nsObj = new VoidNamespace();
    auto *timer = new TimerHeapInternalTimer<Object*, Object*>(
        9999L, static_cast<Object*>(keyObj), static_cast<Object*>(nsObj));

    DataOutputSerializer output(256);
    ts->serialize(static_cast<Object*>(timer), output);

    // Deserialize into pre-allocated buffer
    DataInputDeserializer input(output.getData(), output.getPosition());
    Object *buffer = ts->GetBuffer();
    ASSERT_NE(buffer, nullptr);
    ts->deserialize(buffer, input);

    auto *result = static_cast<TimerHeapInternalTimer<Object*, Object*>*>(buffer);
    EXPECT_EQ(result->getTimestamp(), 9999L);

    result->putRefCount();
    delete timer;
    delete ts;
}

// ============================================================================
// Test: setSubBufferReusable
// ============================================================================
TEST(TimerSerializerTest, SetSubBufferReusable)
{
    auto *ts = new TimerSerializer(
        new LongSerializer(), new VoidNamespaceSerializer(),
        makeLongClass(), makeVoidNamespaceClass());

    ts->setSubBufferReusable(true);
    ts->setSubBufferReusable(false);

    delete ts;
}

// ============================================================================
// Test: GetBuffer creates valid instance
// ============================================================================
TEST(TimerSerializerTest, GetBuffer)
{
    auto *ts = new TimerSerializer(
        new LongSerializer(), new VoidNamespaceSerializer(),
        makeLongClass(), makeVoidNamespaceClass());

    Object *buffer = ts->GetBuffer();
    ASSERT_NE(buffer, nullptr);

    auto *timer = static_cast<TimerHeapInternalTimer<Object*, Object*>*>(buffer);
    EXPECT_EQ(timer->getTimestamp(), 0L);

    timer->putRefCount();
    delete ts;
}

// ============================================================================
// Test: toJson returns non-empty string
// ============================================================================
TEST(TimerSerializerTest, ToJson)
{
    auto *ts = new TimerSerializer(
        new LongSerializer(), new VoidNamespaceSerializer(),
        makeLongClass(), makeVoidNamespaceClass());

    std::string json = ts->toJson();
    EXPECT_FALSE(json.empty());

    delete ts;
}

// ============================================================================
// TimerHeapInternalTimer tests (template class, tested independently)
// ============================================================================

TEST(TimerHeapInternalTimerTest, DefaultConstruction)
{
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer;
    EXPECT_EQ(timer.getTimestamp(), 0L);
    EXPECT_EQ(timer.getKey(), 0L);
}

TEST(TimerHeapInternalTimerTest, ParameterizedConstruction)
{
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer(100L, 5L, VoidNamespace());
    EXPECT_EQ(timer.getTimestamp(), 100L);
    EXPECT_EQ(timer.getKey(), 5L);
}

TEST(TimerHeapInternalTimerTest, SetTimestamp)
{
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer(100L, 5L, VoidNamespace());
    timer.setTimestamp(200L);
    EXPECT_EQ(timer.getTimestamp(), 200L);
}

TEST(TimerHeapInternalTimerTest, SetKey)
{
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer(100L, 5L, VoidNamespace());
    timer.setKey(10L);
    EXPECT_EQ(timer.getKey(), 10L);
}

TEST(TimerHeapInternalTimerTest, SetNamespace)
{
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer(100L, 5L, VoidNamespace());
    timer.setNamespace(VoidNamespace());
}

TEST(TimerHeapInternalTimerTest, EqualityOperator)
{
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer1(100L, 5L, VoidNamespace());
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer2(100L, 5L, VoidNamespace());
    TimerHeapInternalTimer<int64_t, VoidNamespace> timer3(200L, 5L, VoidNamespace());

    EXPECT_TRUE(timer1 == timer2);
    EXPECT_TRUE(timer1 != timer3);
}

TEST(TimerHeapInternalTimerTest, ObjectPtrKeyConstruction)
{
    auto *key = new Long(42L);
    auto *ns = new VoidNamespace();
    auto *timer = new TimerHeapInternalTimer<Object*, Object*>(
        500L, static_cast<Object*>(key), static_cast<Object*>(ns));

    EXPECT_EQ(timer->getTimestamp(), 500L);

    delete timer;
    // timer destructor calls putRefCount on key and ns
}

// ============================================================================
// Paths that may not be fully coverable:
//
// 1. serialize/deserialize for shared_ptr key type:
//    Requires KeySelector<K>::isSharedRowKey_ specialization
//
// 2. serialize/deserialize for int32_t key (Integer):
//    Needs IntegerSerializer which may not be available
//
// 3. deserialize for TimeWindow namespace:
//    Requires TimeWindow::Serializer
//
// 4. snapshotConfiguration():
//    Throws NOT_IMPL_EXCEPTION
//
// 5. GetBuffer with bufferReusable=true path:
//    Requires reuseBuffer to be set (only for Object*/Object* specialization)
// ============================================================================
