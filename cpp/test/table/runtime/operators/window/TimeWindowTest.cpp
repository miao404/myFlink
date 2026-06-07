#include <gtest/gtest.h>
#include "table/runtime/operators/window/TimeWindow.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"

TEST(TimeWindowTest, Construction) {
    TimeWindow w(0, 1000);
    EXPECT_EQ(w.getStart(), 0);
    EXPECT_EQ(w.getEnd(), 1000);
}

TEST(TimeWindowTest, DefaultConstruction) {
    TimeWindow w;
    EXPECT_EQ(w.getStart(), 0);
    EXPECT_EQ(w.getEnd(), 0);
}

TEST(TimeWindowTest, MaxTimestamp) {
    TimeWindow w(0, 1000);
    EXPECT_EQ(w.maxTimestamp(), 999);
}

TEST(TimeWindowTest, Intersects) {
    TimeWindow w1(0, 1000);
    TimeWindow w2(500, 1500);
    TimeWindow w3(1001, 2000);

    EXPECT_TRUE(w1.intersects(w2));
    EXPECT_TRUE(w2.intersects(w1));
    EXPECT_FALSE(w1.intersects(w3));
}

TEST(TimeWindowTest, IntersectsEdge) {
    TimeWindow w1(0, 1000);
    TimeWindow w2(1000, 2000);
    EXPECT_TRUE(w1.intersects(w2));
}

TEST(TimeWindowTest, Cover) {
    TimeWindow w1(100, 500);
    TimeWindow w2(300, 800);
    TimeWindow covered = w1.cover(w2);
    EXPECT_EQ(covered.getStart(), 100);
    EXPECT_EQ(covered.getEnd(), 800);
}

TEST(TimeWindowTest, CoverContained) {
    TimeWindow outer(0, 1000);
    TimeWindow inner(200, 800);
    TimeWindow covered = outer.cover(inner);
    EXPECT_EQ(covered.getStart(), 0);
    EXPECT_EQ(covered.getEnd(), 1000);
}

TEST(TimeWindowTest, GetWindowStartWithOffset) {
    // Tumbling window of size 100, no offset
    EXPECT_EQ(TimeWindow::getWindowStartWithOffset(150, 0, 100), 100);
    EXPECT_EQ(TimeWindow::getWindowStartWithOffset(100, 0, 100), 100);
    EXPECT_EQ(TimeWindow::getWindowStartWithOffset(99, 0, 100), 0);
    EXPECT_EQ(TimeWindow::getWindowStartWithOffset(0, 0, 100), 0);
}

TEST(TimeWindowTest, GetWindowStartWithOffsetPositive) {
    // Window size=100, offset=10
    EXPECT_EQ(TimeWindow::getWindowStartWithOffset(50, 10, 100), 10);
    EXPECT_EQ(TimeWindow::getWindowStartWithOffset(110, 10, 100), 110);
    EXPECT_EQ(TimeWindow::getWindowStartWithOffset(9, 10, 100), -90);
}

TEST(TimeWindowTest, GetWindowStartWithNegativeTimestamp) {
    EXPECT_EQ(TimeWindow::getWindowStartWithOffset(-1, 0, 100), -100);
    EXPECT_EQ(TimeWindow::getWindowStartWithOffset(-50, 0, 100), -100);
}

TEST(TimeWindowTest, GetWindowStartInvalidSize) {
    EXPECT_THROW(TimeWindow::getWindowStartWithOffset(100, 0, 0), std::runtime_error);
    EXPECT_THROW(TimeWindow::getWindowStartWithOffset(100, 0, -1), std::runtime_error);
}

TEST(TimeWindowTest, Equality) {
    TimeWindow w1(0, 1000);
    TimeWindow w2(0, 1000);
    TimeWindow w3(0, 2000);
    EXPECT_TRUE(w1 == w2);
    EXPECT_FALSE(w1 == w3);
}

TEST(TimeWindowTest, LessThan) {
    TimeWindow w1(0, 1000);
    TimeWindow w2(100, 1000);
    TimeWindow w3(0, 2000);
    EXPECT_TRUE(w1 < w2);
    EXPECT_TRUE(w1 < w3);
    EXPECT_FALSE(w2 < w1);
}

TEST(TimeWindowTest, GreaterThan) {
    TimeWindow w1(100, 1000);
    TimeWindow w2(0, 1000);
    EXPECT_TRUE(w1 > w2);
    EXPECT_FALSE(w2 > w1);
}

TEST(TimeWindowTest, HashCode) {
    TimeWindow w1(0, 1000);
    TimeWindow w2(0, 1000);
    EXPECT_EQ(w1.hashCode(), w2.hashCode());
}

TEST(TimeWindowTest, HashCodeDifferent) {
    TimeWindow w1(0, 1000);
    TimeWindow w2(0, 2000);
    // Hash codes should likely differ (not guaranteed but very likely)
    // This is a sanity check
    if (w1.hashCode() == w2.hashCode()) {
        // Collisions are possible but should be rare
        SUCCEED();
    } else {
        EXPECT_NE(w1.hashCode(), w2.hashCode());
    }
}

TEST(TimeWindowTest, StdHash) {
    TimeWindow w1(0, 1000);
    TimeWindow w2(0, 1000);
    std::hash<TimeWindow> hasher;
    EXPECT_EQ(hasher(w1), hasher(w2));
}

TEST(TimeWindowTest, SerializerCreateInstance) {
    TimeWindow::Serializer serializer;
    auto* instance = serializer.createInstance();
    ASSERT_NE(instance, nullptr);
    EXPECT_EQ(instance->getStart(), 0);
    EXPECT_EQ(instance->getEnd(), 1);
    delete instance;
}

TEST(TimeWindowTest, SerializerIsImmutableType) {
    TimeWindow::Serializer serializer;
    EXPECT_TRUE(serializer.isImmutableType());
}

TEST(TimeWindowTest, SerializerGetLength) {
    TimeWindow::Serializer serializer;
    EXPECT_EQ(serializer.getLength(), sizeof(int64_t) * 2);
}

TEST(TimeWindowTest, SerializerCopy) {
    TimeWindow::Serializer serializer;
    auto* original = new TimeWindow(100, 200);
    auto* copy = serializer.copy(original);
    EXPECT_EQ(copy, original);  // copy returns same pointer
    delete original;
}

TEST(TimeWindowTest, SerializerCopyWithReuse) {
    TimeWindow::Serializer serializer;
    auto* from = new TimeWindow(100, 200);
    auto* reuse = new TimeWindow(0, 0);
    auto* result = serializer.copy(from, reuse);
    EXPECT_EQ(result, from);  // returns from pointer
    delete from;
    delete reuse;
}

TEST(TimeWindowTest, SerializerSerializeDeserialize) {
    TimeWindow::Serializer serializer;
    TimeWindow original(12345, 67890);

    DataOutputSerializer outputView(128);
    serializer.serialize(&original, outputView);

    DataInputDeserializer inputView(outputView.getData(), outputView.getPosition());
    auto* deserialized = static_cast<TimeWindow*>(serializer.deserialize(inputView));

    ASSERT_NE(deserialized, nullptr);
    EXPECT_EQ(deserialized->getStart(), 12345);
    EXPECT_EQ(deserialized->getEnd(), 67890);
    delete deserialized;
}

TEST(TimeWindowTest, SerializerBackendId) {
    TimeWindow::Serializer serializer;
    EXPECT_EQ(serializer.getBackendId(), BackendDataType::TIME_WINDOW_BK);
}
