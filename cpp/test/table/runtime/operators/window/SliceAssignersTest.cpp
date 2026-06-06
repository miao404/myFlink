#include <gtest/gtest.h>
#include "table/runtime/operators/window/slicing/SliceAssigners.h"

// --- TimeWindow1 Tests ---

TEST(TimeWindow1Test, GetWindowStartWithOffsetBasic) {
    EXPECT_EQ(TimeWindow1::getWindowStartWithOffset(150, 0, 100), 100);
    EXPECT_EQ(TimeWindow1::getWindowStartWithOffset(100, 0, 100), 100);
    EXPECT_EQ(TimeWindow1::getWindowStartWithOffset(99, 0, 100), 0);
    EXPECT_EQ(TimeWindow1::getWindowStartWithOffset(0, 0, 100), 0);
}

TEST(TimeWindow1Test, GetWindowStartWithOffsetWithOffset) {
    EXPECT_EQ(TimeWindow1::getWindowStartWithOffset(50, 10, 100), 10);
    EXPECT_EQ(TimeWindow1::getWindowStartWithOffset(110, 10, 100), 110);
}

TEST(TimeWindow1Test, GetWindowStartWithNegativeTimestamp) {
    EXPECT_EQ(TimeWindow1::getWindowStartWithOffset(-1, 0, 100), -100);
    EXPECT_EQ(TimeWindow1::getWindowStartWithOffset(-50, 0, 100), -100);
}

// --- ReusableListIterable Tests ---

TEST(ReusableListIterableTest, ResetSingleValue) {
    ReusableListIterable list;
    list.reset(42);
    EXPECT_TRUE(list.hasNext());
    EXPECT_EQ(list.next(), 42);
    EXPECT_FALSE(list.hasNext());
}

TEST(ReusableListIterableTest, ResetTwoValues) {
    ReusableListIterable list;
    list.reset(10, 20);
    EXPECT_TRUE(list.hasNext());
    EXPECT_EQ(list.next(), 10);
    EXPECT_TRUE(list.hasNext());
    EXPECT_EQ(list.next(), 20);
    EXPECT_FALSE(list.hasNext());
}

TEST(ReusableListIterableTest, Clear) {
    ReusableListIterable list;
    list.reset(10, 20);
    list.clear();
    EXPECT_FALSE(list.hasNext());
}

TEST(ReusableListIterableTest, GetList) {
    ReusableListIterable list;
    list.reset(10, 20);
    auto values = list.getList();
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values[0], 10);
    EXPECT_EQ(values[1], 20);
}

// --- HoppingSlicesIterable Tests ---

TEST(HoppingSlicesIterableTest, BasicIteration) {
    HoppingSlicesIterable iter(1000, 100, 3);
    EXPECT_TRUE(iter.hasNext());
    auto v1 = iter.next();
    EXPECT_EQ(v1, 1000);
    EXPECT_TRUE(iter.hasNext());
    auto v2 = iter.next();
    EXPECT_EQ(v2, 900);
    EXPECT_TRUE(iter.hasNext());
    auto v3 = iter.next();
    EXPECT_EQ(v3, 800);
    EXPECT_FALSE(iter.hasNext());
}

TEST(HoppingSlicesIterableTest, SingleSlice) {
    HoppingSlicesIterable iter(500, 100, 1);
    EXPECT_TRUE(iter.hasNext());
    EXPECT_EQ(iter.next(), 500);
    EXPECT_FALSE(iter.hasNext());
}

// --- TumblingSliceAssigner Tests ---

TEST(TumblingSliceAssignerTest, AssignSliceEnd) {
    omnistream::ZoneId tz;
    TumblingSliceAssigner assigner(0, &tz, 1000, 0);

    EXPECT_EQ(assigner.assignSliceEnd(0), 1000);
    EXPECT_EQ(assigner.assignSliceEnd(500), 1000);
    EXPECT_EQ(assigner.assignSliceEnd(999), 1000);
    EXPECT_EQ(assigner.assignSliceEnd(1000), 2000);
}

TEST(TumblingSliceAssignerTest, GetWindowStart) {
    omnistream::ZoneId tz;
    TumblingSliceAssigner assigner(0, &tz, 1000, 0);

    EXPECT_EQ(assigner.getWindowStart(1000), 0);
    EXPECT_EQ(assigner.getWindowStart(2000), 1000);
}

TEST(TumblingSliceAssignerTest, GetLastWindowEnd) {
    omnistream::ZoneId tz;
    TumblingSliceAssigner assigner(0, &tz, 1000, 0);

    EXPECT_EQ(assigner.getLastWindowEnd(1000), 1000);
    EXPECT_EQ(assigner.getLastWindowEnd(2000), 2000);
}

TEST(TumblingSliceAssignerTest, GetSliceEndInterval) {
    omnistream::ZoneId tz;
    TumblingSliceAssigner assigner(0, &tz, 500, 0);

    EXPECT_EQ(assigner.getSliceEndInterval(), 500);
}

TEST(TumblingSliceAssignerTest, IsEventTime) {
    omnistream::ZoneId tz;
    TumblingSliceAssigner eventTimeAssigner(0, &tz, 1000, 0);
    EXPECT_TRUE(eventTimeAssigner.isEventTime());

    TumblingSliceAssigner processingTimeAssigner(-1, &tz, 1000, 0);
    EXPECT_FALSE(processingTimeAssigner.isEventTime());
}

TEST(TumblingSliceAssignerTest, WithOffset) {
    omnistream::ZoneId tz;
    TumblingSliceAssigner assigner(0, &tz, 1000, 0);
    auto* withOffset = assigner.withOffset(100);
    ASSERT_NE(withOffset, nullptr);
    EXPECT_EQ(withOffset->assignSliceEnd(100), 1100);
    delete withOffset;
}

TEST(TumblingSliceAssignerTest, ExpiredSlices) {
    omnistream::ZoneId tz;
    TumblingSliceAssigner assigner(0, &tz, 1000, 0);
    auto* expired = assigner.expiredSlices(1000);
    ASSERT_NE(expired, nullptr);
    EXPECT_TRUE(expired->hasNext());
    EXPECT_EQ(expired->next(), 1000);
    EXPECT_FALSE(expired->hasNext());
}

TEST(TumblingSliceAssignerTest, InvalidSizeThrows) {
    omnistream::ZoneId tz;
    EXPECT_THROW(TumblingSliceAssigner(0, &tz, 0, 0), std::invalid_argument);
    EXPECT_THROW(TumblingSliceAssigner(0, &tz, -100, 0), std::invalid_argument);
}

TEST(TumblingSliceAssignerTest, InvalidOffsetThrows) {
    omnistream::ZoneId tz;
    EXPECT_THROW(TumblingSliceAssigner(0, &tz, 100, 100), std::invalid_argument);
    EXPECT_THROW(TumblingSliceAssigner(0, &tz, 100, -100), std::invalid_argument);
}

// --- ClockService Tests ---

TEST(ClockServiceTest, CurrentProcessingTimeReturns) {
    ClockService clock;
    auto now = clock.currentProcessingTime();
    auto epoch = now.time_since_epoch().count();
    EXPECT_GT(epoch, 0);
}
