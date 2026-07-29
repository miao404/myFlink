#include <gtest/gtest.h>
#include "table/runtime/operators/rank/SetTopNBuffer.h"

struct AscComparator {
    bool operator()(long a, long b) const { return a < b; }
};

struct DescComparator {
    bool operator()(long a, long b) const { return a > b; }
};

TEST(SetTopNBufferTest, AddElementAndGetSize) {
    SetTopNBuffer<AscComparator> buffer(AscComparator{});
    EXPECT_EQ(buffer.GetSize(), 0);
    buffer.AddElement(10);
    EXPECT_EQ(buffer.GetSize(), 1);
    buffer.AddElement(20);
    EXPECT_EQ(buffer.GetSize(), 2);
}

TEST(SetTopNBufferTest, GetSmallestElementAscending) {
    SetTopNBuffer<AscComparator> buffer(AscComparator{});
    buffer.AddElement(10);
    buffer.AddElement(5);
    buffer.AddElement(20);
    // "Smallest" = last element in ascending order = 20
    EXPECT_EQ(buffer.GetSmallestElement(), 20);
}

TEST(SetTopNBufferTest, GetSmallestElementDescending) {
    SetTopNBuffer<DescComparator> buffer(DescComparator{});
    buffer.AddElement(10);
    buffer.AddElement(5);
    buffer.AddElement(20);
    // "Smallest" = last element in descending order = 5
    EXPECT_EQ(buffer.GetSmallestElement(), 5);
}

TEST(SetTopNBufferTest, RemoveSmallestElement) {
    SetTopNBuffer<AscComparator> buffer(AscComparator{});
    buffer.AddElement(10);
    buffer.AddElement(5);
    buffer.AddElement(20);
    buffer.RemoveSmallestElement();
    EXPECT_EQ(buffer.GetSize(), 2);
    EXPECT_EQ(buffer.GetSmallestElement(), 10);
}

TEST(SetTopNBufferTest, RemoveSmallestFromEmpty) {
    SetTopNBuffer<AscComparator> buffer(AscComparator{});
    buffer.RemoveSmallestElement();
    EXPECT_EQ(buffer.GetSize(), 0);
}

TEST(SetTopNBufferTest, ToPlainVector) {
    SetTopNBuffer<AscComparator> buffer(AscComparator{});
    buffer.AddElement(30);
    buffer.AddElement(10);
    buffer.AddElement(20);
    auto* vec = buffer.ToPlainVector();
    ASSERT_EQ(vec->size(), 3);
    // Should be sorted ascending
    EXPECT_EQ((*vec)[0], 10);
    EXPECT_EQ((*vec)[1], 20);
    EXPECT_EQ((*vec)[2], 30);
    delete vec;
}

TEST(SetTopNBufferTest, LoadFromPlainVector) {
    SetTopNBuffer<AscComparator> buffer(AscComparator{});
    buffer.AddElement(99);
    std::vector<long> plain = {5, 15, 25};
    buffer.LoadFromPlainVector(plain);
    EXPECT_EQ(buffer.GetSize(), 3);
    auto* vec = buffer.ToPlainVector();
    EXPECT_EQ((*vec)[0], 5);
    EXPECT_EQ((*vec)[1], 15);
    EXPECT_EQ((*vec)[2], 25);
    delete vec;
}

TEST(SetTopNBufferTest, DuplicateElements) {
    SetTopNBuffer<AscComparator> buffer(AscComparator{});
    buffer.AddElement(10);
    buffer.AddElement(10);
    buffer.AddElement(10);
    EXPECT_EQ(buffer.GetSize(), 3);
}

TEST(SetTopNBufferTest, SetAndGetBufferId) {
    SetTopNBuffer<AscComparator> buffer(AscComparator{});
    EXPECT_EQ(buffer.GetBufferId(), -99);
    buffer.SetBufferId(42);
    EXPECT_EQ(buffer.GetBufferId(), 42);
}

TEST(SetTopNBufferTest, IteratorBeginEnd) {
    SetTopNBuffer<AscComparator> buffer(AscComparator{});
    buffer.AddElement(3);
    buffer.AddElement(1);
    buffer.AddElement(2);
    std::vector<long> result;
    for (auto it = buffer.begin(); it != buffer.end(); ++it) {
        result.push_back(*it);
    }
    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result[0], 1);
    EXPECT_EQ(result[1], 2);
    EXPECT_EQ(result[2], 3);
}
