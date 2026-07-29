#include <gtest/gtest.h>
#include "table/runtime/operators/rank/TopNBuffer.h"
#include "table/data/binary/BinaryRowData.h"

namespace {

BinaryRowData* createSortKey(int64_t value) {
    auto* row = BinaryRowData::createBinaryRowDataWithMem(1);
    row->setLong(0, value);
    return row;
}

} // namespace

TEST(TopNBufferTest, PutAndGet) {
    TopNBuffer buffer;
    auto* sortKey = createSortKey(100);
    auto* value = createSortKey(1);

    int count = buffer.put(sortKey, value);
    EXPECT_EQ(count, 1);
    EXPECT_EQ(buffer.getCurrentTopNum(), 1);

    auto* retrieved = buffer.get(sortKey);
    ASSERT_NE(retrieved, nullptr);
    EXPECT_EQ(retrieved->size(), 1);
    EXPECT_EQ(retrieved->at(0), value);
}

TEST(TopNBufferTest, PutMultipleSameKey) {
    TopNBuffer buffer;
    auto* sortKey = createSortKey(100);
    auto* v1 = createSortKey(1);
    auto* v2 = createSortKey(2);

    buffer.put(sortKey, v1);
    int count = buffer.put(sortKey, v2);
    EXPECT_EQ(count, 2);
    EXPECT_EQ(buffer.getCurrentTopNum(), 2);
}

TEST(TopNBufferTest, Contains) {
    TopNBuffer buffer;
    auto* key1 = createSortKey(100);
    auto* key2 = createSortKey(200);
    auto* v1 = createSortKey(1);

    buffer.put(key1, v1);

    EXPECT_TRUE(buffer.contains(key1));
    EXPECT_FALSE(buffer.contains(key2));
    delete key2;
}

TEST(TopNBufferTest, RemoveAll) {
    TopNBuffer buffer;
    auto* sortKey = createSortKey(100);
    auto* v1 = createSortKey(1);
    auto* v2 = createSortKey(2);

    buffer.put(sortKey, v1);
    buffer.put(sortKey, v2);
    EXPECT_EQ(buffer.getCurrentTopNum(), 2);

    buffer.removeAll(sortKey);
    EXPECT_EQ(buffer.getCurrentTopNum(), 0);
}

TEST(TopNBufferTest, RemoveLastFromSingleEntry) {
    TopNBuffer buffer;
    auto* sortKey = createSortKey(100);
    auto* v1 = createSortKey(1);

    buffer.put(sortKey, v1);
    auto* removed = buffer.removeLast();

    EXPECT_EQ(removed, v1);
    EXPECT_EQ(buffer.getCurrentTopNum(), 0);
}

TEST(TopNBufferTest, RemoveLastFromEmpty) {
    TopNBuffer buffer;
    auto* removed = buffer.removeLast();
    EXPECT_EQ(removed, nullptr);
}

TEST(TopNBufferTest, RemoveLastMultipleEntries) {
    TopNBuffer buffer;
    auto* key1 = createSortKey(100);
    auto* key2 = createSortKey(200);
    auto* v1 = createSortKey(1);
    auto* v2 = createSortKey(2);

    buffer.put(key1, v1);
    buffer.put(key2, v2);
    EXPECT_EQ(buffer.getCurrentTopNum(), 2);

    auto* removed = buffer.removeLast();
    EXPECT_NE(removed, nullptr);
    EXPECT_EQ(buffer.getCurrentTopNum(), 1);
}

TEST(TopNBufferTest, LastElement) {
    TopNBuffer buffer;
    auto* key1 = createSortKey(100);
    auto* v1 = createSortKey(1);

    EXPECT_EQ(buffer.lastElement(), nullptr);

    buffer.put(key1, v1);
    auto* last = buffer.lastElement();
    EXPECT_NE(last, nullptr);
}

TEST(TopNBufferTest, PutAll) {
    TopNBuffer buffer;
    auto* sortKey = createSortKey(100);
    auto* v1 = createSortKey(1);
    auto* v2 = createSortKey(2);

    auto* values = new std::vector<RowData*>();
    values->push_back(v1);
    values->push_back(v2);

    buffer.putAll(sortKey, values);
    EXPECT_EQ(buffer.getCurrentTopNum(), 2);

    auto* retrieved = buffer.get(sortKey);
    ASSERT_NE(retrieved, nullptr);
    EXPECT_EQ(retrieved->size(), 2);
}

TEST(TopNBufferTest, PutAllReplace) {
    TopNBuffer buffer;
    auto* sortKey = createSortKey(100);
    auto* v1 = createSortKey(1);
    buffer.put(sortKey, v1);

    auto* v2 = createSortKey(2);
    auto* v3 = createSortKey(3);
    auto* newValues = new std::vector<RowData*>();
    newValues->push_back(v2);
    newValues->push_back(v3);

    buffer.putAll(sortKey, newValues);
    EXPECT_EQ(buffer.getCurrentTopNum(), 2);
}

TEST(TopNBufferTest, CheckSortKeyInBufferRangeEmpty) {
    TopNBuffer buffer;
    auto* sortKey = createSortKey(100);

    EXPECT_TRUE(buffer.checkSortKeyInBufferRange(sortKey, 3));
    delete sortKey;
}

TEST(TopNBufferTest, CheckSortKeyInBufferRangeNotFull) {
    TopNBuffer buffer;
    auto* key1 = createSortKey(100);
    auto* v1 = createSortKey(1);
    buffer.put(key1, v1);

    auto* testKey = createSortKey(200);
    EXPECT_TRUE(buffer.checkSortKeyInBufferRange(testKey, 3));
    delete testKey;
}

TEST(TopNBufferTest, IteratorBeginEnd) {
    TopNBuffer buffer;
    auto* key1 = createSortKey(100);
    auto* v1 = createSortKey(1);
    buffer.put(key1, v1);

    int count = 0;
    for (auto it = buffer.begin(); it != buffer.end(); ++it) {
        count++;
    }
    EXPECT_EQ(count, 1);
}

TEST(TopNBufferTest, GetLastElementEmptyVector) {
    TopNBuffer buffer;
    std::vector<RowData*> emptyVec;
    EXPECT_EQ(buffer.getLastElement(emptyVec), nullptr);
}

TEST(TopNBufferTest, GetLastElementNonEmpty) {
    TopNBuffer buffer;
    auto* v1 = createSortKey(1);
    auto* v2 = createSortKey(2);
    std::vector<RowData*> vec = {v1, v2};
    EXPECT_EQ(buffer.getLastElement(vec), v2);
}

TEST(TopNBufferTest, RemoveLastOverload) {
    TopNBuffer buffer;
    auto* key = createSortKey(100);
    auto* v1 = createSortKey(1);
    auto* v2 = createSortKey(2);

    buffer.put(key, v1);
    buffer.put(key, v2);

    auto* collection = buffer.get(key);
    buffer.removeLast(collection, v2, key);
    EXPECT_EQ(buffer.getCurrentTopNum(), 1);
}

TEST(TopNBufferTest, RemoveLastOverloadNullElement) {
    TopNBuffer buffer;
    auto* key = createSortKey(100);
    auto* v1 = createSortKey(1);

    auto* collection = new std::vector<RowData*>();
    collection->push_back(v1);

    buffer.removeLast(collection, nullptr, key);
    EXPECT_EQ(collection->size(), 1);

    delete collection;
    delete key;
}
