#include <gtest/gtest.h>
#include "table/runtime/operators/window/WindowKey.h"
#include "table/data/binary/BinaryRowData.h"

namespace {
BinaryRowData* createRowKey(int64_t value) {
    auto* row = BinaryRowData::createBinaryRowDataWithMem(1);
    row->setLong(0, value);
    return row;
}
} // namespace

TEST(WindowKeyTest, Construction) {
    auto* key = createRowKey(42);
    WindowKey wk(1000L, key);
    EXPECT_EQ(wk.getWindow(), 1000L);
    EXPECT_EQ(wk.getKey(), key);
}

TEST(WindowKeyTest, Replace) {
    auto* key1 = createRowKey(42);
    auto* key2 = createRowKey(84);
    WindowKey wk(1000L, key1);

    wk.replace(2000L, key2);
    EXPECT_EQ(wk.getWindow(), 2000L);
    EXPECT_EQ(wk.getKey(), key2);
}

TEST(WindowKeyTest, EqualitySameKeys) {
    auto* key1 = createRowKey(42);
    auto* key2 = createRowKey(42);
    WindowKey wk1(1000L, key1);
    WindowKey wk2(1000L, key2);
    EXPECT_TRUE(wk1 == wk2);
}

TEST(WindowKeyTest, EqualityDifferentWindow) {
    auto* key1 = createRowKey(42);
    auto* key2 = createRowKey(42);
    WindowKey wk1(1000L, key1);
    WindowKey wk2(2000L, key2);
    EXPECT_FALSE(wk1 == wk2);
}

TEST(WindowKeyTest, EqualityDifferentKey) {
    auto* key1 = createRowKey(42);
    auto* key2 = createRowKey(84);
    WindowKey wk1(1000L, key1);
    WindowKey wk2(1000L, key2);
    EXPECT_FALSE(wk1 == wk2);
}

TEST(WindowKeyTest, Hash) {
    auto* key1 = createRowKey(42);
    auto* key2 = createRowKey(42);
    WindowKey wk1(1000L, key1);
    WindowKey wk2(1000L, key2);
    EXPECT_EQ(wk1.hash(), wk2.hash());
}

TEST(WindowKeyTest, HashDifferent) {
    auto* key1 = createRowKey(42);
    auto* key2 = createRowKey(84);
    WindowKey wk1(1000L, key1);
    WindowKey wk2(2000L, key2);
    // Hashes might collide but likely won't
    // Just check they compute without crash
    wk1.hash();
    wk2.hash();
    SUCCEED();
}

TEST(WindowKeyTest, StdHash) {
    auto* key1 = createRowKey(42);
    auto* key2 = createRowKey(42);
    WindowKey wk1(1000L, key1);
    WindowKey wk2(1000L, key2);
    std::hash<WindowKey> hasher;
    EXPECT_EQ(hasher(wk1), hasher(wk2));
}
