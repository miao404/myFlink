#include <gtest/gtest.h>
#include "table/runtime/operators/rank/SortedKVCache.h"

TEST(SortedKVCacheTest, PutAndGet) {
    SortedKVCache<int, int*> cache(3);
    int v1 = 10;
    cache.put(1, &v1);
    EXPECT_EQ(cache.get(1), &v1);
}

TEST(SortedKVCacheTest, GetNonExistent) {
    SortedKVCache<int, int*> cache(3);
    EXPECT_EQ(cache.get(42), nullptr);
}

TEST(SortedKVCacheTest, HasKey) {
    SortedKVCache<int, int*> cache(3);
    int v1 = 10;
    cache.put(1, &v1);
    EXPECT_TRUE(cache.hasKey(1));
    EXPECT_FALSE(cache.hasKey(2));
}

TEST(SortedKVCacheTest, UpdateExistingKey) {
    SortedKVCache<int, int*> cache(3);
    int v1 = 10, v2 = 20;
    cache.put(1, &v1);
    cache.put(1, &v2);
    EXPECT_EQ(cache.get(1), &v2);
}

TEST(SortedKVCacheTest, EvictionOnCapacity) {
    SortedKVCache<int, int*> cache(2);
    int v1 = 10, v2 = 20, v3 = 30;
    cache.put(1, &v1);
    cache.put(2, &v2);
    cache.put(3, &v3);
    // Key 1 should be evicted (LRU)
    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.get(2), &v2);
    EXPECT_EQ(cache.get(3), &v3);
}

TEST(SortedKVCacheTest, LRUOrder) {
    SortedKVCache<int, int*> cache(2);
    int v1 = 10, v2 = 20, v3 = 30;
    cache.put(1, &v1);
    cache.put(2, &v2);
    // Access key 1 to make it most recently used
    cache.get(1);
    cache.put(3, &v3);
    // Key 2 should be evicted
    EXPECT_EQ(cache.get(2), nullptr);
    EXPECT_NE(cache.get(1), nullptr);
    EXPECT_NE(cache.get(3), nullptr);
}

TEST(SortedKVCacheTest, DefaultCapacity) {
    SortedKVCache<int, int*> cache;
    // Default capacity is 1024
    for (int i = 0; i < 1024; i++) {
        int* v = new int(i);
        cache.put(i, v);
    }
    // All 1024 should be present
    for (int i = 0; i < 1024; i++) {
        EXPECT_NE(cache.get(i), nullptr);
    }
    // Add one more, evicts first
    int v = 9999;
    cache.put(1024, &v);
    EXPECT_EQ(cache.get(0), nullptr);
    // cleanup
    for (int i = 1; i < 1024; i++) {
        int* p = cache.get(i);
        delete p;
    }
}

TEST(SortedKVCacheTest, ClearOldValues) {
    SortedKVCache<int, int*> cache(3);
    int* v1 = new int(10);
    int* v2 = new int(20);
    cache.put(1, v1);
    cache.put(1, v2);  // v1 goes to oldValues
    cache.clearOldValues();  // deletes v1
    EXPECT_EQ(cache.get(1), v2);
    delete v2;
}

TEST(SortedKVCacheTest, EvictionDoesNotFreeByDefault) {
    SortedKVCache<int, int*> cache(1);
    int* v1 = new int(10);
    int* v2 = new int(20);
    cache.put(1, v1);
    cache.put(2, v2);  // v1 evicted but not freed (ownsValues=false by default)
    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.get(2), v2);
    delete v1;
}

TEST(SortedKVCacheTest, PutUpdateBringsToFront) {
    SortedKVCache<int, int*> cache(3);
    int v1 = 10, v2 = 20, v3 = 30, v4 = 40;
    cache.put(1, &v1);
    cache.put(2, &v2);
    cache.put(3, &v3);
    // Update key 1 to bring it to front
    cache.put(1, &v4);
    // Now LRU should be key 2
    int v5 = 50;
    cache.put(4, &v5);
    EXPECT_EQ(cache.get(2), nullptr);
    EXPECT_NE(cache.get(1), nullptr);
}
