#include <gtest/gtest.h>
#include "table/runtime/operators/rank/rank_range.h"

// --- ConstantRankRange Tests ---

TEST(ConstantRankRangeTest, BasicConstruction) {
    ConstantRankRange range(1, 10);
    EXPECT_EQ(range.getRankStart(), 1);
    EXPECT_EQ(range.getRankEnd(), 10);
}

TEST(ConstantRankRangeTest, SameStartEnd) {
    ConstantRankRange range(5, 5);
    EXPECT_EQ(range.getRankStart(), 5);
    EXPECT_EQ(range.getRankEnd(), 5);
}

TEST(ConstantRankRangeTest, ZeroRange) {
    ConstantRankRange range(0, 0);
    EXPECT_EQ(range.getRankStart(), 0);
    EXPECT_EQ(range.getRankEnd(), 0);
}

TEST(ConstantRankRangeTest, LargeValues) {
    ConstantRankRange range(1000000L, 9999999L);
    EXPECT_EQ(range.getRankStart(), 1000000L);
    EXPECT_EQ(range.getRankEnd(), 9999999L);
}

TEST(ConstantRankRangeTest, NegativeValues) {
    ConstantRankRange range(-10, -1);
    EXPECT_EQ(range.getRankStart(), -10);
    EXPECT_EQ(range.getRankEnd(), -1);
}

TEST(ConstantRankRangeTest, Polymorphism) {
    RankRange* base = new ConstantRankRange(1, 5);
    auto* derived = dynamic_cast<ConstantRankRange*>(base);
    ASSERT_NE(derived, nullptr);
    EXPECT_EQ(derived->getRankStart(), 1);
    EXPECT_EQ(derived->getRankEnd(), 5);
    delete base;
}

// --- VariableRankRange Tests ---

TEST(VariableRankRangeTest, BasicConstruction) {
    VariableRankRange range(3);
    EXPECT_EQ(range.getRankEndIndex(), 3);
}

TEST(VariableRankRangeTest, ZeroIndex) {
    VariableRankRange range(0);
    EXPECT_EQ(range.getRankEndIndex(), 0);
}

TEST(VariableRankRangeTest, LargeIndex) {
    VariableRankRange range(999);
    EXPECT_EQ(range.getRankEndIndex(), 999);
}

TEST(VariableRankRangeTest, Polymorphism) {
    RankRange* base = new VariableRankRange(7);
    auto* derived = dynamic_cast<VariableRankRange*>(base);
    ASSERT_NE(derived, nullptr);
    EXPECT_EQ(derived->getRankEndIndex(), 7);
    delete base;
}

TEST(VariableRankRangeTest, NegativeIndex) {
    VariableRankRange range(-1);
    EXPECT_EQ(range.getRankEndIndex(), -1);
}
