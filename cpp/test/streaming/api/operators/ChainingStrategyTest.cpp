#include <gtest/gtest.h>
#include "streaming/api/operators/ChainingStrategy.h"

TEST(ChainingStrategyTest, EnumValues) {
    EXPECT_NE(ChainingStrategy::ALWAYS, ChainingStrategy::NEVER);
    EXPECT_NE(ChainingStrategy::ALWAYS, ChainingStrategy::HEAD);
    EXPECT_NE(ChainingStrategy::ALWAYS, ChainingStrategy::HEAD_WITH_SOURCES);
    EXPECT_NE(ChainingStrategy::NEVER, ChainingStrategy::HEAD);
}

TEST(ChainingStrategyTest, Assignment) {
    ChainingStrategy s = ChainingStrategy::ALWAYS;
    EXPECT_EQ(s, ChainingStrategy::ALWAYS);
    s = ChainingStrategy::NEVER;
    EXPECT_EQ(s, ChainingStrategy::NEVER);
    s = ChainingStrategy::HEAD;
    EXPECT_EQ(s, ChainingStrategy::HEAD);
    s = ChainingStrategy::HEAD_WITH_SOURCES;
    EXPECT_EQ(s, ChainingStrategy::HEAD_WITH_SOURCES);
}

TEST(ChainingStrategyTest, SwitchStatement) {
    ChainingStrategy s = ChainingStrategy::HEAD;
    int result = 0;
    switch (s) {
        case ChainingStrategy::ALWAYS: result = 1; break;
        case ChainingStrategy::NEVER: result = 2; break;
        case ChainingStrategy::HEAD: result = 3; break;
        case ChainingStrategy::HEAD_WITH_SOURCES: result = 4; break;
    }
    EXPECT_EQ(result, 3);
}
