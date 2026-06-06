#include <gtest/gtest.h>
#include "streaming/api/operators/InputSelection.h"

TEST(InputSelectionTest, FairSelectNextIndexBothAvailable) {
    // inputMask=0b11, availableMask=0b11, lastRead=-1
    int result = InputSelection::fairSelectNextIndex(0b11, 0b11, -1);
    EXPECT_EQ(result, 0);
}

TEST(InputSelectionTest, FairSelectNextIndexSecondInput) {
    // inputMask=0b11, availableMask=0b11, lastRead=0
    int result = InputSelection::fairSelectNextIndex(0b11, 0b11, 0);
    EXPECT_EQ(result, 1);
}

TEST(InputSelectionTest, FairSelectNextIndexWrapAround) {
    // inputMask=0b11, availableMask=0b11, lastRead=1
    int result = InputSelection::fairSelectNextIndex(0b11, 0b11, 1);
    EXPECT_EQ(result, 0);
}

TEST(InputSelectionTest, FairSelectNextIndexNoAvailable) {
    // inputMask=0b11, availableMask=0b00, lastRead=-1
    int result = InputSelection::fairSelectNextIndex(0b11, 0b00, -1);
    EXPECT_EQ(result, -1);
}

TEST(InputSelectionTest, FairSelectNextIndexOnlyFirstAvailable) {
    // inputMask=0b11, availableMask=0b01, lastRead=-1
    int result = InputSelection::fairSelectNextIndex(0b11, 0b01, -1);
    EXPECT_EQ(result, 0);
}

TEST(InputSelectionTest, FairSelectNextIndexOnlySecondAvailable) {
    // inputMask=0b11, availableMask=0b10, lastRead=-1
    int result = InputSelection::fairSelectNextIndex(0b11, 0b10, -1);
    EXPECT_EQ(result, 1);
}

TEST(InputSelectionTest, FairSelectNextIndexInputMaskRestriction) {
    // inputMask=0b01, availableMask=0b11, lastRead=-1
    int result = InputSelection::fairSelectNextIndex(0b01, 0b11, -1);
    EXPECT_EQ(result, 0);
}

TEST(InputSelectionTest, FairSelectNextIndexInputMaskRestriction2) {
    // inputMask=0b10, availableMask=0b11, lastRead=-1
    int result = InputSelection::fairSelectNextIndex(0b10, 0b11, -1);
    EXPECT_EQ(result, 1);
}

TEST(InputSelectionTest, SelectFirstBitRightFromNext) {
    // bits=0b1010, next=0
    EXPECT_EQ(InputSelection::selectFirstBitRightFromNext(0b1010, 0), 1);
}

TEST(InputSelectionTest, SelectFirstBitRightFromNextStart) {
    // bits=0b0001, next=0
    EXPECT_EQ(InputSelection::selectFirstBitRightFromNext(0b0001, 0), 0);
}

TEST(InputSelectionTest, SelectFirstBitRightFromNextNoBit) {
    // bits=0b0000, next=0
    EXPECT_EQ(InputSelection::selectFirstBitRightFromNext(0, 0), -1);
}

TEST(InputSelectionTest, SelectFirstBitRightFromNextBeyond64) {
    EXPECT_EQ(InputSelection::selectFirstBitRightFromNext(0xFF, 64), -1);
}

TEST(InputSelectionTest, SelectFirstBitRightFromNextAllBits) {
    // bits = all ones
    long allBits = -1L;
    EXPECT_EQ(InputSelection::selectFirstBitRightFromNext(allBits, 0), 0);
    EXPECT_EQ(InputSelection::selectFirstBitRightFromNext(allBits, 32), 32);
    EXPECT_EQ(InputSelection::selectFirstBitRightFromNext(allBits, 63), 63);
}

TEST(InputSelectionTest, FairSelectMultipleInputs) {
    // 4 inputs, all available
    long inputMask = 0b1111;
    long availableMask = 0b1111;

    int idx = InputSelection::fairSelectNextIndex(inputMask, availableMask, -1);
    EXPECT_EQ(idx, 0);
    idx = InputSelection::fairSelectNextIndex(inputMask, availableMask, 0);
    EXPECT_EQ(idx, 1);
    idx = InputSelection::fairSelectNextIndex(inputMask, availableMask, 1);
    EXPECT_EQ(idx, 2);
    idx = InputSelection::fairSelectNextIndex(inputMask, availableMask, 2);
    EXPECT_EQ(idx, 3);
    idx = InputSelection::fairSelectNextIndex(inputMask, availableMask, 3);
    EXPECT_EQ(idx, 0);
}
