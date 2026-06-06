#include <gtest/gtest.h>
#include "table/runtime/operators/VectorBatchUtils.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"

namespace {

BinaryRowData* createRow(int arity) {
    return BinaryRowData::createBinaryRowDataWithMem(arity);
}

} // namespace

TEST(VectorBatchUtilsTest, AppendLongVectorForInt64) {
    int numRows = 3;
    std::vector<RowData*> rows;
    for (int i = 0; i < numRows; i++) {
        auto* row = createRow(1);
        row->setLong(0, (i + 1) * 100L);
        rows.push_back(row);
    }

    auto* vb = new omnistream::VectorBatch(numRows);
    VectorBatchUtils::AppendLongVectorForInt64(vb, rows, numRows, 0);

    EXPECT_EQ(vb->GetVectorCount(), 1);
    auto* vec = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vb->Get(0));
    EXPECT_EQ(vec->GetValue(0), 100L);
    EXPECT_EQ(vec->GetValue(1), 200L);
    EXPECT_EQ(vec->GetValue(2), 300L);

    for (auto* r : rows) {
        delete[] r->getSegment();
        delete r;
    }
    delete vb;
}

TEST(VectorBatchUtilsTest, AppendLongVectorForDouble) {
    int numRows = 2;
    std::vector<RowData*> rows;
    for (int i = 0; i < numRows; i++) {
        auto* row = createRow(1);
        row->setLong(0, (i + 1) * 50L);
        rows.push_back(row);
    }

    auto* vb = new omnistream::VectorBatch(numRows);
    VectorBatchUtils::AppendLongVectorForDouble(vb, rows, numRows, 0);

    EXPECT_EQ(vb->GetVectorCount(), 1);

    for (auto* r : rows) {
        delete[] r->getSegment();
        delete r;
    }
    delete vb;
}

TEST(VectorBatchUtilsTest, AppendIntVector) {
    int numRows = 3;
    std::vector<RowData*> rows;
    for (int i = 0; i < numRows; i++) {
        auto* row = createRow(1);
        row->setInt(0, (i + 1) * 10);
        rows.push_back(row);
    }

    auto* vb = new omnistream::VectorBatch(numRows);
    VectorBatchUtils::AppendIntVector(vb, rows, numRows, 0);

    EXPECT_EQ(vb->GetVectorCount(), 1);
    auto* vec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(vb->Get(0));
    EXPECT_EQ(vec->GetValue(0), 10);
    EXPECT_EQ(vec->GetValue(1), 20);
    EXPECT_EQ(vec->GetValue(2), 30);

    for (auto* r : rows) {
        delete[] r->getSegment();
        delete r;
    }
    delete vb;
}

TEST(VectorBatchUtilsTest, AppendIntVectorForBool) {
    int numRows = 2;
    std::vector<RowData*> rows;
    for (int i = 0; i < numRows; i++) {
        auto* row = createRow(1);
        row->setInt(0, i % 2);
        rows.push_back(row);
    }

    auto* vb = new omnistream::VectorBatch(numRows);
    VectorBatchUtils::AppendIntVectorForBool(vb, rows, numRows, 0);

    EXPECT_EQ(vb->GetVectorCount(), 1);

    for (auto* r : rows) {
        delete[] r->getSegment();
        delete r;
    }
    delete vb;
}

TEST(VectorBatchUtilsTest, AppendStringVectorInvalidNegativeThrows) {
    auto* vb = new omnistream::VectorBatch(1);
    std::vector<RowData*> rows;
    EXPECT_THROW(VectorBatchUtils::AppendStringVector(vb, rows, -1, 0), std::runtime_error);
    delete vb;
}

TEST(VectorBatchUtilsTest, AppendStringVectorExceedsRowsThrows) {
    auto* vb = new omnistream::VectorBatch(5);
    std::vector<RowData*> rows;
    auto* row = createRow(1);
    rows.push_back(row);
    EXPECT_THROW(VectorBatchUtils::AppendStringVector(vb, rows, 5, 0), std::runtime_error);
    delete[] row->getSegment();
    delete row;
    delete vb;
}

TEST(VectorBatchUtilsTest, AppendMultipleColumnTypes) {
    int numRows = 2;
    std::vector<RowData*> rows;
    for (int i = 0; i < numRows; i++) {
        auto* row = createRow(2);
        row->setLong(0, (i + 1) * 100L);
        row->setInt(1, (i + 1) * 10);
        rows.push_back(row);
    }

    auto* vb = new omnistream::VectorBatch(numRows);
    VectorBatchUtils::AppendLongVectorForInt64(vb, rows, numRows, 0);
    VectorBatchUtils::AppendIntVector(vb, rows, numRows, 1);

    EXPECT_EQ(vb->GetVectorCount(), 2);
    auto* longVec = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vb->Get(0));
    auto* intVec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(vb->Get(1));
    EXPECT_EQ(longVec->GetValue(0), 100L);
    EXPECT_EQ(intVec->GetValue(0), 10);

    for (auto* r : rows) {
        delete[] r->getSegment();
        delete r;
    }
    delete vb;
}

TEST(VectorBatchUtilsTest, ZeroRowsInt64) {
    auto* vb = new omnistream::VectorBatch(0);
    std::vector<RowData*> rows;
    VectorBatchUtils::AppendLongVectorForInt64(vb, rows, 0, 0);
    EXPECT_EQ(vb->GetVectorCount(), 1);
    delete vb;
}

TEST(VectorBatchUtilsTest, ZeroRowsInt) {
    auto* vb = new omnistream::VectorBatch(0);
    std::vector<RowData*> rows;
    VectorBatchUtils::AppendIntVector(vb, rows, 0, 0);
    EXPECT_EQ(vb->GetVectorCount(), 1);
    delete vb;
}
