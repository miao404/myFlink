#include <gtest/gtest.h>
#include "table/runtime/operators/sink/ConstraintEnforcer.h"
#include "test/core/operators/OutputTest.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"

TEST(ConstraintEnforcerTest, ProcessElement) {
    OutputTest output;
    ConstraintEnforcer enforcer(&output);
    enforcer.open();

    BinaryRowData* rowData = BinaryRowData::createBinaryRowDataWithMem(1);
    rowData->setLong(0, 42);
    StreamRecord* record = new StreamRecord(rowData);

    enforcer.processElement(record);

    auto* collected = output.getRecord();
    ASSERT_NE(collected, nullptr);
    auto* value = static_cast<RowData*>(collected->getValue());
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(*value->getLong(0), 42);
}

TEST(ConstraintEnforcerTest, ProcessBatch) {
    OutputTestVectorBatch output;
    ConstraintEnforcer enforcer(&output);
    enforcer.open();

    auto* vb = new omnistream::VectorBatch(3);
    auto* vec = new omniruntime::vec::Vector<int64_t>(3);
    vec->SetValue(0, 10);
    vec->SetValue(1, 20);
    vec->SetValue(2, 30);
    vb->Append(vec);

    StreamRecord* record = new StreamRecord(vb);
    enforcer.processBatch(record);

    auto& all = output.getAll();
    EXPECT_EQ(all.size(), 1);
}

TEST(ConstraintEnforcerTest, MultipleProcessElement) {
    OutputTest output;
    ConstraintEnforcer enforcer(&output);
    enforcer.open();

    for (int i = 0; i < 5; i++) {
        BinaryRowData* rowData = BinaryRowData::createBinaryRowDataWithMem(1);
        rowData->setLong(0, i * 10);
        StreamRecord* record = new StreamRecord(rowData);
        enforcer.processElement(record);
    }

    auto& all = output.getAll();
    EXPECT_EQ(all.size(), 5);
}

TEST(ConstraintEnforcerTest, InitializeState) {
    OutputTest output;
    ConstraintEnforcer enforcer(&output);
    enforcer.initializeState(nullptr, nullptr);
    SUCCEED();
}

TEST(ConstraintEnforcerTest, ProcessWatermark) {
    OutputTest output;
    ConstraintEnforcer enforcer(&output);
    Watermark* wm = new Watermark(1000);
    enforcer.ProcessWatermark(wm);
    SUCCEED();
}
