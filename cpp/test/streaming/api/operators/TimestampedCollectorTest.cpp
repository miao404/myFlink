#include <gtest/gtest.h>
#include "streaming/api/operators/TimestampedCollector.h"
#include "test/core/operators/OutputTest.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"

TEST(TimestampedCollectorTest, Construction) {
    OutputTest output;
    TimestampedCollector collector(&output, false);
    SUCCEED();
}

TEST(TimestampedCollectorTest, ConstructionDataStream) {
    OutputTest output;
    TimestampedCollector collector(&output, true);
    SUCCEED();
}

TEST(TimestampedCollectorTest, SetTimestampWithTimestamp) {
    OutputTest output;
    TimestampedCollector collector(&output, false);

    StreamRecord record(nullptr);
    record.setTimestamp(12345);
    record.setTag(StreamElementTag::TAG_REC_WITH_TIMESTAMP);

    collector.setTimestamp(&record);

    BinaryRowData* rowData = BinaryRowData::createBinaryRowDataWithMem(1);
    rowData->setLong(0, 42);
    collector.collect(rowData);

    auto* collected = output.getRecord();
    ASSERT_NE(collected, nullptr);
    EXPECT_EQ(collected->getTimestamp(), 12345);
}

TEST(TimestampedCollectorTest, SetTimestampWithoutTimestamp) {
    OutputTest output;
    TimestampedCollector collector(&output, false);

    StreamRecord record(nullptr);
    record.setTag(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP);

    collector.setTimestamp(&record);
    // eraseTimestamp should be called internally
    SUCCEED();
}

TEST(TimestampedCollectorTest, CollectNonDataStream) {
    OutputTest output;
    TimestampedCollector collector(&output, false);

    StreamRecord record(nullptr);
    record.setTimestamp(100);
    record.setTag(StreamElementTag::TAG_REC_WITH_TIMESTAMP);
    collector.setTimestamp(&record);

    BinaryRowData* rowData = BinaryRowData::createBinaryRowDataWithMem(1);
    rowData->setLong(0, 55);
    collector.collect(rowData);

    auto* collected = output.getRecord();
    ASSERT_NE(collected, nullptr);
    EXPECT_EQ(collected->getTimestamp(), 100);
}

TEST(TimestampedCollectorTest, Close) {
    OutputTest output;
    TimestampedCollector collector(&output, false);
    collector.close();
    SUCCEED();
}

TEST(TimestampedCollectorTest, EmitWatermark) {
    OutputTest output;
    TimestampedCollector collector(&output, false);

    Watermark* wm = new Watermark(9999);
    collector.emitWatermark(wm);

    auto* received = output.getWatermark();
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->getTimestamp(), 9999);
}

TEST(TimestampedCollectorTest, EmitWatermarkStatus) {
    OutputTest output;
    TimestampedCollector collector(&output, false);

    WatermarkStatus status(WatermarkStatus::IDLE_STATUS);
    collector.emitWatermarkStatus(&status);
    SUCCEED();
}

TEST(TimestampedCollectorTest, SetAbsoluteTimestamp) {
    OutputTest output;
    TimestampedCollector collector(&output, true);

    collector.setAbsoluteTimestamp(77777);
    SUCCEED();
}

TEST(TimestampedCollectorTest, EraseTimestamp) {
    OutputTest output;
    TimestampedCollector collector(&output, false);

    collector.eraseTimestamp();
    SUCCEED();
}

TEST(TimestampedCollectorTest, MultipleCollects) {
    OutputTest output;
    TimestampedCollector collector(&output, false);

    StreamRecord record(nullptr);
    record.setTimestamp(100);
    record.setTag(StreamElementTag::TAG_REC_WITH_TIMESTAMP);
    collector.setTimestamp(&record);

    for (int i = 0; i < 5; i++) {
        BinaryRowData* rowData = BinaryRowData::createBinaryRowDataWithMem(1);
        rowData->setLong(0, i);
        collector.collect(rowData);
    }

    auto& all = output.getAll();
    EXPECT_EQ(all.size(), 5);
}
