#include <gtest/gtest.h>
#include "streaming/api/operators/CountingOutput.h"
#include "streaming/runtime/tasks/WatermarkGaugeExposingOutput.h"
#include "runtime/metrics/Counter.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"

namespace {

class MockWatermarkOutput : public WatermarkGaugeExposingOutput {
public:
    void collect(void *record) override {
        collectedRecord = reinterpret_cast<StreamRecord*>(record);
        collectCount++;
    }
    void close() override { closeCalled = true; }
    void emitWatermark(Watermark *mark) override { watermark = mark; }
    void emitWatermarkStatus(WatermarkStatus *status) override { wmStatus = status; }

    StreamRecord* collectedRecord = nullptr;
    int collectCount = 0;
    bool closeCalled = false;
    Watermark* watermark = nullptr;
    WatermarkStatus* wmStatus = nullptr;
};

class SimpleCounter : public omnistream::Counter {
public:
    void Inc() override { count++; }
    void Inc(long var1) override { count += var1; }
    void Dec() override { count--; }
    void Dec(long var1) override { count -= var1; }
    long GetCount() override { return count; }
    long count = 0;
};

} // namespace

TEST(CountingOutputTest, CollectIncrementsCounter) {
    MockWatermarkOutput inner;
    SimpleCounter counter;
    CountingOutput countingOutput(&inner, &counter);

    auto* record = new StreamRecord(nullptr);
    countingOutput.collect(record);

    EXPECT_EQ(counter.GetCount(), 1);
    EXPECT_EQ(inner.collectCount, 1);
}

TEST(CountingOutputTest, CollectMultiple) {
    MockWatermarkOutput inner;
    SimpleCounter counter;
    CountingOutput countingOutput(&inner, &counter);

    for (int i = 0; i < 10; i++) {
        auto* record = new StreamRecord(nullptr);
        countingOutput.collect(record);
    }

    EXPECT_EQ(counter.GetCount(), 10);
    EXPECT_EQ(inner.collectCount, 10);
}

TEST(CountingOutputTest, EmitWatermark) {
    MockWatermarkOutput inner;
    SimpleCounter counter;
    CountingOutput countingOutput(&inner, &counter);

    Watermark* wm = new Watermark(5000);
    countingOutput.emitWatermark(wm);

    ASSERT_NE(inner.watermark, nullptr);
    EXPECT_EQ(inner.watermark->getTimestamp(), 5000);
    EXPECT_EQ(counter.GetCount(), 0);
}

TEST(CountingOutputTest, EmitWatermarkStatus) {
    MockWatermarkOutput inner;
    SimpleCounter counter;
    CountingOutput countingOutput(&inner, &counter);

    WatermarkStatus status(WatermarkStatus::IDLE_STATUS);
    countingOutput.emitWatermarkStatus(&status);

    EXPECT_EQ(inner.wmStatus, &status);
}

TEST(CountingOutputTest, Close) {
    MockWatermarkOutput inner;
    SimpleCounter counter;
    CountingOutput countingOutput(&inner, &counter);

    countingOutput.close();
    EXPECT_TRUE(inner.closeCalled);
}

TEST(CountingOutputTest, CountNotIncrementedByWatermark) {
    MockWatermarkOutput inner;
    SimpleCounter counter;
    CountingOutput countingOutput(&inner, &counter);

    Watermark* wm = new Watermark(1000);
    countingOutput.emitWatermark(wm);

    EXPECT_EQ(counter.GetCount(), 0);
}

TEST(CountingOutputTest, CountNotIncrementedByClose) {
    MockWatermarkOutput inner;
    SimpleCounter counter;
    CountingOutput countingOutput(&inner, &counter);

    countingOutput.close();
    EXPECT_EQ(counter.GetCount(), 0);
}
