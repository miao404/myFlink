/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for WatermarkAssignerOperator (P0 priority) covering
 *              various event time distributions, out-of-order tolerance, and
 *              emission intervals.
 *              Reference UT: WatermarkAssignerOperatorTest.cpp
 */

#include "table_fuzz_wrapper.h"
#include "dt_fuzz_data.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "streaming/api/operators/WatermarkAssignerOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "core/operators/OutputTest.h"
#include "streaming/api/operators/SystemProcessingTimeService.h"
#include <test/util/test_util.h>

using json = nlohmann::json;

static void TestWatermarkBasic(const WatermarkAssignerFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "WatermarkAssignerFuzz: Basic processBatch" << std::endl;

    int timeRowIndex = 0;
    long outOfOrderT = std::abs(fzd.outOfOrderness % 10000) + 1;
    long emissionInterval = std::abs(fzd.emissionInterval % 5000) + 100;

    OutputTest *out = new OutputTest();
    WatermarkAssignerOperator *op = new WatermarkAssignerOperator(
        out, timeRowIndex, outOfOrderT, emissionInterval, new SystemProcessingTimeService());
    op->open();

    int rowCount = (loopCount % 20) + 2;
    omnistream::VectorBatch vb(rowCount);

    std::vector<long> eventTimes(rowCount);
    std::vector<long> values(rowCount);
    for (int i = 0; i < rowCount; ++i) {
        eventTimes[i] = std::abs(fzd.eventTime) + static_cast<long>(i) * 100;
        values[i] = fzd.eventTime2 + static_cast<long>(i);
    }
    vb.Append(omniruntime::TestUtil::CreateVector(rowCount, eventTimes.data()));
    vb.Append(omniruntime::TestUtil::CreateVector(rowCount, values.data()));

    StreamRecord *record = new StreamRecord(&vb);
    op->processBatch(record);
    op->finish();

    delete op;
    delete out;
}

static void TestWatermarkOutOfOrder(const WatermarkAssignerFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "WatermarkAssignerFuzz: Out-of-order events" << std::endl;

    int timeRowIndex = 0;
    long outOfOrderT = std::abs(fzd.outOfOrderness % 8000) + 500;
    long emissionInterval = std::abs(fzd.emissionInterval % 3000) + 200;

    OutputTest *out = new OutputTest();
    WatermarkAssignerOperator *op = new WatermarkAssignerOperator(
        out, timeRowIndex, outOfOrderT, emissionInterval, new SystemProcessingTimeService());
    op->open();

    int rowCount = (loopCount % 15) + 3;
    omnistream::VectorBatch vb(rowCount);

    std::vector<long> eventTimes(rowCount);
    std::vector<long> values(rowCount);
    for (int i = 0; i < rowCount; ++i) {
        // Simulate out-of-order: even indices go forward, odd go backward
        if (i % 2 == 0) {
            eventTimes[i] = std::abs(fzd.eventTime) + static_cast<long>(i) * 200;
        } else {
            eventTimes[i] = std::abs(fzd.eventTime) + static_cast<long>(i - 2) * 200;
        }
        values[i] = fzd.eventTime2 + static_cast<long>(i) * 10;
    }
    vb.Append(omniruntime::TestUtil::CreateVector(rowCount, eventTimes.data()));
    vb.Append(omniruntime::TestUtil::CreateVector(rowCount, values.data()));

    StreamRecord *record = new StreamRecord(&vb);
    op->processBatch(record);
    op->finish();

    delete op;
    delete out;
}

static void TestWatermarkMultipleBatches(const WatermarkAssignerFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "WatermarkAssignerFuzz: Multiple batches" << std::endl;

    int timeRowIndex = 0;
    long outOfOrderT = std::abs(fzd.outOfOrderness % 5000) + 100;
    long emissionInterval = std::abs(fzd.emissionInterval % 2000) + 50;

    OutputTest *out = new OutputTest();
    WatermarkAssignerOperator *op = new WatermarkAssignerOperator(
        out, timeRowIndex, outOfOrderT, emissionInterval, new SystemProcessingTimeService());
    op->open();

    uint16_t batchCount = (loopCount % 5) + 2;
    for (uint16_t b = 0; b < batchCount; ++b) {
        int rowCount = (loopCount % 10) + 2;
        omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCount);

        std::vector<long> eventTimes(rowCount);
        std::vector<long> values(rowCount);
        for (int i = 0; i < rowCount; ++i) {
            eventTimes[i] = std::abs(fzd.eventTime) + static_cast<long>(b * 5000 + i * 300);
            values[i] = fzd.eventTime3 + static_cast<long>(b * rowCount + i);
        }
        vb->Append(omniruntime::TestUtil::CreateVector(rowCount, eventTimes.data()));
        vb->Append(omniruntime::TestUtil::CreateVector(rowCount, values.data()));

        StreamRecord *record = new StreamRecord(vb);
        op->processBatch(record);
    }
    op->finish();

    delete op;
    delete out;
}

int WatermarkAssignerFuzz(struct WatermarkAssignerFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 3) {
            case 0:
                TestWatermarkBasic(fzd, loopCount);
                break;
            case 1:
                TestWatermarkOutOfOrder(fzd, loopCount);
                break;
            case 2:
                TestWatermarkMultipleBatches(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "WatermarkAssignerFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
