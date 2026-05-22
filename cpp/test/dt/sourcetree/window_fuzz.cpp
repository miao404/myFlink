/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for AggregateWindowOperator covering tumbling/sliding windows
 *              with BIGINT data types and various window sizes.
 *              Reference UT: AggregateWindowOperatorTest.cpp
 */

#include "table_fuzz_wrapper.h"
#include "dt_fuzz_data.h"
#include "runtime_env_util.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "table/runtime/operators/window/AggregateWindowOperator.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/typeutils/RowDataSerializer.h"
#include "core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include <test/util/test_util.h>

using json = nlohmann::json;
using namespace DtRuntimeEnvUtil;

static const std::string nexmarkQ5Description = R"DELIM({
    "operators": [{
        "uniqueName": "org.apache.flink.table.runtime.operators.window.AggregateWindowOperator",
        "name": "LocalWindowAgg_By_Simple",
        "inputTypes": ["BIGINT", "BIGINT"],
        "outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
        "description": {
            "originDescription": null,
            "windowType": "TumblingWindow",
            "windowSize": 10000,
            "windowSlide": 10000,
            "rowtimeIndex": 1,
            "inputTypes": ["BIGINT", "BIGINT"],
            "outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
            "grouping": [0],
            "aggInfoList": {
                "aggregateCalls": [{
                    "name": "COUNT($0)",
                    "aggregationFunction": "CountAggFunction",
                    "argIndexes": [0],
                    "consumeRetraction": "false",
                    "filterArg": -1
                }],
                "accTypes": ["BIGINT"],
                "aggValueTypes": ["BIGINT"],
                "indexOfCountStar": 0
            }
        }
    }]
})DELIM";

static void TestWindowTumbling(const WindowFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "WindowFuzz: TumblingWindow with CountAgg" << std::endl;

    json parsedJson = json::parse(nexmarkQ5Description);
    std::string uniqueName = "org.apache.flink.table.runtime.operators.window.AggregateWindowOperator";
    omnistream::OperatorConfig opConfig(
        uniqueName,
        "LocalWindowAgg_By_Simple",
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]
    );

    auto *output = new BatchOutputTest();
    auto *windowAggOperator = dynamic_cast<AggregateWindowOperator<RowData *, TimeWindow> *>(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(opConfig, output));

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    windowAggOperator->initializeState(initializer, new LongSerializer());
    windowAggOperator->open();

    int rowCnt = (loopCount % 20) + 3;
    auto *vbatch = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> bidCol(rowCnt);
    std::vector<int64_t> tsCol(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        bidCol[i] = (fzd.keyValue + static_cast<int64_t>(i)) % 10;
        tsCol[i] = fzd.timestamp + static_cast<int64_t>(i) * 1000;
    }
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, bidCol.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, tsCol.data()));

    for (int i = 0; i < rowCnt; ++i) {
        vbatch->setRowKind(i, RowKind::INSERT);
        vbatch->setTimestamp(i, tsCol[i]);
    }

    StreamRecord *record = new StreamRecord(vbatch);
    windowAggOperator->processBatch(record);

    delete env2;
    delete taskInfo;
}

static void TestWindowMultipleBatches(const WindowFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "WindowFuzz: Multiple batches crossing window boundaries" << std::endl;

    json parsedJson = json::parse(nexmarkQ5Description);
    std::string uniqueName = "org.apache.flink.table.runtime.operators.window.AggregateWindowOperator";
    omnistream::OperatorConfig opConfig(
        uniqueName,
        "LocalWindowAgg_By_Simple",
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]
    );

    auto *output = new BatchOutputTest();
    auto *windowAggOperator = dynamic_cast<AggregateWindowOperator<RowData *, TimeWindow> *>(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(opConfig, output));

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    windowAggOperator->initializeState(initializer, new LongSerializer());
    windowAggOperator->open();

    uint16_t batchCount = (loopCount % 5) + 2;
    for (uint16_t b = 0; b < batchCount; ++b) {
        int rowCnt = (loopCount % 10) + 2;
        auto *vbatch = new omnistream::VectorBatch(rowCnt);

        std::vector<int64_t> bidCol(rowCnt);
        std::vector<int64_t> tsCol(rowCnt);
        for (int i = 0; i < rowCnt; ++i) {
            bidCol[i] = (fzd.keyValue + static_cast<int64_t>(i + b * rowCnt)) % 8;
            tsCol[i] = fzd.timestamp + static_cast<int64_t>(b * 15000 + i * 2000);
        }
        vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, bidCol.data()));
        vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, tsCol.data()));

        for (int i = 0; i < rowCnt; ++i) {
            vbatch->setRowKind(i, RowKind::INSERT);
            vbatch->setTimestamp(i, tsCol[i]);
        }

        StreamRecord *record = new StreamRecord(vbatch);
        windowAggOperator->processBatch(record);
    }

    delete env2;
    delete taskInfo;
}

int WindowFuzz(struct WindowFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 2) {
            case 0:
                TestWindowTumbling(fzd, loopCount);
                break;
            case 1:
                TestWindowMultipleBatches(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "WindowFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
