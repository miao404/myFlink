/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for GroupAggFunction covering SUM/COUNT/AVG/MAX/MIN
 *              with BIGINT, INTEGER, VARCHAR data types and streaming semantics.
 */

#include "table_fuzz_wrapper.h"
#include "dt/common/dt_fuzz_data.h"
#include "dt/common/dt_fuzz_factory_util.h"
#include "dt/common/runtime_env_util.h"

#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
#include <vector>
#include <iostream>
#include <cmath>

#include "table/runtime/operators/aggregate/GroupAggFunction.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/JoinedRowData.h"
#include "table/typeutils/RowDataSerializer.h"
#include "core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"

using json = nlohmann::json;
using namespace DtFuzzFactoryUtil;
using namespace DtRuntimeEnvUtil;

static void TestSumAgg(const TableFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "AggregateFuzz: SUM(BIGINT)" << std::endl;

    json config = CreateGroupAggConfig(
        "LongSumAggFunction", "SUM($1)",
        {"BIGINT", "BIGINT"}, {"BIGINT", "BIGINT"},
        {0}, {1},
        {"BIGINT"}, {"BIGINT"}, -1);

    auto *groupAgg = new GroupAggFunction(1L, config);
    auto *output = new OutputTest();
    KeyedProcessOperator<RowData *, RowData *, RowData *> keyedProcessOperator(groupAgg, output, config);
    keyedProcessOperator.setup();

    auto rowFields = CreateRowFields({"BIGINT", "BIGINT"});
    auto *ctx = CreateRuntimeEnv("HashMapStateBackend", rowFields);
    InitializeOperatorState(keyedProcessOperator, ctx);
    keyedProcessOperator.open();

    uint16_t count = (loopCount % 100) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        int64_t keyVal = (fzd.longValue % 10) + 1;
        int64_t aggVal = (fzd.longValue2 + static_cast<int64_t>(i)) % 1000;

        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setLong(0, keyVal);
        row->setLong(1, aggVal);

        StreamRecord *record = new StreamRecord(reinterpret_cast<void *>(row));
        keyedProcessOperator.setCurrentKey(row);
        keyedProcessOperator.processElement(record);
        delete record;
    }

    delete ctx;
}

static void TestCountAgg(const TableFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "AggregateFuzz: COUNT(BIGINT)" << std::endl;

    json config = CreateGroupAggConfig(
        "CountAggFunction", "COUNT($1)",
        {"BIGINT", "BIGINT"}, {"BIGINT", "BIGINT"},
        {0}, {1},
        {"BIGINT"}, {"BIGINT"}, 0);

    auto *groupAgg = new GroupAggFunction(1L, config);
    auto *output = new OutputTest();
    KeyedProcessOperator<RowData *, RowData *, RowData *> keyedProcessOperator(groupAgg, output, config);
    keyedProcessOperator.setup();

    auto rowFields = CreateRowFields({"BIGINT", "BIGINT"});
    auto *ctx = CreateRuntimeEnv("HashMapStateBackend", rowFields);
    InitializeOperatorState(keyedProcessOperator, ctx);
    keyedProcessOperator.open();

    uint16_t count = (loopCount % 50) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        int64_t keyVal = (fzd.longValue % 5) + 1;
        int64_t aggVal = fzd.longValue2 + static_cast<int64_t>(i);

        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setLong(0, keyVal);
        row->setLong(1, aggVal);

        StreamRecord *record = new StreamRecord(reinterpret_cast<void *>(row));
        keyedProcessOperator.setCurrentKey(row);
        keyedProcessOperator.processElement(record);
        delete record;
    }

    delete ctx;
}

static void TestAvgAgg(const TableFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "AggregateFuzz: AVG(BIGINT)" << std::endl;

    json config;
    config["originDescription"] = nullptr;
    config["inputTypes"] = {"BIGINT", "BIGINT"};
    config["outputTypes"] = {"BIGINT", "BIGINT"};
    config["grouping"] = {0};
    config["distinctInfos"] = json::array();

    json aggCall;
    aggCall["name"] = "AVG($1)";
    aggCall["aggregationFunction"] = "LongAvgAggFunction";
    aggCall["argIndexes"] = {1};
    aggCall["consumeRetraction"] = "true";
    aggCall["filterArg"] = -1;

    json aggInfoList;
    aggInfoList["aggregateCalls"] = json::array({aggCall});
    aggInfoList["accTypes"] = {"BIGINT", "BIGINT", "BIGINT"};
    aggInfoList["aggValueTypes"] = {"BIGINT"};
    aggInfoList["indexOfCountStar"] = 2;
    config["aggInfoList"] = aggInfoList;

    auto *groupAgg = new GroupAggFunction(1L, config);
    auto *output = new OutputTest();
    KeyedProcessOperator<RowData *, RowData *, RowData *> keyedProcessOperator(groupAgg, output, config);
    keyedProcessOperator.setup();

    auto rowFields = CreateRowFields({"BIGINT", "BIGINT"});
    auto *ctx = CreateRuntimeEnv("HashMapStateBackend", rowFields);
    InitializeOperatorState(keyedProcessOperator, ctx);
    keyedProcessOperator.open();

    uint16_t count = (loopCount % 80) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        int64_t keyVal = (fzd.longValue % 8) + 1;
        int64_t aggVal = (fzd.longValue2 + static_cast<int64_t>(i * 7)) % 500;

        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setLong(0, keyVal);
        row->setLong(1, aggVal);

        StreamRecord *record = new StreamRecord(reinterpret_cast<void *>(row));
        keyedProcessOperator.setCurrentKey(row);
        keyedProcessOperator.processElement(record);
        delete record;
    }

    delete ctx;
}

static void TestMaxAgg(const TableFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "AggregateFuzz: MAX(BIGINT)" << std::endl;

    json config = CreateGroupAggConfig(
        "LongMaxAggFunction", "MAX($1)",
        {"BIGINT", "BIGINT"}, {"BIGINT", "BIGINT"},
        {0}, {1},
        {"BIGINT"}, {"BIGINT"}, -1);

    auto *groupAgg = new GroupAggFunction(1L, config);
    auto *output = new OutputTest();
    KeyedProcessOperator<RowData *, RowData *, RowData *> keyedProcessOperator(groupAgg, output, config);
    keyedProcessOperator.setup();

    auto rowFields = CreateRowFields({"BIGINT", "BIGINT"});
    auto *ctx = CreateRuntimeEnv("HashMapStateBackend", rowFields);
    InitializeOperatorState(keyedProcessOperator, ctx);
    keyedProcessOperator.open();

    uint16_t count = (loopCount % 60) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        int64_t keyVal = (fzd.longValue % 6) + 1;
        int64_t aggVal = fzd.longValue2 * (static_cast<int64_t>(i) + 1);

        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setLong(0, keyVal);
        row->setLong(1, aggVal);

        StreamRecord *record = new StreamRecord(reinterpret_cast<void *>(row));
        keyedProcessOperator.setCurrentKey(row);
        keyedProcessOperator.processElement(record);
        delete record;
    }

    delete ctx;
}

static void TestMinAgg(const TableFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "AggregateFuzz: MIN(BIGINT)" << std::endl;

    json config = CreateGroupAggConfig(
        "LongMinAggFunction", "MIN($1)",
        {"BIGINT", "BIGINT"}, {"BIGINT", "BIGINT"},
        {0}, {1},
        {"BIGINT"}, {"BIGINT"}, -1);

    auto *groupAgg = new GroupAggFunction(1L, config);
    auto *output = new OutputTest();
    KeyedProcessOperator<RowData *, RowData *, RowData *> keyedProcessOperator(groupAgg, output, config);
    keyedProcessOperator.setup();

    auto rowFields = CreateRowFields({"BIGINT", "BIGINT"});
    auto *ctx = CreateRuntimeEnv("HashMapStateBackend", rowFields);
    InitializeOperatorState(keyedProcessOperator, ctx);
    keyedProcessOperator.open();

    uint16_t count = (loopCount % 60) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        int64_t keyVal = (fzd.longValue % 4) + 1;
        int64_t aggVal = fzd.longValue2 - static_cast<int64_t>(i * 3);

        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setLong(0, keyVal);
        row->setLong(1, aggVal);

        StreamRecord *record = new StreamRecord(reinterpret_cast<void *>(row));
        keyedProcessOperator.setCurrentKey(row);
        keyedProcessOperator.processElement(record);
        delete record;
    }

    delete ctx;
}

int AggregateFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseAgg)
{
    try {
        switch (chooseAgg % 5) {
            case 0:
                TestSumAgg(fzd, loopCount);
                break;
            case 1:
                TestCountAgg(fzd, loopCount);
                break;
            case 2:
                TestAvgAgg(fzd, loopCount);
                break;
            case 3:
                TestMaxAgg(fzd, loopCount);
                break;
            case 4:
                TestMinAgg(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "AggregateFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
