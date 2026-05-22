/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for KeyedProcessOperator covering GroupAggFunction,
 *              MockUserFunction, and batch processing modes with various data types.
 */

#include "streaming_fuzz_wrapper.h"
#include "dt_fuzz_data.h"
#include "dt_fuzz_factory_util.h"
#include "runtime_env_util.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "streaming/api/operators/KeyedProcessOperator.h"
#include "table/runtime/operators/aggregate/GroupAggFunction.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/RowData.h"
#include "table/typeutils/RowDataSerializer.h"
#include "core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"

using json = nlohmann::json;
using namespace DtFuzzFactoryUtil;
using namespace DtRuntimeEnvUtil;

class FuzzMockUserFunction : public KeyedProcessFunction<RowData *, RowData *, RowData *> {
public:
    void open(const Configuration &parameters) override {}
    void processElement(RowData *input, Context *ctx, TimestampedCollector *out) override
    {
        processCount++;
    }
    void processBatch(omnistream::VectorBatch *inputBatch, Context &ctx, TimestampedCollector &out) override {}
    JoinedRowData *getResultRow() override { return nullptr; }
    ValueState<RowData *> *getValueState() override { return nullptr; }
    int processCount = 0;
};

static void TestKeyedProcessWithGroupAgg(const StreamingFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "KeyedProcessFuzz: GroupAggFunction" << std::endl;

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

    uint16_t count = (loopCount % 50) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        int64_t keyVal = (fzd.longValue + static_cast<int64_t>(i)) % 10;
        int64_t aggVal = fzd.longValue2 + static_cast<int64_t>(i) * 7;

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

static void TestKeyedProcessWithMock(const StreamingFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "KeyedProcessFuzz: MockUserFunction" << std::endl;

    json config;
    config["originDescription"] = nullptr;
    config["inputTypes"] = {"BIGINT", "BIGINT"};
    config["outputTypes"] = {"BIGINT", "BIGINT"};
    config["grouping"] = {0};
    config["distinctInfos"] = json::array();
    json aggInfoList;
    aggInfoList["aggregateCalls"] = json::array({
        {{"name", "AVG($1)"}, {"aggregationFunction", "LongAvgAggFunction"},
         {"argIndexes", {1}}, {"consumeRetraction", "true"}, {"filterArg", -1}}
    });
    aggInfoList["accTypes"] = {"BIGINT", "BIGINT", "BIGINT"};
    aggInfoList["aggValueTypes"] = {"BIGINT"};
    aggInfoList["indexOfCountStar"] = 2;
    config["aggInfoList"] = aggInfoList;

    auto *mockFunc = new FuzzMockUserFunction();
    auto *output = new OutputTest();
    KeyedProcessOperator<RowData *, RowData *, RowData *> keyedProcessOperator(mockFunc, output, config);
    keyedProcessOperator.setup();

    auto rowFields = CreateRowFields({"BIGINT", "BIGINT"});
    auto *ctx = CreateRuntimeEnv("HashMapStateBackend", rowFields);
    InitializeOperatorState(keyedProcessOperator, ctx);
    keyedProcessOperator.open();

    uint16_t count = (loopCount % 30) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setInt(0, static_cast<int>(fzd.intValue + i));
        row->setInt(1, static_cast<int>(fzd.longValue + i * 3));

        StreamRecord *record = new StreamRecord(reinterpret_cast<void *>(row));
        keyedProcessOperator.setCurrentKey(row);
        keyedProcessOperator.processElement(record);
        delete record;
    }

    delete ctx;
}

static void TestKeyedProcessMultipleKeys(const StreamingFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "KeyedProcessFuzz: MultipleKeys" << std::endl;

    json config = CreateGroupAggConfig(
        "LongMaxAggFunction", "MAX($2)",
        {"BIGINT", "BIGINT", "BIGINT"}, {"BIGINT", "BIGINT", "BIGINT"},
        {0, 1}, {2},
        {"BIGINT"}, {"BIGINT"}, -1);

    auto *groupAgg = new GroupAggFunction(1L, config);
    auto *output = new OutputTest();
    KeyedProcessOperator<RowData *, RowData *, RowData *> keyedProcessOperator(groupAgg, output, config);
    keyedProcessOperator.setup();

    auto rowFields = CreateRowFields({"BIGINT", "BIGINT"});
    auto *ctx = CreateRuntimeEnv("HashMapStateBackend", rowFields);
    InitializeOperatorState(keyedProcessOperator, ctx);
    keyedProcessOperator.open();

    uint16_t count = (loopCount % 40) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        int64_t key1 = (fzd.longValue + static_cast<int64_t>(i)) % 5;
        int64_t key2 = (fzd.longValue2 + static_cast<int64_t>(i)) % 3;
        int64_t val = fzd.longValue + static_cast<int64_t>(i) * 11;

        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(3);
        row->setLong(0, key1);
        row->setLong(1, key2);
        row->setLong(2, val);

        StreamRecord *record = new StreamRecord(reinterpret_cast<void *>(row));
        keyedProcessOperator.setCurrentKey(row);
        keyedProcessOperator.processElement(record);
        delete record;
    }

    delete ctx;
}

int KeyedProcessFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 3) {
            case 0:
                TestKeyedProcessWithGroupAgg(fzd, loopCount);
                break;
            case 1:
                TestKeyedProcessWithMock(fzd, loopCount);
                break;
            case 2:
                TestKeyedProcessMultipleKeys(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "KeyedProcessFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
