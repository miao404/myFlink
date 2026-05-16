/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for RowTimeDeduplicateFunction covering keepLastRow/keepFirstRow modes,
 *              different data types, and various rowtime distributions.
 */

#include "table_fuzz_wrapper.h"
#include "dt/common/dt_fuzz_data.h"
#include "dt/common/dt_fuzz_factory_util.h"
#include "dt/common/runtime_env_util.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "table/runtime/operators/deduplicate/RowTimeDeduplicateFunction.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/RowDataSerializer.h"
#include "core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include <test/util/test_util.h>

using json = nlohmann::json;
using namespace DtFuzzFactoryUtil;
using namespace DtRuntimeEnvUtil;

static void TestDeduplicateKeepLast(const TableFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "DeduplicateFuzz: KeepLastRow" << std::endl;

    json config = CreateDeduplicateConfig(
        {"BIGINT", "BIGINT", "BIGINT"},
        {0},
        2,
        true,
        false,
        true);

    auto *dedup = new RowTimeDeduplicateFunction(config);
    auto *output = new BatchOutputTest();
    json opConfig;
    opConfig["originDescription"] = nullptr;
    opConfig["inputTypes"] = {"BIGINT", "BIGINT", "BIGINT"};
    opConfig["outputTypes"] = {"BIGINT", "BIGINT", "BIGINT"};

    KeyedProcessOperator<RowData *, omnistream::VectorBatch *, omnistream::VectorBatch *> keyedProcessOperator(
        dedup, output, opConfig);
    keyedProcessOperator.setup();

    auto rowFields = CreateRowFields({"BIGINT", "BIGINT", "BIGINT"});
    auto *ctx = CreateRuntimeEnv("HashMapStateBackend", rowFields);
    keyedProcessOperator.initializeState(ctx->initializer, ctx->serializer);
    keyedProcessOperator.open();

    int rowCnt = (loopCount % 20) + 2;
    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    std::vector<long> col2(rowCnt);

    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = (fzd.longValue + i) % 5;
        col1[i] = fzd.longValue2 + i * 10;
        col2[i] = fzd.timestampMillis + i * 1000;
    }

    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));

    StreamRecord *record = new StreamRecord(vb);
    keyedProcessOperator.processBatch(record);

    delete ctx;
}

static void TestDeduplicateKeepFirst(const TableFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "DeduplicateFuzz: KeepFirstRow" << std::endl;

    json config = CreateDeduplicateConfig(
        {"BIGINT", "BIGINT", "BIGINT"},
        {0},
        2,
        false,
        true,
        true);

    auto *dedup = new RowTimeDeduplicateFunction(config);
    auto *output = new BatchOutputTest();
    json opConfig;
    opConfig["originDescription"] = nullptr;
    opConfig["inputTypes"] = {"BIGINT", "BIGINT", "BIGINT"};
    opConfig["outputTypes"] = {"BIGINT", "BIGINT", "BIGINT"};

    KeyedProcessOperator<RowData *, omnistream::VectorBatch *, omnistream::VectorBatch *> keyedProcessOperator(
        dedup, output, opConfig);
    keyedProcessOperator.setup();

    auto rowFields = CreateRowFields({"BIGINT", "BIGINT", "BIGINT"});
    auto *ctx = CreateRuntimeEnv("HashMapStateBackend", rowFields);
    keyedProcessOperator.initializeState(ctx->initializer, ctx->serializer);
    keyedProcessOperator.open();

    int rowCnt = (loopCount % 15) + 2;
    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    std::vector<long> col2(rowCnt);

    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = (fzd.longValue + i) % 3;
        col1[i] = fzd.longValue2 - i * 5;
        col2[i] = fzd.timestampMillis + i * 500;
    }

    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));

    StreamRecord *record = new StreamRecord(vb);
    keyedProcessOperator.processBatch(record);

    delete ctx;
}

int DeduplicateFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseDedupMode)
{
    try {
        switch (chooseDedupMode % 2) {
            case 0:
                TestDeduplicateKeepLast(fzd, loopCount);
                break;
            case 1:
                TestDeduplicateKeepFirst(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "DeduplicateFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
