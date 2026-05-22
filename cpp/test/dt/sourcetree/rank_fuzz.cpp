/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for AbstractTopNFunction/AppendOnlyTopNFunction and FastTop1Function
 *              covering different partition keys, sort orders, and rank ranges.
 */

#include "table_fuzz_wrapper.h"
#include "dt_fuzz_data.h"
#include "dt_fuzz_factory_util.h"
#include "runtime_env_util.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "table/runtime/operators/rank/AppendOnlyTopNFunction.h"
#include "table/runtime/operators/rank/FastTop1Function.h"
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

static void TestAppendOnlyTopN(const TableFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "RankFuzz: AppendOnlyTopNFunction" << std::endl;

    json config = CreateRankConfig(
        "AppendOnlyTopNFunction",
        {"BIGINT", "BIGINT", "BIGINT"},
        {"BIGINT", "BIGINT", "BIGINT"},
        {0},
        {1},
        {false},
        {true},
        true,
        "rankStart=1, rankEnd=3",
        false);

    auto *topNFunction = new AppendOnlyTopNFunction<long>(config);
    auto *output = new BatchOutputTest();
    KeyedProcessOperator keyedProcessOperator(topNFunction, output, config);
    keyedProcessOperator.setup();

    auto rowFields = CreateRowFields({"BIGINT", "BIGINT", "BIGINT"});
    auto *ctx = CreateRuntimeEnv("HashMapStateBackend", rowFields);
    keyedProcessOperator.initializeState(ctx->initializer, ctx->serializer);
    keyedProcessOperator.open();

    int rowCnt = (loopCount % 15) + 3;
    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    std::vector<long> col2(rowCnt);

    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = (fzd.longValue + i) % 4 + 1;
        col1[i] = fzd.longValue2 + i * 10;
        col2[i] = fzd.longValue3 + i * 5;
    }

    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));

    StreamRecord *record = new StreamRecord(vb);
    keyedProcessOperator.processBatch(record);

    delete ctx;
}

static void TestFastTop1(const TableFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "RankFuzz: FastTop1Function" << std::endl;

    json config = CreateRankConfig(
        "FastTop1Function",
        {"BIGINT", "BIGINT", "BIGINT"},
        {"BIGINT", "BIGINT", "BIGINT"},
        {0},
        {1, 2},
        {false, true},
        {true, false},
        false,
        "rankStart=1, rankEnd=1",
        false);

    auto *fastTop1 = new FastTop1Function<long>(config);
    auto *output = new BatchOutputTest();
    KeyedProcessOperator keyedProcessOperator(fastTop1, output, config);
    keyedProcessOperator.setup();

    std::vector<omnistream::RowField> rowFields = {
        omnistream::RowField("col1", BasicLogicalType::BIGINT),
        omnistream::RowField("col2", BasicLogicalType::BIGINT),
        omnistream::RowField("col3", BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE)
    };
    auto *ctx = CreateRuntimeEnv("HashMapStateBackend", rowFields);
    keyedProcessOperator.initializeState(ctx->initializer, ctx->serializer);
    keyedProcessOperator.open();

    int rowCnt = (loopCount % 12) + 3;
    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    std::vector<long> col2(rowCnt);

    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = (fzd.longValue + i) % 5 + 1;
        col1[i] = fzd.longValue2 + i * 7;
        col2[i] = fzd.longValue3 + i * 3;
    }

    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));

    StreamRecord *record = new StreamRecord(vb);
    keyedProcessOperator.processBatch(record);

    delete ctx;
}

int RankFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseRankFunc)
{
    try {
        switch (chooseRankFunc % 2) {
            case 0:
                TestAppendOnlyTopN(fzd, loopCount);
                break;
            case 1:
                TestFastTop1(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "RankFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
