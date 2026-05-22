/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for StreamCalcBatch covering projection, filter conditions,
 *              and expression evaluation with various BIGINT column inputs.
 *              Reference UT: StreamCalcBatchTest.cpp
 */

#include "streaming_fuzz_wrapper.h"
#include "dt_fuzz_data.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "table/runtime/operators/calc/StreamCalcBatch.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "core/operators/OutputTest.h"
#include <test/util/test_util.h>

using json = nlohmann::json;

static const std::string projectionDesc = R"DELIM({
    "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
    "outputTypes": ["BIGINT", "BIGINT"],
    "calcProjection": [0, 2],
    "calcCondition": null
})DELIM";

static const std::string filterDesc = R"DELIM({
    "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
    "outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
    "calcProjection": [0, 1, 2],
    "calcCondition": {"op": ">", "field": 0, "value": 0}
})DELIM";

static void TestCalcProjection(const CalcFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "CalcFuzz: Projection (select col0, col2)" << std::endl;

    json parsedJson = json::parse(projectionDesc);
    OutputTestVectorBatch *output = new OutputTestVectorBatch();
    StreamCalcBatch streamCalcBatchOp(parsedJson, output);
    streamCalcBatchOp.open();

    int rowCnt = (loopCount % 20) + 2;
    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    std::vector<int64_t> col2(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.col0 + static_cast<int64_t>(i);
        col1[i] = fzd.col1 + static_cast<int64_t>(i) * 2;
        col2[i] = fzd.col2 + static_cast<int64_t>(i) * 3;
    }
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));

    StreamRecord *record = new StreamRecord(vb);
    streamCalcBatchOp.processBatch(record);

    delete output;
}

static void TestCalcFilter(const CalcFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "CalcFuzz: Filter condition (col0 > 0)" << std::endl;

    json parsedJson = json::parse(filterDesc);
    OutputTestVectorBatch *output = new OutputTestVectorBatch();
    StreamCalcBatch streamCalcBatchOp(parsedJson, output);
    streamCalcBatchOp.open();

    int rowCnt = (loopCount % 15) + 3;
    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    std::vector<int64_t> col2(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        // Mix positive and negative values to test filter
        col0[i] = fzd.col0 + static_cast<int64_t>(i) - static_cast<int64_t>(rowCnt / 2);
        col1[i] = fzd.col1 + static_cast<int64_t>(i) * 5;
        col2[i] = fzd.col2 - static_cast<int64_t>(i);
    }
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));

    StreamRecord *record = new StreamRecord(vb);
    streamCalcBatchOp.processBatch(record);

    delete output;
}

static void TestCalcWithRowKind(const CalcFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "CalcFuzz: Projection with RowKind variations" << std::endl;

    json parsedJson = json::parse(projectionDesc);
    OutputTestVectorBatch *output = new OutputTestVectorBatch();
    StreamCalcBatch streamCalcBatchOp(parsedJson, output);
    streamCalcBatchOp.open();

    int rowCnt = (loopCount % 12) + 4;
    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    std::vector<int64_t> col2(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.col0 * static_cast<int64_t>(i + 1);
        col1[i] = fzd.col1 + static_cast<int64_t>(i * 7);
        col2[i] = fzd.col2 + static_cast<int64_t>(i * 11);
    }
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));

    RowKind kinds[] = {RowKind::INSERT, RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER, RowKind::DELETE};
    for (int i = 0; i < rowCnt; ++i) {
        vb->setRowKind(i, kinds[i % 4]);
    }

    StreamRecord *record = new StreamRecord(vb);
    streamCalcBatchOp.processBatch(record);

    delete output;
}

int CalcFuzz(struct CalcFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 3) {
            case 0:
                TestCalcProjection(fzd, loopCount);
                break;
            case 1:
                TestCalcFilter(fzd, loopCount);
                break;
            case 2:
                TestCalcWithRowKind(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "CalcFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
