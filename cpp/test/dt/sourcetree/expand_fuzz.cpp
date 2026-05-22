/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for StreamExpand operator covering multi-projection
 *              expansion with BIGINT columns and various row counts.
 *              Reference UT: StreamExpandTest.cpp
 */

#include "streaming_fuzz_wrapper.h"
#include "dt_fuzz_data.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "table/runtime/operators/expand/StreamExpand.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "core/operators/OutputTest.h"
#include <test/util/test_util.h>

using json = nlohmann::json;

static const std::string expandDesc2Projections = R"DELIM({
    "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
    "outputTypes": ["BIGINT", "BIGINT", "BIGINT", "BIGINT"],
    "projects": [[0, 1, "null", "$e"], [0, "null", 2, "$e"]]
})DELIM";

static const std::string expandDesc3Projections = R"DELIM({
    "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
    "outputTypes": ["BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT"],
    "projects": [[0, 1, "null", "null", "$e"], [0, "null", 2, "null", "$e"], [0, "null", "null", 2, "$e"]]
})DELIM";

static void TestExpand2Projections(const ExpandFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "ExpandFuzz: 2 projections" << std::endl;

    json parsedJson = json::parse(expandDesc2Projections);
    OutputTestVectorBatch *output = new OutputTestVectorBatch();
    StreamExpand streamExpandOp(parsedJson, output);
    streamExpandOp.open();

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
    streamExpandOp.processBatch(record);

    delete output;
}

static void TestExpand3Projections(const ExpandFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "ExpandFuzz: 3 projections" << std::endl;

    json parsedJson = json::parse(expandDesc3Projections);
    OutputTestVectorBatch *output = new OutputTestVectorBatch();
    StreamExpand streamExpandOp(parsedJson, output);
    streamExpandOp.open();

    int rowCnt = (loopCount % 15) + 3;
    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    std::vector<int64_t> col2(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.col0 * static_cast<int64_t>(i + 1);
        col1[i] = fzd.col1 + static_cast<int64_t>(i * 5);
        col2[i] = fzd.col2 - static_cast<int64_t>(i * 2);
    }
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));

    StreamRecord *record = new StreamRecord(vb);
    streamExpandOp.processBatch(record);

    delete output;
}

static void TestExpandWithRowKind(const ExpandFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "ExpandFuzz: With RowKind variations" << std::endl;

    json parsedJson = json::parse(expandDesc2Projections);
    OutputTestVectorBatch *output = new OutputTestVectorBatch();
    StreamExpand streamExpandOp(parsedJson, output);
    streamExpandOp.open();

    int rowCnt = (loopCount % 10) + 4;
    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    std::vector<int64_t> col2(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.col0 + static_cast<int64_t>(i) * 10;
        col1[i] = fzd.col1 + static_cast<int64_t>(i) * 20;
        col2[i] = fzd.col2 + static_cast<int64_t>(i) * 30;
    }
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));

    RowKind kinds[] = {RowKind::INSERT, RowKind::UPDATE_AFTER, RowKind::INSERT, RowKind::DELETE};
    for (int i = 0; i < rowCnt; ++i) {
        vb->setRowKind(i, kinds[i % 4]);
    }

    StreamRecord *record = new StreamRecord(vb);
    streamExpandOp.processBatch(record);

    delete output;
}

int ExpandFuzz(struct ExpandFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 3) {
            case 0:
                TestExpand2Projections(fzd, loopCount);
                break;
            case 1:
                TestExpand3Projections(fzd, loopCount);
                break;
            case 2:
                TestExpandWithRowKind(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "ExpandFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
