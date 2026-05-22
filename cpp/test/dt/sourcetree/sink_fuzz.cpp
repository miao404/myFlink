/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for SinkOperator covering VectorBatch-based output
 *              with various column types and row counts.
 *              Reference UT: SinkOperatorTest.cpp
 */

#include "table_fuzz_wrapper.h"
#include "dt_fuzz_data.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "streaming/api/operators/SinkOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "core/operators/OutputTest.h"
#include <test/util/test_util.h>

using json = nlohmann::json;

static void TestSinkProcessElement(const SinkFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "SinkFuzz: processElement with BinaryRowData" << std::endl;

    std::string sinkDescription = R"({"outputfile":"/tmp/flink_fuzz_output.txt"})";
    auto obj = json::parse(sinkDescription);
    auto op = SinkOperator(obj);
    op.open();

    uint16_t count = (loopCount % 20) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        BinaryRowData *rowData = BinaryRowData::createBinaryRowDataWithMem(2);
        rowData->setLong(0, fzd.longCol + static_cast<int64_t>(i));
        rowData->setLong(1, fzd.longCol2 + static_cast<int64_t>(i) * 10);

        StreamRecord *record = new StreamRecord(rowData);
        op.processElement(record);
        delete record;
    }
}

static void TestSinkProcessBatch(const SinkFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "SinkFuzz: processBatch with VectorBatch" << std::endl;

    std::string sinkDescription = R"({"outputfile":"/tmp/flink_fuzz_output.txt"})";
    auto obj = json::parse(sinkDescription);
    auto op = SinkOperator(obj);
    op.open();

    int rowCnt = (loopCount % 20) + 2;
    auto *vbatch = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.longCol + static_cast<int64_t>(i) * 100;
        col1[i] = fzd.longCol2 + static_cast<int64_t>(i) * 50;
    }
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));

    for (int i = 0; i < rowCnt; ++i) {
        vbatch->setRowKind(i, RowKind::INSERT);
    }

    StreamRecord *record = new StreamRecord(vbatch);
    op.processBatch(record);
    delete record;
}

static void TestSinkWithRowKindVariations(const SinkFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "SinkFuzz: VectorBatch with mixed RowKind" << std::endl;

    std::string sinkDescription = R"({"outputfile":"/tmp/flink_fuzz_output.txt"})";
    auto obj = json::parse(sinkDescription);
    auto op = SinkOperator(obj);
    op.open();

    int rowCnt = (loopCount % 15) + 3;
    auto *vbatch = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.longCol + static_cast<int64_t>(i);
        col1[i] = fzd.longCol2 - static_cast<int64_t>(i) * 3;
    }
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));

    RowKind kinds[] = {RowKind::INSERT, RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER, RowKind::DELETE};
    for (int i = 0; i < rowCnt; ++i) {
        vbatch->setRowKind(i, kinds[i % 4]);
    }

    StreamRecord *record = new StreamRecord(vbatch);
    op.processBatch(record);
    op.finish();
    delete record;
}

int SinkFuzz(struct SinkFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 3) {
            case 0:
                TestSinkProcessElement(fzd, loopCount);
                break;
            case 1:
                TestSinkProcessBatch(fzd, loopCount);
                break;
            case 2:
                TestSinkWithRowKindVariations(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "SinkFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
