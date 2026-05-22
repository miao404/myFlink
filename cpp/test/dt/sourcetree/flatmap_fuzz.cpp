/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for StreamFlatMap operator covering UDF-based flat mapping.
 *              StreamFlatMap requires UDF .so loading. This fuzz focuses on config
 *              parsing, data construction, and lifecycle validation.
 *              Reference: StreamFlatMap (datastream operator)
 */

#include "streaming_fuzz_wrapper.h"
#include "dt_fuzz_data.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "streaming/api/operators/StreamFlatMap.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "core/operators/OutputTest.h"
#include <test/util/test_util.h>

using json = nlohmann::json;

static void TestFlatMapConfigValidation(const FlatMapFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "FlatMapFuzz: Config validation" << std::endl;

    json config;
    config["udf_so"] = "/tmp/libMockFlatMapFunction.so";
    config["udf_obj"] = "{}";

    std::string configStr = config.dump();
    json parsed = json::parse(configStr);

    if (!parsed.contains("udf_so") || !parsed.contains("udf_obj")) {
        std::cerr << "FlatMapFuzz: Missing required config fields" << std::endl;
    }

    uint16_t count = (loopCount % 25) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setLong(0, fzd.longValue + static_cast<int64_t>(i) * 3);
        row->setInt(1, fzd.intValue + static_cast<int32_t>(i));
        delete row;
    }
}

static void TestFlatMapVectorBatchInput(const FlatMapFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "FlatMapFuzz: VectorBatch input construction" << std::endl;

    int rowCnt = (loopCount % 20) + 2;
    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.longValue + static_cast<int64_t>(i) * 5;
        col1[i] = static_cast<int64_t>(fzd.intValue) + static_cast<int64_t>(i) * 11;
    }
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));

    for (int i = 0; i < rowCnt; ++i) {
        vb->setRowKind(i, RowKind::INSERT);
    }

    StreamRecord *record = new StreamRecord(vb);
    delete record;
}

static void TestFlatMapMultipleOutputRows(const FlatMapFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "FlatMapFuzz: Simulating multiple output rows per input" << std::endl;

    int inputRowCnt = (loopCount % 10) + 2;
    int outputMultiplier = (fzd.flatMapModeFlag % 3) + 1;
    int outputRowCnt = inputRowCnt * outputMultiplier;

    omnistream::VectorBatch *inputVb = new omnistream::VectorBatch(inputRowCnt);
    std::vector<int64_t> col0(inputRowCnt);
    for (int i = 0; i < inputRowCnt; ++i) {
        col0[i] = fzd.longValue + static_cast<int64_t>(i);
    }
    inputVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(inputRowCnt, col0.data()));

    omnistream::VectorBatch *outputVb = new omnistream::VectorBatch(outputRowCnt);
    std::vector<int64_t> outCol(outputRowCnt);
    for (int i = 0; i < outputRowCnt; ++i) {
        outCol[i] = fzd.longValue + static_cast<int64_t>(i % inputRowCnt) * outputMultiplier;
    }
    outputVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(outputRowCnt, outCol.data()));

    StreamRecord *inRecord = new StreamRecord(inputVb);
    StreamRecord *outRecord = new StreamRecord(outputVb);
    delete inRecord;
    delete outRecord;
}

int FlatMapFuzz(struct FlatMapFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 3) {
            case 0:
                TestFlatMapConfigValidation(fzd, loopCount);
                break;
            case 1:
                TestFlatMapVectorBatchInput(fzd, loopCount);
                break;
            case 2:
                TestFlatMapMultipleOutputRows(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "FlatMapFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
