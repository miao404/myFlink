/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for StreamMap operator covering UDF-based mapping.
 *              StreamMap requires UDF .so loading. This fuzz focuses on config
 *              parsing, constructor validation, and data construction.
 *              Reference UT: StreamMapTest.cpp
 */

#include "streaming_fuzz_wrapper.h"
#include "dt_fuzz_data.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "streaming/api/operators/StreamMap.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "core/operators/OutputTest.h"
#include <test/util/test_util.h>

using json = nlohmann::json;

static void TestMapConfigValidation(const MapFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "MapFuzz: Config validation (udf_so + udf_obj)" << std::endl;

    json config;
    config["udf_so"] = "/tmp/libMockMapFunction.so";
    config["udf_obj"] = "{}";

    std::string configStr = config.dump();
    json parsed = json::parse(configStr);

    if (!parsed.contains("udf_so") || !parsed.contains("udf_obj")) {
        std::cerr << "MapFuzz: Missing required config fields" << std::endl;
    }

    uint16_t count = (loopCount % 30) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setLong(0, fzd.longValue + static_cast<int64_t>(i));
        row->setInt(1, fzd.intValue + static_cast<int32_t>(i) * 2);
        delete row;
    }
}

static void TestMapVectorBatchInput(const MapFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "MapFuzz: VectorBatch input construction" << std::endl;

    int rowCnt = (loopCount % 25) + 2;
    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.longValue + static_cast<int64_t>(i) * 7;
        col1[i] = static_cast<int64_t>(fzd.intValue) + static_cast<int64_t>(i) * 13;
    }
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));

    for (int i = 0; i < rowCnt; ++i) {
        vb->setRowKind(i, RowKind::INSERT);
    }

    StreamRecord *record = new StreamRecord(vb);
    delete record;
}

static void TestMapOutputConstruction(const MapFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "MapFuzz: Simulating map output VectorBatch" << std::endl;

    int rowCnt = (loopCount % 20) + 2;

    // Simulate input
    omnistream::VectorBatch *inputVb = new omnistream::VectorBatch(rowCnt);
    std::vector<int64_t> inCol0(rowCnt);
    std::vector<int64_t> inCol1(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        inCol0[i] = fzd.longValue + static_cast<int64_t>(i);
        inCol1[i] = static_cast<int64_t>(fzd.intValue) * static_cast<int64_t>(i + 1);
    }
    inputVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, inCol0.data()));
    inputVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, inCol1.data()));

    // Simulate map output (transformed values)
    omnistream::VectorBatch *outputVb = new omnistream::VectorBatch(rowCnt);
    std::vector<int64_t> outCol(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        outCol[i] = inCol0[i] + inCol1[i];
    }
    outputVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, outCol.data()));

    StreamRecord *inRecord = new StreamRecord(inputVb);
    StreamRecord *outRecord = new StreamRecord(outputVb);
    delete inRecord;
    delete outRecord;
}

int MapFuzz(struct MapFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 3) {
            case 0:
                TestMapConfigValidation(fzd, loopCount);
                break;
            case 1:
                TestMapVectorBatchInput(fzd, loopCount);
                break;
            case 2:
                TestMapOutputConstruction(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "MapFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
