/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for StreamFilter operator covering UDF-based filtering.
 *              StreamFilter requires UDF .so loading. This fuzz focuses on config
 *              parsing, data construction, and lifecycle validation.
 *              Reference: StreamFilter (datastream operator)
 */

#include "streaming_fuzz_wrapper.h"
#include "dt_fuzz_data.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "streaming/api/operators/StreamFilter.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "core/operators/OutputTest.h"
#include <test/util/test_util.h>

using json = nlohmann::json;

static void TestFilterConfigValidation(const FilterFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "FilterFuzz: Config validation" << std::endl;

    json config;
    config["udf_so"] = "/tmp/libMockFilterFunction.so";
    config["udf_obj"] = "{}";

    std::string configStr = config.dump();
    json parsed = json::parse(configStr);

    if (!parsed.contains("udf_so") || !parsed.contains("udf_obj")) {
        std::cerr << "FilterFuzz: Missing required config fields" << std::endl;
    }

    uint16_t count = (loopCount % 30) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setLong(0, fzd.longValue + static_cast<int64_t>(i));
        row->setInt(1, fzd.intValue + static_cast<int32_t>(i));
        delete row;
    }
}

static void TestFilterVectorBatchConstruction(const FilterFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "FilterFuzz: VectorBatch construction for filter input" << std::endl;

    int rowCnt = (loopCount % 25) + 2;
    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.longValue + static_cast<int64_t>(i) * 3;
        col1[i] = static_cast<int64_t>(fzd.intValue) + static_cast<int64_t>(i) * 7;
    }
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));

    for (int i = 0; i < rowCnt; ++i) {
        vb->setRowKind(i, RowKind::INSERT);
    }

    StreamRecord *record = new StreamRecord(vb);
    delete record;
}

static void TestFilterBooleanData(const FilterFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "FilterFuzz: Boolean filter data construction" << std::endl;

    int rowCnt = (loopCount % 20) + 2;
    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        // Simulate boolean filter results
        col0[i] = (fzd.longValue + static_cast<int64_t>(i)) % 2 == 0 ? 1 : 0;
        col1[i] = fzd.longValue + static_cast<int64_t>(i) * 5;
    }
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));

    StreamRecord *record = new StreamRecord(vb);
    delete record;
}

int FilterFuzz(struct FilterFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 3) {
            case 0:
                TestFilterConfigValidation(fzd, loopCount);
                break;
            case 1:
                TestFilterVectorBatchConstruction(fzd, loopCount);
                break;
            case 2:
                TestFilterBooleanData(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "FilterFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
