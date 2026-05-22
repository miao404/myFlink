/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for StreamSource operator (DataStream source) covering
 *              source function configuration and output data construction.
 *              Reference UT: SourceTest.cpp (streaming source operators)
 */

#include "streaming_fuzz_wrapper.h"
#include "dt_fuzz_data.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "core/operators/OutputTest.h"
#include <test/util/test_util.h>

using json = nlohmann::json;

static void TestSourceOperatorConfig(const SourceOperatorFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "SourceOperatorFuzz: Source function config validation" << std::endl;

    json config;
    config["sourceType"] = "InputFormatSourceFunction";
    config["parallelism"] = 1;
    config["inputFormat"] = "csv";
    config["batchSize"] = (loopCount % 5000) + 100;

    json schema;
    schema["fieldNames"] = {"f0", "f1", "f2"};
    schema["fieldTypes"] = {"BIGINT", "BIGINT", "INTEGER"};
    config["schema"] = schema;

    std::string configStr = config.dump();
    json parsed = json::parse(configStr);

    if (!parsed.contains("sourceType") || !parsed.contains("schema")) {
        std::cerr << "SourceOperatorFuzz: Missing required config fields" << std::endl;
    }
}

static void TestSourceOperatorOutput(const SourceOperatorFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "SourceOperatorFuzz: Simulating source output VectorBatch" << std::endl;

    int rowCnt = (loopCount % 30) + 2;
    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    std::vector<int64_t> col2(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.longField + static_cast<int64_t>(i);
        col1[i] = fzd.longField2 + static_cast<int64_t>(i) * 100;
        col2[i] = static_cast<int64_t>(fzd.intField) + static_cast<int64_t>(i);
    }
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));

    for (int i = 0; i < rowCnt; ++i) {
        vb->setRowKind(i, RowKind::INSERT);
    }

    StreamRecord *record = new StreamRecord(vb);
    delete record;
}

static void TestSourceOperatorMultipleSplits(const SourceOperatorFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "SourceOperatorFuzz: Multiple input splits simulation" << std::endl;

    uint16_t splitCount = (loopCount % 5) + 1;

    for (uint16_t s = 0; s < splitCount; ++s) {
        int rowCnt = (loopCount % 15) + 2;
        omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

        std::vector<int64_t> col0(rowCnt);
        std::vector<int64_t> col1(rowCnt);
        for (int i = 0; i < rowCnt; ++i) {
            col0[i] = fzd.longField + static_cast<int64_t>(s * 1000 + i);
            col1[i] = fzd.longField2 + static_cast<int64_t>(s * 500 + i * 10);
        }
        vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
        vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));

        for (int i = 0; i < rowCnt; ++i) {
            vb->setRowKind(i, RowKind::INSERT);
        }

        StreamRecord *record = new StreamRecord(vb);
        delete record;
    }
}

int SourceOperatorFuzz(struct SourceOperatorFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 3) {
            case 0:
                TestSourceOperatorConfig(fzd, loopCount);
                break;
            case 1:
                TestSourceOperatorOutput(fzd, loopCount);
                break;
            case 2:
                TestSourceOperatorMultipleSplits(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "SourceOperatorFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
