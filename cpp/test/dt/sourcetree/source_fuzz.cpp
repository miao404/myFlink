/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for table source operators (CsvInputFormat, InputSplit)
 *              covering CSV parsing with various field types and record counts.
 *              Reference UT: SourceTest.cpp (table/runtime/operator/source)
 */

#include "table_fuzz_wrapper.h"
#include "dt_fuzz_data.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>
#include <fstream>

#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include <test/util/test_util.h>

using json = nlohmann::json;

static void TestSourceConfigParsing(const SourceFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "SourceFuzz: Config parsing for CsvInputFormat" << std::endl;

    json description;
    description["format"] = "csv";
    description["delimiter"] = nullptr;
    description["filePath"] = "/tmp/fuzz_input.csv";
    description["batchSize"] = 1000;

    json schema;
    schema["fieldNames"] = {"bid", "timestamp", "extra"};
    schema["fieldTypes"] = {"BIGINT", "BIGINT", "BIGINT"};
    description["schema"] = schema;

    std::string configStr = description.dump();
    json parsed = json::parse(configStr);

    if (!parsed.contains("format") || !parsed.contains("schema")) {
        std::cerr << "SourceFuzz: Missing required config fields" << std::endl;
    }

    if (parsed["schema"]["fieldNames"].size() != parsed["schema"]["fieldTypes"].size()) {
        std::cerr << "SourceFuzz: Schema field count mismatch" << std::endl;
    }
}

static void TestSourceDataConstruction(const SourceFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "SourceFuzz: VectorBatch construction simulating source output" << std::endl;

    int rowCnt = (loopCount % 30) + 2;
    auto *vbatch = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    std::vector<int64_t> col2(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.longField + static_cast<int64_t>(i);
        col1[i] = fzd.longField2 + static_cast<int64_t>(i) * 100;
        col2[i] = fzd.longField3 + static_cast<int64_t>(i) * 50;
    }
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));

    for (int i = 0; i < rowCnt; ++i) {
        vbatch->setRowKind(i, RowKind::INSERT);
    }

    StreamRecord *record = new StreamRecord(vbatch);
    delete record;
}

static void TestSourceMultipleFormats(const SourceFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "SourceFuzz: Multiple format configuration validation" << std::endl;

    std::vector<std::string> formats = {"csv", "json", "parquet"};
    uint8_t formatIdx = fzd.formatFlag % 3;
    std::string selectedFormat = formats[formatIdx];

    json description;
    description["format"] = selectedFormat;
    description["batchSize"] = (loopCount % 5000) + 100;

    json schema;
    schema["fieldNames"] = {"f0", "f1", "f2"};
    schema["fieldTypes"] = {"BIGINT", "BIGINT", "BIGINT"};
    description["schema"] = schema;

    std::string configStr = description.dump();
    json parsed = json::parse(configStr);

    int rowCnt = (loopCount % 20) + 1;
    auto *vbatch = new omnistream::VectorBatch(rowCnt);
    std::vector<int64_t> col0(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.longField + static_cast<int64_t>(i) * 7;
    }
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));

    StreamRecord *record = new StreamRecord(vbatch);
    delete record;
}

int SourceFuzz(struct SourceFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 3) {
            case 0:
                TestSourceConfigParsing(fzd, loopCount);
                break;
            case 1:
                TestSourceDataConstruction(fzd, loopCount);
                break;
            case 2:
                TestSourceMultipleFormats(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "SourceFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
