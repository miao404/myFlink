/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for StreamFilter/StreamMap/StreamFlatMap operators
 *              covering UDF-based transformations with various data types.
 *
 * Note: StreamFilter/StreamMap/StreamFlatMap require UDF .so loading which is
 * not available in the DT fuzz environment. This file provides structural fuzz
 * test stubs that validate configuration parsing, data construction, and basic
 * setup paths without UDF loading.
 */

#include "streaming_fuzz_wrapper.h"
#include "dt/common/dt_fuzz_data.h"
#include "dt/common/dt_fuzz_factory_util.h"
#include "dt/common/runtime_env_util.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/RowDataSerializer.h"
#include "core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"

using json = nlohmann::json;
using namespace DtFuzzFactoryUtil;
using namespace DtRuntimeEnvUtil;

static void TestFilterConfigAndData(const StreamingFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "TransformFuzz: StreamFilter config and data construction" << std::endl;

    json config;
    config["operatorType"] = "StreamFilter";
    config["inputTypes"] = {"BIGINT", "BIGINT"};
    config["outputTypes"] = {"BIGINT", "BIGINT"};
    config["filterCondition"] = "col0 > 0";

    std::string configStr = config.dump();
    json parsed = json::parse(configStr);

    uint16_t count = (loopCount % 50) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setLong(0, (fzd.longValue + static_cast<int64_t>(i)) % 100);
        row->setLong(1, fzd.longValue2 + static_cast<int64_t>(i) * 13);

        StreamRecord *record = new StreamRecord(reinterpret_cast<void *>(row));
        delete record;
    }
}

static void TestMapConfigAndData(const StreamingFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "TransformFuzz: StreamMap config and data construction" << std::endl;

    json config;
    config["operatorType"] = "StreamMap";
    config["inputTypes"] = {"BIGINT", "BIGINT"};
    config["outputTypes"] = {"BIGINT"};
    config["mapExpression"] = "col0 + col1";

    std::string configStr = config.dump();
    json parsed = json::parse(configStr);

    uint16_t count = (loopCount % 40) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setLong(0, fzd.longValue + static_cast<int64_t>(i));
        row->setLong(1, fzd.longValue2 - static_cast<int64_t>(i) * 2);

        StreamRecord *record = new StreamRecord(reinterpret_cast<void *>(row));
        delete record;
    }
}

static void TestFlatMapConfigAndData(const StreamingFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "TransformFuzz: StreamFlatMap config and data construction" << std::endl;

    json config;
    config["operatorType"] = "StreamFlatMap";
    config["inputTypes"] = {"BIGINT", "BIGINT"};
    config["outputTypes"] = {"BIGINT"};
    config["flatMapExpression"] = "explode(col0)";

    std::string configStr = config.dump();
    json parsed = json::parse(configStr);

    uint16_t count = (loopCount % 30) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setLong(0, (fzd.longValue + static_cast<int64_t>(i)) % 50);
        row->setLong(1, fzd.longValue2 + static_cast<int64_t>(i) * 7);

        StreamRecord *record = new StreamRecord(reinterpret_cast<void *>(row));
        delete record;
    }
}

int TransformFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseTransform)
{
    try {
        switch (chooseTransform % 3) {
            case 0:
                TestFilterConfigAndData(fzd, loopCount);
                break;
            case 1:
                TestMapConfigAndData(fzd, loopCount);
                break;
            case 2:
                TestFlatMapConfigAndData(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "TransformFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
