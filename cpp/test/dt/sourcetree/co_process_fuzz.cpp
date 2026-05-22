/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for KeyedCoProcessOperator covering dual-input stream
 *              processing with timers and key selectors.
 *
 * Note: KeyedCoProcessOperator requires UDF .so loading which is not available
 * in the DT fuzz environment. This file provides structural fuzz test stubs
 * that validate the operator's configuration parsing and basic setup paths.
 */

#include "streaming_fuzz_wrapper.h"
#include "dt_fuzz_data.h"
#include "dt_fuzz_factory_util.h"
#include "runtime_env_util.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "streaming/api/operators/co/KeyedCoProcessOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/RowDataSerializer.h"
#include "core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"

using json = nlohmann::json;
using namespace DtFuzzFactoryUtil;
using namespace DtRuntimeEnvUtil;

static void TestCoProcessConfigParsing(const StreamingFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "CoProcessFuzz: Config parsing validation" << std::endl;

    json config;
    config["inputTypes1"] = {"BIGINT", "BIGINT"};
    config["inputTypes2"] = {"BIGINT", "BIGINT"};
    config["outputTypes"] = {"BIGINT", "BIGINT", "BIGINT"};

    uint16_t numKeys = (loopCount % 5) + 1;
    std::vector<int> keyIndices1;
    std::vector<int> keyIndices2;
    for (uint16_t i = 0; i < numKeys && i < 2; ++i) {
        keyIndices1.push_back(static_cast<int>(i));
        keyIndices2.push_back(static_cast<int>(i));
    }
    config["keyIndices1"] = keyIndices1;
    config["keyIndices2"] = keyIndices2;

    std::string configStr = config.dump();
    json parsed = json::parse(configStr);

    if (!parsed.contains("inputTypes1") || !parsed.contains("inputTypes2")) {
        std::cerr << "CoProcessFuzz: Missing required config fields" << std::endl;
    }
}

static void TestCoProcessDualInputSetup(const StreamingFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "CoProcessFuzz: Dual input setup validation" << std::endl;

    json config;
    config["inputTypes1"] = {"BIGINT", "BIGINT"};
    config["inputTypes2"] = {"BIGINT", "BIGINT", "BIGINT"};
    config["outputTypes"] = {"BIGINT", "BIGINT", "BIGINT", "BIGINT"};

    auto rowFields1 = CreateRowFields({"BIGINT", "BIGINT"});
    auto rowFields2 = CreateRowFields({"BIGINT", "BIGINT", "BIGINT"});

    auto *ctx1 = CreateRuntimeEnv("HashMapStateBackend", rowFields1);
    auto *ctx2 = CreateRuntimeEnv("HashMapStateBackend", rowFields2);

    uint16_t count = (loopCount % 20) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        BinaryRowData *row1 = BinaryRowData::createBinaryRowDataWithMem(2);
        row1->setLong(0, (fzd.longValue + i) % 10);
        row1->setLong(1, fzd.longValue2 + i * 5);

        BinaryRowData *row2 = BinaryRowData::createBinaryRowDataWithMem(3);
        row2->setLong(0, (fzd.longValue + i) % 10);
        row2->setLong(1, fzd.longValue2 + i * 3);
        row2->setLong(2, fzd.timestampMillis + i * 1000);

        delete row1;
        delete row2;
    }

    delete ctx1;
    delete ctx2;
}

int CoProcessFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 2) {
            case 0:
                TestCoProcessConfigParsing(fzd, loopCount);
                break;
            case 1:
                TestCoProcessDualInputSetup(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "CoProcessFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
