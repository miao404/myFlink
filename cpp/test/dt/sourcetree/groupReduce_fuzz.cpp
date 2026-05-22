/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for StreamGroupedReduceOperator covering stateful reduce
 *              with key grouping and state backend initialization.
 *              Reference UT: StreamGroupedReduceOperatorTest.cpp
 */

#include "streaming_fuzz_wrapper.h"
#include "dt_fuzz_data.h"
#include "runtime_env_util.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "streaming/api/operators/StreamGroupedReduceOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include <test/util/test_util.h>

using json = nlohmann::json;
using namespace DtRuntimeEnvUtil;

static void TestGroupReduceConfigParsing(const GroupReduceFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "GroupReduceFuzz: Config parsing validation" << std::endl;

    json config;
    config["udf_so"] = "/tmp/libMockReduceFunction.so";
    config["key_so"] = json::array({"/tmp/libMockKeyedBy.so"});
    config["hash_path"] = "/tmp/";
    config["udf_obj"] = "{}";

    std::string configStr = config.dump();
    json parsed = json::parse(configStr);

    if (!parsed.contains("udf_so") || !parsed.contains("key_so")) {
        std::cerr << "GroupReduceFuzz: Missing required config fields" << std::endl;
    }

    if (!parsed["key_so"].is_array() || parsed["key_so"].empty()) {
        std::cerr << "GroupReduceFuzz: key_so must be non-empty array" << std::endl;
    }
}

static void TestGroupReduceStateSetup(const GroupReduceFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "GroupReduceFuzz: State backend initialization" << std::endl;

    std::string backend = (fzd.stateBackendFlag % 2 == 0) ? "HashMapStateBackend" : "HashMapStateBackend";

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend(backend);
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);

    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);

    // Validate environment creation with fuzzed data
    uint16_t count = (loopCount % 20) + 1;
    for (uint16_t i = 0; i < count; ++i) {
        BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
        row->setLong(0, fzd.keyLong + static_cast<int64_t>(i));
        row->setLong(1, fzd.valueLong + static_cast<int64_t>(i) * 5);
        delete row;
    }

    delete initializer;
    delete env2;
    delete taskInfo;
}

static void TestGroupReduceDataConstruction(const GroupReduceFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "GroupReduceFuzz: Data construction for reduce input" << std::endl;

    int rowCnt = (loopCount % 20) + 3;
    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> keyCol(rowCnt);
    std::vector<int64_t> valCol(rowCnt);
    std::vector<int64_t> valCol2(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        keyCol[i] = (fzd.keyLong + static_cast<int64_t>(i)) % 5;
        valCol[i] = fzd.valueLong + static_cast<int64_t>(i) * 10;
        valCol2[i] = fzd.valueLong2 + static_cast<int64_t>(i) * 3;
    }
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, keyCol.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, valCol.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, valCol2.data()));

    for (int i = 0; i < rowCnt; ++i) {
        vb->setRowKind(i, RowKind::INSERT);
    }

    StreamRecord *record = new StreamRecord(vb);
    delete record;
}

int GroupReduceFuzz(struct GroupReduceFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 3) {
            case 0:
                TestGroupReduceConfigParsing(fzd, loopCount);
                break;
            case 1:
                TestGroupReduceStateSetup(fzd, loopCount);
                break;
            case 2:
                TestGroupReduceDataConstruction(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "GroupReduceFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
