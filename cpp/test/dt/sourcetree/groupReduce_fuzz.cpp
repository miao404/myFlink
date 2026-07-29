#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/StreamGroupedReduceOperator.h"
#include "test/core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/TaskStateManager.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createGroupReduceTestVectorBatch(int rowCount, int64_t longValue)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, longValue + i);
        vb->setRowKind(i, RowKind::INSERT);
    }

    vb->Append(col0);

    return vb;
}

static const std::string REDUCE_DESC = R"JSON({"name":"StreamGroupedReduce","description":{"inputTypes":["BIGINT"],"outputTypes":["BIGINT"],"udfClassName":"TestReduceFunction","keySelector":[0]},"id":"StreamGroupedReduceOperator"})JSON";

void TestGroupReduceBasic(const GroupReduceFuzzData& fzd)
{
    std::cout << "TestGroupReduceBasic" << std::endl;

    json parsedJson = json::parse(REDUCE_DESC);
    std::cout << "GroupReduce config parsed: " << parsedJson["name"] << std::endl;

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);

    omnistream::VectorBatch* vb = createGroupReduceTestVectorBatch(fzd.loopCount, fzd.longValue);
    std::cout << "GroupReduce VectorBatch created with " << fzd.loopCount << " rows" << std::endl;

    delete vb;
}

void TestGroupReduceWithState(const GroupReduceFuzzData& fzd)
{
    std::cout << "TestGroupReduceWithState" << std::endl;

    json parsedJson = json::parse(REDUCE_DESC);

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);

    for (int batch = 0; batch < 3; batch++) {
        omnistream::VectorBatch* vb = createGroupReduceTestVectorBatch(fzd.loopCount, fzd.longValue + batch * 100);
        std::cout << "GroupReduce state batch " << batch << " created" << std::endl;
        delete vb;
    }
}

void TestGroupReduceMultiBatch(const GroupReduceFuzzData& fzd)
{
    std::cout << "TestGroupReduceMultiBatch" << std::endl;

    json parsedJson = json::parse(REDUCE_DESC);

    for (int batch = 0; batch < 5; batch++) {
        omnistream::VectorBatch* vb = createGroupReduceTestVectorBatch(fzd.loopCount, fzd.longValue + batch * 50);
        std::cout << "GroupReduce batch " << batch << " created" << std::endl;
        delete vb;
    }
}

int GlobalGroupReduceFuzz(struct GroupReduceFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "GroupReduceFuzz: chooseFunc=" << chooseFunc
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestGroupReduceBasic(fzd); break;
        case 2: TestGroupReduceWithState(fzd); break;
        case 3: TestGroupReduceMultiBatch(fzd); break;
        default:
            TestGroupReduceBasic(fzd);
            TestGroupReduceWithState(fzd);
            TestGroupReduceMultiBatch(fzd);
            break;
    }
    return 0;
}
