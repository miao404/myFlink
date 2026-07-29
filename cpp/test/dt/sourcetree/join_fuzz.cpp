#include "fuzz_wrapper.h"
#include "table/data/binary/BinaryRowData.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "test/core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/TaskStateManager.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"
#include "table/types/logical/LogicalType.h"
#include "test/util/test_util.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

#include "table/runtime/operators/join/stream/InnerJoinOperator.h"
#include "table/runtime/operators/join/stream/LeftOuterJoinOperator.h"
#include "table/data/RowKind.h"

omnistream::VectorBatch* createJoinTestVectorBatch(int rowCount, int32_t keyValue, int64_t windowEndTime, int32_t dataValue)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto colKey = new omniruntime::vec::Vector<int32_t>(rowCount);
    auto colWindow = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto colData = new omniruntime::vec::Vector<int32_t>(rowCount);

    for (int i = 0; i < rowCount; i++) {
        colKey->SetValue(i, keyValue + i);
        colWindow->SetValue(i, windowEndTime);
        colData->SetValue(i, dataValue + i);
        vb->setRowKind(i, RowKind::INSERT);
    }

    vb->Append(colKey);
    vb->Append(colWindow);
    vb->Append(colData);

    return vb;
}

static const std::string JOIN_INNER_DESC = R"JSON({"input_channels":[0,1],"operators":[{"description":{"joinType":"InnerJoin","leftInputTypes":["INTEGER","BIGINT","INTEGER"],"rightInputTypes":["INTEGER","BIGINT","INTEGER"],"leftWindowEndCol":1,"rightWindowEndCol":1,"outputTypes":["INTEGER","BIGINT","INTEGER","INTEGER","BIGINT","INTEGER"]},"id":"WindowJoinOperator","name":"WindowJoin[inner]"}],"partition":{"channelNumber":1,"partitionName":"forward"}})JSON";

static const std::string JOIN_LEFT_DESC = R"JSON({"input_channels":[0,1],"operators":[{"description":{"joinType":"LeftOuterJoin","leftInputTypes":["INTEGER","BIGINT","INTEGER"],"rightInputTypes":["INTEGER","BIGINT","INTEGER"],"leftWindowEndCol":1,"rightWindowEndCol":1,"outputTypes":["INTEGER","BIGINT","INTEGER","INTEGER","BIGINT","INTEGER"]},"id":"WindowJoinOperator","name":"WindowJoin[leftOuter]"}],"partition":{"channelNumber":1,"partitionName":"forward"}})JSON";

void TestJoinInner(const JoinFuzzData& fzd)
{
    std::cout << "TestJoinInner" << std::endl;

    json parsedJson = json::parse(JOIN_INNER_DESC);
    json opDesc = parsedJson["operators"][0]["description"];

    BatchOutputTest* output = new BatchOutputTest();
    InnerJoinOperator *joinOp = new InnerJoinOperator(opDesc, output);

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    joinOp->initializeState(initializer);
    joinOp->open();

    omnistream::VectorBatch* leftVb = createJoinTestVectorBatch(fzd.loopCount, fzd.leftKeyValue, fzd.windowEndTime, fzd.leftValue);
    omnistream::VectorBatch* rightVb = createJoinTestVectorBatch(fzd.loopCount, fzd.rightKeyValue, fzd.windowEndTime, fzd.rightValue);

    joinOp->processBatch(new StreamRecord(leftVb), 0);
    joinOp->processBatch(new StreamRecord(rightVb), 1);

    delete leftVb;
    delete rightVb;
}

void TestJoinLeftOuter(const JoinFuzzData& fzd)
{
    std::cout << "TestJoinLeftOuter" << std::endl;

    json parsedJson = json::parse(JOIN_LEFT_DESC);
    json opDesc = parsedJson["operators"][0]["description"];

    BatchOutputTest* output = new BatchOutputTest();
    LeftOuterJoinOperator *joinOp = new LeftOuterJoinOperator(opDesc, output);

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    joinOp->initializeState(initializer);
    joinOp->open();

    omnistream::VectorBatch* leftVb = createJoinTestVectorBatch(fzd.loopCount, fzd.leftKeyValue, fzd.windowEndTime, fzd.leftValue);
    omnistream::VectorBatch* rightVb = createJoinTestVectorBatch(fzd.loopCount, fzd.rightKeyValue, fzd.windowEndTime, fzd.rightValue);

    joinOp->processBatch(new StreamRecord(leftVb), 0);
    joinOp->processBatch(new StreamRecord(rightVb), 1);

    delete leftVb;
    delete rightVb;
}

void TestJoinMultiBatch(const JoinFuzzData& fzd)
{
    std::cout << "TestJoinMultiBatch" << std::endl;

    json parsedJson = json::parse(JOIN_INNER_DESC);
    json opDesc = parsedJson["operators"][0]["description"];

    BatchOutputTest* output = new BatchOutputTest();
    InnerJoinOperator *joinOp = new InnerJoinOperator(opDesc, output);

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    joinOp->initializeState(initializer);
    joinOp->open();

    for (int batch = 0; batch < 3; batch++) {
        omnistream::VectorBatch* leftVb = createJoinTestVectorBatch(fzd.loopCount, fzd.leftKeyValue + batch, fzd.windowEndTime + batch * 1000, fzd.leftValue);
        omnistream::VectorBatch* rightVb = createJoinTestVectorBatch(fzd.loopCount, fzd.rightKeyValue + batch, fzd.windowEndTime + batch * 1000, fzd.rightValue);

        joinOp->processBatch(new StreamRecord(leftVb), 0);
        joinOp->processBatch(new StreamRecord(rightVb), 1);
    }
}

int GlobalJoinFuzz(struct JoinFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "JoinFuzz: chooseFunc=" << chooseFunc
              << ", joinType=" << fzd.joinType
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestJoinInner(fzd); break;
        case 2: TestJoinLeftOuter(fzd); break;
        case 3: TestJoinMultiBatch(fzd); break;
        default:
            TestJoinInner(fzd);
            TestJoinLeftOuter(fzd);
            TestJoinMultiBatch(fzd);
            break;
    }
    return 0;
}
