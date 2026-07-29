#include "fuzz_wrapper.h"
#include "table/runtime/operators/aggregate/GroupAggFunction.h"
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

omnistream::VectorBatch* createKeyedProcessTestVectorBatch(int rowCount, int64_t keyValue, int64_t value1, int64_t value2)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col1 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col2 = new omniruntime::vec::Vector<int64_t>(rowCount);

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, keyValue + i);
        col1->SetValue(i, value1 + i);
        col2->SetValue(i, value2 + i);
        vb->setRowKind(i, RowKind::INSERT);
    }

    vb->Append(col0);
    vb->Append(col1);
    vb->Append(col2);

    return vb;
}

static const std::string KP_DESC_SUM = R"JSON({"input_channels":[0],"operators":[{"description":{"aggInfoList":{"accTypes":["BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"LongSumAggFunction","argIndexes":[1],"consumeRetraction":"false","name":"SUM($1)","filterArg":-1}],"indexOfCountStar":-1},"grouping":[0],"distinctInfos":[],"inputTypes":["BIGINT","BIGINT","BIGINT"],"originDescription":"[3]:GroupAggregate(groupBy=[col0], select=[col0, SUM(col1) AS EXPR$1])","outputTypes":["BIGINT","BIGINT"]},"id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator","inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],"name":"GroupAggregate[3]","output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],"partition":{"channelNumber":1,"partitionName":"forward"}})JSON";

void TestKeyedProcessGroupAgg(const KeyedProcessFuzzData& fzd)
{
    std::cout << "TestKeyedProcessGroupAgg" << std::endl;

    json parsedJson = json::parse(KP_DESC_SUM);

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    omnistream::OperatorConfig opConfig(
            uniqueName, "Group_By_Simple",
            parsedJson["operators"][0]["inputTypes"],
            parsedJson["operators"][0]["outputTypes"],
            parsedJson["operators"][0]["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    GroupAggFunction *func = new GroupAggFunction(0l, opConfig.getDescription());
    KeyedProcessOperator<RowData *, RowData*, RowData*> *keyedOp = new KeyedProcessOperator(func, output, opConfig.getDescription());
    keyedOp->setup();

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> *typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT), omnistream::RowField("col1", BasicLogicalType::BIGINT), omnistream::RowField("col2", BasicLogicalType::BIGINT)});
    TypeSerializer *ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();

    omnistream::VectorBatch* vb = createKeyedProcessTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.value1, fzd.value2);
    StreamRecord *record = new StreamRecord(vb);
    keyedOp->processBatch(record);

    delete record;
}

void TestKeyedProcessMultiKey(const KeyedProcessFuzzData& fzd)
{
    std::cout << "TestKeyedProcessMultiKey" << std::endl;

    json parsedJson = json::parse(KP_DESC_SUM);

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    omnistream::OperatorConfig opConfig(
            uniqueName, "Group_By_Simple",
            parsedJson["operators"][0]["inputTypes"],
            parsedJson["operators"][0]["outputTypes"],
            parsedJson["operators"][0]["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    GroupAggFunction *func = new GroupAggFunction(0l, opConfig.getDescription());
    KeyedProcessOperator<RowData *, RowData*, RowData*> *keyedOp = new KeyedProcessOperator(func, output, opConfig.getDescription());
    keyedOp->setup();

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> *typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT), omnistream::RowField("col1", BasicLogicalType::BIGINT), omnistream::RowField("col2", BasicLogicalType::BIGINT)});
    TypeSerializer *ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();

    for (int k = 0; k < 3; k++) {
        omnistream::VectorBatch* vb = createKeyedProcessTestVectorBatch(fzd.loopCount, fzd.keyValue + k * 100, fzd.value1, fzd.value2);
        keyedOp->processBatch(new StreamRecord(vb));
    }
}

void TestKeyedProcessMultiBatch(const KeyedProcessFuzzData& fzd)
{
    std::cout << "TestKeyedProcessMultiBatch" << std::endl;

    json parsedJson = json::parse(KP_DESC_SUM);

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    omnistream::OperatorConfig opConfig(
            uniqueName, "Group_By_Simple",
            parsedJson["operators"][0]["inputTypes"],
            parsedJson["operators"][0]["outputTypes"],
            parsedJson["operators"][0]["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    GroupAggFunction *func = new GroupAggFunction(0l, opConfig.getDescription());
    KeyedProcessOperator<RowData *, RowData*, RowData*> *keyedOp = new KeyedProcessOperator(func, output, opConfig.getDescription());
    keyedOp->setup();

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> *typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT), omnistream::RowField("col1", BasicLogicalType::BIGINT), omnistream::RowField("col2", BasicLogicalType::BIGINT)});
    TypeSerializer *ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();

    for (int batch = 0; batch < 5; batch++) {
        omnistream::VectorBatch* vb = createKeyedProcessTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.value1 + batch * 50, fzd.value2);
        keyedOp->processBatch(new StreamRecord(vb));
    }
}

int GlobalKeyedProcessFuzz(struct KeyedProcessFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "KeyedProcessFuzz: chooseFunc=" << chooseFunc
              << ", functionType=" << fzd.functionType
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestKeyedProcessGroupAgg(fzd); break;
        case 2: TestKeyedProcessMultiKey(fzd); break;
        case 3: TestKeyedProcessMultiBatch(fzd); break;
        default:
            TestKeyedProcessGroupAgg(fzd);
            TestKeyedProcessMultiKey(fzd);
            TestKeyedProcessMultiBatch(fzd);
            break;
    }
    return 0;
}
