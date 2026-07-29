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

#include "table/runtime/operators/aggregate/GroupAggFunction.h"

omnistream::VectorBatch* createAggTestVectorBatch(int rowCount, int64_t keyValue, int64_t aggValue1, int64_t aggValue2)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col1 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col2 = new omniruntime::vec::Vector<int64_t>(rowCount);

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, keyValue + i);
        col1->SetValue(i, aggValue1 + i);
        col2->SetValue(i, aggValue2 + i);
        vb->setRowKind(i, RowKind::INSERT);
    }

    vb->Append(col0);
    vb->Append(col1);
    vb->Append(col2);

    return vb;
}

static const std::string AGG_DESC_SUM = R"JSON({"input_channels":[0],"operators":[{"description":{"aggInfoList":{"accTypes":["BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"LongSumAggFunction","argIndexes":[1],"consumeRetraction":"false","name":"SUM($1)","filterArg":-1}],"indexOfCountStar":-1},"grouping":[0],"distinctInfos":[],"inputTypes":["BIGINT","BIGINT","BIGINT"],"originDescription":"[3]:GroupAggregate(groupBy=[col0], select=[col0, SUM(col1) AS EXPR$1])","outputTypes":["BIGINT","BIGINT"]},"id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator","inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],"name":"GroupAggregate[3]","output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],"partition":{"channelNumber":1,"partitionName":"forward"}})JSON";
static const std::string AGG_DESC_RETRACT = R"JSON({"input_channels":[0],"operators":[{"description":{"aggInfoList":{"accTypes":["BIGINT","BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"LongSumAggFunction","argIndexes":[1],"consumeRetraction":"true","name":"SUM($1)","filterArg":-1}],"indexOfCountStar":1},"grouping":[0],"distinctInfos":[],"inputTypes":["BIGINT","BIGINT","BIGINT"],"originDescription":"[3]:GroupAggregate(groupBy=[col0], select=[col0, SUM(col1) AS EXPR$1])","outputTypes":["BIGINT","BIGINT"]},"id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator","inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],"name":"GroupAggregate[3]","output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],"partition":{"channelNumber":1,"partitionName":"forward"}})JSON";
static const std::string AGG_DESC_AVG = R"JSON({"input_channels":[0],"operators":[{"description":{"aggInfoList":{"accTypes":["BIGINT","BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"LongAvgAggFunction","argIndexes":[1],"consumeRetraction":"false","filterArg":-1,"name":"AVG($1)"}],"indexOfCountStar":-1},"grouping":[0],"inputTypes":["BIGINT","BIGINT","BIGINT"],"originDescription":"[3]:GroupAggregate(groupBy=[col0], select=[col0, AVG(col1) AS EXPR$1])","outputTypes":["BIGINT","BIGINT"],"distinctInfos":[]},"id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator","inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],"name":"GroupAggregate[3]","output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],"partition":{"channelNumber":1,"partitionName":"forward"}})JSON";

void TestGroupAggBasic(const GroupAggFuzzData& fzd)
{
    std::cout << "TestGroupAggBasic" << std::endl;

    std::string description = AGG_DESC_SUM;

    json parsedJson = json::parse(description);

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    omnistream::OperatorConfig opConfig(
            uniqueName,
            "Group_By_Simple",
            parsedJson["operators"][0]["inputTypes"],
            parsedJson["operators"][0]["outputTypes"],
            parsedJson["operators"][0]["description"]
    );

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

    omnistream::VectorBatch* vb = createAggTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.aggValue1, fzd.aggValue2);
    StreamRecord *record = new StreamRecord(vb);
    keyedOp->processBatch(record);

    delete record;
}

void TestGroupAggWithRetract(const GroupAggFuzzData& fzd)
{
    std::cout << "TestGroupAggWithRetract" << std::endl;

    std::string description = AGG_DESC_RETRACT;

    json parsedJson = json::parse(description);

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    omnistream::OperatorConfig opConfig(
            uniqueName,
            "Group_By_Simple",
            parsedJson["operators"][0]["inputTypes"],
            parsedJson["operators"][0]["outputTypes"],
            parsedJson["operators"][0]["description"]
    );

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

    omnistream::VectorBatch* vb = createAggTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.aggValue1, fzd.aggValue2);
    StreamRecord *record = new StreamRecord(vb);
    keyedOp->processBatch(record);

    delete record;
}

void TestGroupAggBatch(const GroupAggFuzzData& fzd)
{
    std::cout << "TestGroupAggBatch" << std::endl;

    std::string description = AGG_DESC_AVG;

    json parsedJson = json::parse(description);

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    omnistream::OperatorConfig opConfig(
            uniqueName,
            "Group_By_Simple",
            parsedJson["operators"][0]["inputTypes"],
            parsedJson["operators"][0]["outputTypes"],
            parsedJson["operators"][0]["description"]
    );

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

    omnistream::VectorBatch* vb = createAggTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.aggValue1, fzd.aggValue2);
    StreamRecord *record = new StreamRecord(vb);
    keyedOp->processBatch(record);

    delete record;
}


int GlobalGroupAggFuzz(struct GroupAggFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "GroupAggFuzz: chooseFunc=" << chooseFunc
              << ", aggFunctionType=" << fzd.aggFunctionType
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1:
            TestGroupAggBasic(fzd);
            break;
        case 2:
            TestGroupAggWithRetract(fzd);
            break;
        case 3:
            TestGroupAggBatch(fzd);
            break;
        default:
            TestGroupAggBasic(fzd);
            TestGroupAggWithRetract(fzd);
            TestGroupAggBatch(fzd);
            break;
    }

    return 0;
}
