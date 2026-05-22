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

#include "table/runtime/operators/rank/AppendOnlyTopNFunction.h"

omnistream::VectorBatch* createRankTestVectorBatch(int rowCount, int64_t keyValue, int64_t sortValue, int64_t dataValue)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col1 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col2 = new omniruntime::vec::Vector<int64_t>(rowCount);

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, keyValue);
        col1->SetValue(i, sortValue + i);
        col2->SetValue(i, dataValue + i);
        vb->setRowKind(i, RowKind::INSERT);
    }

    vb->Append(col0);
    vb->Append(col1);
    vb->Append(col2);

    return vb;
}

static const std::string RANK_DESC_STR = R"JSON({"input_channels":[0],"operators":[{"description":{"topN":3,"generateUpdateBefore":true,"outputRankNumber":false,"inputTypes":["BIGINT","BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT","BIGINT"],"partitionByFields":[0],"sortFields":[1],"sortOrders":[true],"originDescription":"[3]:Rank(orderBy=[col1 ASC])"},"id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator","name":"AppendOnlyTopN[3]"}],"partition":{"channelNumber":1,"partitionName":"forward"}})JSON";

void TestRankBasic(const RankFuzzData& fzd)
{
    std::cout << "TestRankBasic" << std::endl;

    json parsedJson = json::parse(RANK_DESC_STR);

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    OperatorConfig opConfig(uniqueName, "AppendOnlyTopN",
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    AppendOnlyTopNFunction *func = new AppendOnlyTopNFunction(opConfig.getDescription());
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

    omnistream::VectorBatch* vb = createRankTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.sortValue, fzd.dataValue);
    StreamRecord *record = new StreamRecord(vb);
    keyedOp->processBatch(record);

    delete record;
}

void TestRankWithUpdate(const RankFuzzData& fzd)
{
    std::cout << "TestRankWithUpdate" << std::endl;

    json parsedJson = json::parse(RANK_DESC_STR);

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    OperatorConfig opConfig(uniqueName, "AppendOnlyTopN",
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    AppendOnlyTopNFunction *func = new AppendOnlyTopNFunction(opConfig.getDescription());
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

    omnistream::VectorBatch* vb1 = createRankTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.sortValue, fzd.dataValue);
    keyedOp->processBatch(new StreamRecord(vb1));

    omnistream::VectorBatch* vb2 = createRankTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.sortValue + 100, fzd.dataValue + 100);
    keyedOp->processBatch(new StreamRecord(vb2));
}

void TestRankMultiPartition(const RankFuzzData& fzd)
{
    std::cout << "TestRankMultiPartition" << std::endl;

    json parsedJson = json::parse(RANK_DESC_STR);

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    OperatorConfig opConfig(uniqueName, "AppendOnlyTopN",
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    AppendOnlyTopNFunction *func = new AppendOnlyTopNFunction(opConfig.getDescription());
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

    for (int p = 0; p < 3; p++) {
        omnistream::VectorBatch* vb = createRankTestVectorBatch(fzd.loopCount, fzd.keyValue + p, fzd.sortValue, fzd.dataValue);
        keyedOp->processBatch(new StreamRecord(vb));
    }
}

int GlobalRankFuzz(struct RankFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "RankFuzz: chooseFunc=" << chooseFunc
              << ", topN=" << fzd.topN
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestRankBasic(fzd); break;
        case 2: TestRankWithUpdate(fzd); break;
        case 3: TestRankMultiPartition(fzd); break;
        default:
            TestRankBasic(fzd);
            TestRankWithUpdate(fzd);
            TestRankMultiPartition(fzd);
            break;
    }
    return 0;
}
