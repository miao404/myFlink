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

#include "table/runtime/operators/deduplicate/RowTimeDeduplicateFunction.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "table/data/RowKind.h"

omnistream::VectorBatch* createDedupTestVectorBatch(int rowCount, int64_t keyValue, int64_t keyValue2,
                                                     int64_t rowtimeValue, int64_t timestampValue)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col1 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col2 = new omniruntime::vec::Vector<int64_t>(rowCount);

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, keyValue);
        col1->SetValue(i, keyValue2 + i);
        col2->SetValue(i, rowtimeValue + i);
        vb->setTimestamp(i, timestampValue + i);
        vb->setRowKind(i, RowKind::INSERT);
    }

    auto stringVec = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(rowCount);
    for (int i = 0; i < rowCount; i++) {
        std::string str = "str_" + std::to_string(i);
        std::string_view sv(str.data(), str.size());
        stringVec->SetValue(i, sv);
    }

    vb->Append(col0);
    vb->Append(col1);
    vb->Append(col2);
    vb->Append(stringVec);

    return vb;
}

static const std::string DEDUP_DESC_LAST = R"JSON({"input_channels":[0],"operators":[{"description":{"miniBatchSize":-1,"inputTypes":["BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)"],"keepLastRow":true,"isCompactChanges":false,"minRetentionTime":0,"rowtimeIndex":2,"isRowtime":true,"generateUpdateBefore":true,"outputTypes":["BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)"],"generateInsert":true,"originDescription":null,"grouping":[0,1]}}],"partition":{"channelNumber":1,"partitionName":"forward"}})JSON";
static const std::string DEDUP_DESC_FIRST = R"JSON({"input_channels":[0],"operators":[{"description":{"miniBatchSize":-1,"inputTypes":["BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)"],"keepLastRow":false,"isCompactChanges":false,"minRetentionTime":0,"rowtimeIndex":2,"isRowtime":true,"generateUpdateBefore":true,"outputTypes":["BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)"],"generateInsert":true,"originDescription":null,"grouping":[0,1]}}],"partition":{"channelNumber":1,"partitionName":"forward"}})JSON";

void TestDeduplicateKeepLast(const DeduplicateFuzzData& fzd)
{
    std::cout << "TestDeduplicateKeepLast" << std::endl;

    json parsedJson = json::parse(DEDUP_DESC_LAST);

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    OperatorConfig opConfig(uniqueName,
        "Deduplicate(keep=[LastRow], key=[$1, $2], order=[ROWTIME])",
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    BatchOutputTest *output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;
    auto *keyedOp = dynamic_cast<KeyedProcessOperator<RowData *, omnistream::VectorBatch *, omnistream::VectorBatch *> *>(
        streamOperatorFactory.createOperatorAndCollector(opConfig, output));

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> *typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT), omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer *ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();

    omnistream::VectorBatch* vb = createDedupTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.keyValue2, fzd.rowtimeValue, fzd.timestampValue);
    keyedOp->processBatch(new StreamRecord(vb));
}

void TestDeduplicateKeepFirst(const DeduplicateFuzzData& fzd)
{
    std::cout << "TestDeduplicateKeepFirst" << std::endl;

    json parsedJson = json::parse(DEDUP_DESC_FIRST);

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    OperatorConfig opConfig(uniqueName,
        "Deduplicate(keep=[FirstRow], key=[$1, $2], order=[ROWTIME])",
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    BatchOutputTest *output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;
    auto *keyedOp = dynamic_cast<KeyedProcessOperator<RowData *, omnistream::VectorBatch *, omnistream::VectorBatch *> *>(
        streamOperatorFactory.createOperatorAndCollector(opConfig, output));

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> *typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT), omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer *ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();

    omnistream::VectorBatch* vb = createDedupTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.keyValue2, fzd.rowtimeValue, fzd.timestampValue);
    keyedOp->processBatch(new StreamRecord(vb));
}

void TestDeduplicateMultiBatch(const DeduplicateFuzzData& fzd)
{
    std::cout << "TestDeduplicateMultiBatch" << std::endl;

    json parsedJson = json::parse(DEDUP_DESC_LAST);

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    OperatorConfig opConfig(uniqueName,
        "Deduplicate(keep=[LastRow], key=[$1, $2], order=[ROWTIME])",
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    BatchOutputTest *output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;
    auto *keyedOp = dynamic_cast<KeyedProcessOperator<RowData *, omnistream::VectorBatch *, omnistream::VectorBatch *> *>(
        streamOperatorFactory.createOperatorAndCollector(opConfig, output));

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> *typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT), omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer *ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();

    omnistream::VectorBatch* vb1 = createDedupTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.keyValue2, fzd.rowtimeValue, fzd.timestampValue);
    keyedOp->processBatch(new StreamRecord(vb1));

    omnistream::VectorBatch* vb2 = createDedupTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.keyValue2, fzd.rowtimeValue + 10, fzd.timestampValue + 100);
    keyedOp->processBatch(new StreamRecord(vb2));
}

int GlobalDeduplicateFuzz(struct DeduplicateFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "DeduplicateFuzz: chooseFunc=" << chooseFunc
              << ", keepLastRow=" << fzd.keepLastRow
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestDeduplicateKeepLast(fzd); break;
        case 2: TestDeduplicateKeepFirst(fzd); break;
        case 3: TestDeduplicateMultiBatch(fzd); break;
        default:
            TestDeduplicateKeepLast(fzd);
            TestDeduplicateKeepFirst(fzd);
            TestDeduplicateMultiBatch(fzd);
            break;
    }
    return 0;
}
