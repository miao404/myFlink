#include "fuzz_wrapper.h"
#include "table/runtime/operators/rank/AppendOnlyTopNFunction.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "test/core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"
#include "table/types/logical/LogicalType.h"
#include "test/util/test_util.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

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

// Rank config: partition by field0, sort descending by field1, top3 with outputRankNumber
static const std::string RANK_DESC_TOP3 = R"DELIM({"originDescription":null,
"inputTypes":["BIGINT", "BIGINT", "BIGINT"],
"outputTypes":["BIGINT","BIGINT","BIGINT","BIGINT"],
"partitionKey":[0],
"outputRankNumber":true,
"rankRange":"rankStart=1, rankEnd=3",
"generateUpdateBefore":false,
"processFunction":"AppendOnlyTopNFunction",
"sortFieldIndices":[1],
"sortAscendingOrders":[false],
"sortNullsIsLast":[true]})DELIM";

// Rank config: without row number, generateUpdateBefore=true
static const std::string RANK_DESC_NO_ROWNUM = R"DELIM({"originDescription":null,
"sortAscendingOrders": [false],
"inputTypes": ["BIGINT","BIGINT","BIGINT"],
"rankRange": "rankStart=1, rankEnd=3",
"processFunction": "AppendOnlyTopNFunction",
"sortFieldIndices": [2],
"partitionKey": [1],
"sortNullsIsLast": [true],
"generateUpdateBefore": true,
"outputTypes": ["BIGINT","BIGINT","BIGINT"],
"outputRankNumber": false})DELIM";

void TestRankBasic(const RankFuzzData& fzd)
{
    std::cout << "TestRankBasic" << std::endl;

    const json rankConfig = json::parse(RANK_DESC_TOP3);

    auto func = reinterpret_cast<KeyedProcessFunction<RowData*, RowData *, RowData *> *>(
        new AppendOnlyTopNFunction<RowData*>(rankConfig));

    json newRankConfig = rankConfig;
    BatchOutputTest *output = new BatchOutputTest();
    auto *op = new KeyedProcessOperator(func, output, newRankConfig);
    op->setup();

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> typeInfo {
        omnistream::RowField("col0", BasicLogicalType::BIGINT),
        omnistream::RowField("col1", BasicLogicalType::BIGINT),
        omnistream::RowField("col2", BasicLogicalType::BIGINT)};
    TypeSerializer *ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));
    op->initializeState(initializer, ser);
    op->open();

    omnistream::VectorBatch* vb = createRankTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.sortValue, fzd.dataValue);
    op->processBatch(new StreamRecord(vb));
}

void TestRankWithUpdate(const RankFuzzData& fzd)
{
    std::cout << "TestRankWithUpdate" << std::endl;

    const json rankConfig = json::parse(RANK_DESC_TOP3);

    auto func = reinterpret_cast<KeyedProcessFunction<RowData*, RowData *, RowData *> *>(
        new AppendOnlyTopNFunction<RowData*>(rankConfig));

    json newRankConfig = rankConfig;
    BatchOutputTest *output = new BatchOutputTest();
    auto *op = new KeyedProcessOperator(func, output, newRankConfig);
    op->setup();

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> typeInfo {
        omnistream::RowField("col0", BasicLogicalType::BIGINT),
        omnistream::RowField("col1", BasicLogicalType::BIGINT),
        omnistream::RowField("col2", BasicLogicalType::BIGINT)};
    TypeSerializer *ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));
    op->initializeState(initializer, ser);
    op->open();

    omnistream::VectorBatch* vb1 = createRankTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.sortValue, fzd.dataValue);
    op->processBatch(new StreamRecord(vb1));

    omnistream::VectorBatch* vb2 = createRankTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.sortValue + 100, fzd.dataValue + 100);
    op->processBatch(new StreamRecord(vb2));
}

void TestRankWithoutRowNumber(const RankFuzzData& fzd)
{
    std::cout << "TestRankWithoutRowNumber" << std::endl;

    const json rankConfig = json::parse(RANK_DESC_NO_ROWNUM);

    auto func = reinterpret_cast<KeyedProcessFunction<long, RowData *, RowData *> *>(
        new AppendOnlyTopNFunction<long>(rankConfig));

    json newRankConfig = rankConfig;
    BatchOutputTest *output = new BatchOutputTest();
    auto *op = new KeyedProcessOperator(func, output, newRankConfig);
    op->setup();

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> typeInfo {
        omnistream::RowField("col0", BasicLogicalType::BIGINT),
        omnistream::RowField("col1", BasicLogicalType::BIGINT),
        omnistream::RowField("col2", BasicLogicalType::BIGINT)};
    TypeSerializer *ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));
    op->initializeState(initializer, ser);
    op->open();

    for (int p = 0; p < 3; p++) {
        omnistream::VectorBatch* vb = createRankTestVectorBatch(fzd.loopCount, fzd.keyValue + p, fzd.sortValue, fzd.dataValue);
        op->processBatch(new StreamRecord(vb));
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
        case 3: TestRankWithoutRowNumber(fzd); break;
        default:
            TestRankBasic(fzd);
            TestRankWithUpdate(fzd);
            TestRankWithoutRowNumber(fzd);
            break;
    }
    return 0;
}
