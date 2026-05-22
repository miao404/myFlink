#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/ProcessOperator.h"
#include "test/core/operators/OutputTest.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createProcessTestVectorBatch(int rowCount, int64_t v1, int64_t v2, int64_t v3, int64_t v4, int64_t lookupKey)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col1 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col2 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col3 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col4 = new omniruntime::vec::Vector<int64_t>(rowCount);

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, v1 + i);
        col1->SetValue(i, v2 + i);
        col2->SetValue(i, v3 + i);
        col3->SetValue(i, v4 + i);
        col4->SetValue(i, lookupKey + i);
        vb->setRowKind(i, RowKind::INSERT);
    }

    vb->Append(col0);
    vb->Append(col1);
    vb->Append(col2);
    vb->Append(col3);
    vb->Append(col4);

    return vb;
}

static const std::string PROCESS_DESC = R"JSON({"name":"LookupJoin","description":{"inputTypes":["BIGINT","BIGINT","BIGINT","BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT","BIGINT","BIGINT","BIGINT"],"lookupKeys":[4],"resultFieldIndex":[0,1,2,3,4]},"id":"org.apache.flink.streaming.api.operators.ProcessOperator"})JSON";

void TestProcessBasic(const ProcessFuzzData& fzd)
{
    std::cout << "TestProcessBasic" << std::endl;

    json parsedJson = json::parse(PROCESS_DESC);

    BatchOutputTest* output = new BatchOutputTest();
    ProcessOperator *processOp = new ProcessOperator(parsedJson, output);
    processOp->open();

    omnistream::VectorBatch* vb = createProcessTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2, fzd.value3, fzd.value4, fzd.lookupKey);
    StreamRecord *record = new StreamRecord(vb);
    processOp->processBatch(record);

    delete record;
}

void TestProcessMultiBatch(const ProcessFuzzData& fzd)
{
    std::cout << "TestProcessMultiBatch" << std::endl;

    json parsedJson = json::parse(PROCESS_DESC);

    BatchOutputTest* output = new BatchOutputTest();
    ProcessOperator *processOp = new ProcessOperator(parsedJson, output);
    processOp->open();

    for (int batch = 0; batch < 3; batch++) {
        omnistream::VectorBatch* vb = createProcessTestVectorBatch(fzd.loopCount, fzd.value1 + batch * 100, fzd.value2, fzd.value3, fzd.value4, fzd.lookupKey);
        processOp->processBatch(new StreamRecord(vb));
    }
}

void TestProcessLargeScale(const ProcessFuzzData& fzd)
{
    std::cout << "TestProcessLargeScale" << std::endl;

    json parsedJson = json::parse(PROCESS_DESC);

    BatchOutputTest* output = new BatchOutputTest();
    ProcessOperator *processOp = new ProcessOperator(parsedJson, output);
    processOp->open();

    int scaleCount = fzd.loopCount > 0 ? fzd.loopCount : 100;
    omnistream::VectorBatch* vb = createProcessTestVectorBatch(scaleCount, fzd.value1, fzd.value2, fzd.value3, fzd.value4, fzd.lookupKey);
    processOp->processBatch(new StreamRecord(vb));
}

int GlobalProcessFuzz(struct ProcessFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "ProcessFuzz: chooseFunc=" << chooseFunc
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestProcessBasic(fzd); break;
        case 2: TestProcessMultiBatch(fzd); break;
        case 3: TestProcessLargeScale(fzd); break;
        default:
            TestProcessBasic(fzd);
            TestProcessMultiBatch(fzd);
            TestProcessLargeScale(fzd);
            break;
    }
    return 0;
}
