#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/SinkOperator.h"
#include "test/core/operators/OutputTest.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createSinkTestVectorBatch(int rowCount, int64_t value1, int64_t value2, int32_t rowKindVal)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col1 = new omniruntime::vec::Vector<int64_t>(rowCount);

    RowKind rk = RowKind::INSERT;
    if (rowKindVal == 1) rk = RowKind::UPDATE_AFTER;
    else if (rowKindVal == 2) rk = RowKind::UPDATE_BEFORE;
    else if (rowKindVal == 3) rk = RowKind::DELETE;

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, value1 + i);
        col1->SetValue(i, value2 + i);
        vb->setRowKind(i, rk);
    }

    vb->Append(col0);
    vb->Append(col1);

    return vb;
}

static const std::string SINK_DESC_STR = R"JSON({"name":"blackHole sink","description":{"inputTypes":["BIGINT","BIGINT"]},"id":"org.apache.flink.table.runtime.operators.sink.SinkOperator"})JSON";

void TestSinkBasic(const SinkFuzzData& fzd)
{
    std::cout << "TestSinkBasic" << std::endl;

    json parsedJson = json::parse(SINK_DESC_STR);
    SinkOperator *sinkOp = new SinkOperator(parsedJson);
    sinkOp->open();

    omnistream::VectorBatch* vb = createSinkTestVectorBatch(fzd.loopCount, fzd.intValue, fzd.longValue, fzd.rowKind);
    StreamRecord *record = new StreamRecord(vb);
    sinkOp->processBatch(record);

    delete record;
    delete sinkOp;
}

void TestSinkWithRowKind(const SinkFuzzData& fzd)
{
    std::cout << "TestSinkWithRowKind" << std::endl;

    json parsedJson = json::parse(SINK_DESC_STR);
    SinkOperator *sinkOp = new SinkOperator(parsedJson);
    sinkOp->open();

    for (int rk = 0; rk < 4; rk++) {
        omnistream::VectorBatch* vb = createSinkTestVectorBatch(fzd.loopCount, fzd.intValue, fzd.longValue, rk);
        StreamRecord *record = new StreamRecord(vb);
        sinkOp->processBatch(record);
        delete record;
    }

    delete sinkOp;
}

void TestSinkMultiBatch(const SinkFuzzData& fzd)
{
    std::cout << "TestSinkMultiBatch" << std::endl;

    json parsedJson = json::parse(SINK_DESC_STR);
    SinkOperator *sinkOp = new SinkOperator(parsedJson);
    sinkOp->open();

    for (int batch = 0; batch < 5; batch++) {
        omnistream::VectorBatch* vb = createSinkTestVectorBatch(fzd.loopCount, fzd.intValue + batch * 100, fzd.longValue, fzd.rowKind);
        StreamRecord *record = new StreamRecord(vb);
        sinkOp->processBatch(record);
        delete record;
    }

    delete sinkOp;
}

int GlobalSinkFuzz(struct SinkFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "SinkFuzz: chooseFunc=" << chooseFunc
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestSinkBasic(fzd); break;
        case 2: TestSinkWithRowKind(fzd); break;
        case 3: TestSinkMultiBatch(fzd); break;
        default:
            TestSinkBasic(fzd);
            TestSinkWithRowKind(fzd);
            TestSinkMultiBatch(fzd);
            break;
    }
    return 0;
}
