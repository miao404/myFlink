#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/WatermarkAssignerOperator.h"
#include "test/core/operators/OutputTest.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createWatermarkTestVectorBatch(int rowCount, int64_t timestampValue, int64_t dataValue)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col1 = new omniruntime::vec::Vector<int64_t>(rowCount);

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, timestampValue + i * 100);
        col1->SetValue(i, dataValue + i);
        vb->setRowKind(i, RowKind::INSERT);
    }

    vb->Append(col0);
    vb->Append(col1);

    return vb;
}

void TestWatermarkBasic(const WatermarkAssignerFuzzData& fzd)
{
    std::cout << "TestWatermarkBasic" << std::endl;

    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    WatermarkAssignerOperator *wmOp = new WatermarkAssignerOperator(
        output, fzd.timeRowIndex, fzd.outOfOrderTime, fzd.emissionInterval, nullptr);
    wmOp->open();

    omnistream::VectorBatch* vb = createWatermarkTestVectorBatch(fzd.loopCount, fzd.timestampValue, fzd.dataValue);
    StreamRecord *record = new StreamRecord(vb);
    wmOp->processBatch(record);

    delete record;
}

void TestWatermarkOutOfOrder(const WatermarkAssignerFuzzData& fzd)
{
    std::cout << "TestWatermarkOutOfOrder" << std::endl;

    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    WatermarkAssignerOperator *wmOp = new WatermarkAssignerOperator(
        output, fzd.timeRowIndex, fzd.outOfOrderTime, fzd.emissionInterval, nullptr);
    wmOp->open();

    omnistream::VectorBatch* vb1 = createWatermarkTestVectorBatch(fzd.loopCount, fzd.timestampValue + 1000, fzd.dataValue);
    wmOp->processBatch(new StreamRecord(vb1));

    omnistream::VectorBatch* vb2 = createWatermarkTestVectorBatch(fzd.loopCount, fzd.timestampValue, fzd.dataValue);
    wmOp->processBatch(new StreamRecord(vb2));
}

void TestWatermarkMultiBatch(const WatermarkAssignerFuzzData& fzd)
{
    std::cout << "TestWatermarkMultiBatch" << std::endl;

    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    WatermarkAssignerOperator *wmOp = new WatermarkAssignerOperator(
        output, fzd.timeRowIndex, fzd.outOfOrderTime, fzd.emissionInterval, nullptr);
    wmOp->open();

    for (int batch = 0; batch < 5; batch++) {
        omnistream::VectorBatch* vb = createWatermarkTestVectorBatch(fzd.loopCount, fzd.timestampValue + batch * 500, fzd.dataValue);
        wmOp->processBatch(new StreamRecord(vb));
    }
}

int GlobalWatermarkAssignerFuzz(struct WatermarkAssignerFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "WatermarkAssignerFuzz: chooseFunc=" << chooseFunc
              << ", outOfOrderTime=" << fzd.outOfOrderTime
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestWatermarkBasic(fzd); break;
        case 2: TestWatermarkOutOfOrder(fzd); break;
        case 3: TestWatermarkMultiBatch(fzd); break;
        default:
            TestWatermarkBasic(fzd);
            TestWatermarkOutOfOrder(fzd);
            TestWatermarkMultiBatch(fzd);
            break;
    }
    return 0;
}
