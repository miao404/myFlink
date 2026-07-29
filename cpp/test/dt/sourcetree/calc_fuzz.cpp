#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/runtime/operators/calc/StreamCalcBatch.h"
#include "test/core/operators/OutputTest.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createCalcTestVectorBatch(int rowCount, int64_t value1, int64_t value2, int64_t value3)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col1 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col2 = new omniruntime::vec::Vector<int64_t>(rowCount);

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, value1 + i);
        col1->SetValue(i, value2 + i);
        col2->SetValue(i, value3 + i);
        vb->setRowKind(i, RowKind::INSERT);
    }

    vb->Append(col0);
    vb->Append(col1);
    vb->Append(col2);

    return vb;
}

static const std::string CALC_DESC_PROJ = R"JSON({"name":"Calc(select=[col0, col1])","description":{"originDescription":null,"inputTypes":["BIGINT","BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT"],"indices":[{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":0},{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":1}],"condition":null},"id":"StreamExecCalc"})JSON";

static const std::string CALC_DESC_FILTER = R"JSON({"name":"Calc(select=[col0, col1], where=[(col2 > 10)])","description":{"originDescription":null,"inputTypes":["BIGINT","BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT"],"indices":[{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":0},{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":1}],"condition":{"exprType":"GREATER_THAN","dataType":1,"left":{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":2},"right":{"exprType":"LITERAL","dataType":9,"value":"10"}}},"id":"StreamExecCalc"})JSON";

static const std::string CALC_DESC_EXPR = R"JSON({"name":"Calc(select=[(col0 + col1)])","description":{"originDescription":null,"inputTypes":["BIGINT","BIGINT","BIGINT"],"outputTypes":["BIGINT"],"indices":[{"exprType":"ADD","dataType":9,"left":{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":0},"right":{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":1}}],"condition":null},"id":"StreamExecCalc"})JSON";

void TestCalcProjection(const CalcFuzzData& fzd)
{
    std::cout << "TestCalcProjection" << std::endl;

    json parsedJson = json::parse(CALC_DESC_PROJ);

    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    StreamCalcBatch calcOp(parsedJson, output);
    calcOp.open();

    omnistream::VectorBatch* vb = createCalcTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2, fzd.value3);
    StreamRecord *record = new StreamRecord(vb);
    calcOp.processBatch(record);

    delete record;
}

void TestCalcWithFilter(const CalcFuzzData& fzd)
{
    std::cout << "TestCalcWithFilter" << std::endl;

    json parsedJson = json::parse(CALC_DESC_FILTER);

    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    StreamCalcBatch calcOp(parsedJson, output);
    calcOp.open();

    omnistream::VectorBatch* vb = createCalcTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2, fzd.value3);
    StreamRecord *record = new StreamRecord(vb);
    calcOp.processBatch(record);

    delete record;
}

void TestCalcExpression(const CalcFuzzData& fzd)
{
    std::cout << "TestCalcExpression" << std::endl;

    json parsedJson = json::parse(CALC_DESC_EXPR);

    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    StreamCalcBatch calcOp(parsedJson, output);
    calcOp.open();

    omnistream::VectorBatch* vb = createCalcTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2, fzd.value3);
    StreamRecord *record = new StreamRecord(vb);
    calcOp.processBatch(record);

    delete record;
}

int GlobalCalcFuzz(struct CalcFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "CalcFuzz: chooseFunc=" << chooseFunc
              << ", exprType=" << fzd.exprType
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestCalcProjection(fzd); break;
        case 2: TestCalcWithFilter(fzd); break;
        case 3: TestCalcExpression(fzd); break;
        default:
            TestCalcProjection(fzd);
            TestCalcWithFilter(fzd);
            TestCalcExpression(fzd);
            break;
    }
    return 0;
}
