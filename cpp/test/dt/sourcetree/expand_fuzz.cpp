#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/runtime/operators/expand/StreamExpand.h"
#include "test/core/operators/OutputTest.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createExpandTestVectorBatch(int rowCount, int64_t value1, int64_t value2, int64_t value3)
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

static const std::string EXPAND_DESC_2 = R"JSON({"name":"Expand(projects=[{col0, col1, 0}, {col0, col2, 1}])","description":{"originDescription":null,"inputTypes":["BIGINT","BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT","BIGINT"],"projects":[[{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":0},{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":1},{"exprType":"LITERAL","dataType":9,"value":"0"}],[{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":0},{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":2},{"exprType":"LITERAL","dataType":9,"value":"1"}]]},"id":"StreamExecExpand"})JSON";

static const std::string EXPAND_DESC_3 = R"JSON({"name":"Expand(projects=[{col0, col1, 0}, {col0, col2, 1}, {col0, null, 2}])","description":{"originDescription":null,"inputTypes":["BIGINT","BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT","BIGINT"],"projects":[[{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":0},{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":1},{"exprType":"LITERAL","dataType":9,"value":"0"}],[{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":0},{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":2},{"exprType":"LITERAL","dataType":9,"value":"1"}],[{"exprType":"FIELD_REFERENCE","dataType":9,"colVal":0},{"exprType":"LITERAL","dataType":0,"value":null},{"exprType":"LITERAL","dataType":9,"value":"2"}]]},"id":"StreamExecExpand"})JSON";

void TestExpandBasic(const ExpandFuzzData& fzd)
{
    std::cout << "TestExpandBasic" << std::endl;

    json parsedJson = json::parse(EXPAND_DESC_2);

    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    StreamExpand expandOp(parsedJson, output);
    expandOp.open();

    omnistream::VectorBatch* vb = createExpandTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2, fzd.value3);
    StreamRecord *record = new StreamRecord(vb);
    expandOp.processBatch(record);

    delete record;
}

void TestExpandThreeProjections(const ExpandFuzzData& fzd)
{
    std::cout << "TestExpandThreeProjections" << std::endl;

    json parsedJson = json::parse(EXPAND_DESC_3);

    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    StreamExpand expandOp(parsedJson, output);
    expandOp.open();

    omnistream::VectorBatch* vb = createExpandTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2, fzd.value3);
    StreamRecord *record = new StreamRecord(vb);
    expandOp.processBatch(record);

    delete record;
}

void TestExpandWithRowKind(const ExpandFuzzData& fzd)
{
    std::cout << "TestExpandWithRowKind" << std::endl;

    json parsedJson = json::parse(EXPAND_DESC_2);

    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    StreamExpand expandOp(parsedJson, output);
    expandOp.open();

    omnistream::VectorBatch* vb = createExpandTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2, fzd.value3);
    for (int i = 0; i < fzd.loopCount; i++) {
        vb->setRowKind(i, (i % 2 == 0) ? RowKind::INSERT : RowKind::UPDATE_AFTER);
    }
    StreamRecord *record = new StreamRecord(vb);
    expandOp.processBatch(record);

    delete record;
}

int GlobalExpandFuzz(struct ExpandFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "ExpandFuzz: chooseFunc=" << chooseFunc
              << ", projectCount=" << fzd.projectCount
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestExpandBasic(fzd); break;
        case 2: TestExpandThreeProjections(fzd); break;
        case 3: TestExpandWithRowKind(fzd); break;
        default:
            TestExpandBasic(fzd);
            TestExpandThreeProjections(fzd);
            TestExpandWithRowKind(fzd);
            break;
    }
    return 0;
}
