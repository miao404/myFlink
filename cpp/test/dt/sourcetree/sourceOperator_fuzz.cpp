#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "test/core/operators/OutputTest.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createSourceOpTestVectorBatch(int rowCount, int64_t value1, int64_t value2)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col1 = new omniruntime::vec::Vector<int64_t>(rowCount);

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, value1 + i);
        col1->SetValue(i, value2 + i);
        vb->setRowKind(i, RowKind::INSERT);
    }

    vb->Append(col0);
    vb->Append(col1);

    return vb;
}

static const std::string SOURCE_OP_DESC = R"JSON({"name":"StreamSource","description":{"inputTypes":["BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT"],"sourceClassName":"CsvInputFormat","filePath":"/tmp/test.csv","fieldDelimiter":","},"id":"StreamSource"})JSON";

void TestSourceOperatorBasic(const SourceOperatorFuzzData& fzd)
{
    std::cout << "TestSourceOperatorBasic" << std::endl;

    json parsedJson = json::parse(SOURCE_OP_DESC);
    std::cout << "SourceOperator config parsed: " << parsedJson["name"] << std::endl;

    omnistream::VectorBatch* vb = createSourceOpTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2);
    std::cout << "SourceOperator VectorBatch created with " << fzd.loopCount << " rows" << std::endl;

    delete vb;
}

void TestSourceOperatorMultiField(const SourceOperatorFuzzData& fzd)
{
    std::cout << "TestSourceOperatorMultiField" << std::endl;

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(fzd.loopCount);
    for (int f = 0; f < fzd.fieldCount; f++) {
        auto col = new omniruntime::vec::Vector<int64_t>(fzd.loopCount);
        for (int i = 0; i < fzd.loopCount; i++) {
            col->SetValue(i, fzd.value1 + f * 100 + i);
        }
        vb->Append(col);
    }

    std::cout << "SourceOperator multi-field VectorBatch: " << fzd.fieldCount << " fields" << std::endl;

    delete vb;
}

void TestSourceOperatorMultiSplit(const SourceOperatorFuzzData& fzd)
{
    std::cout << "TestSourceOperatorMultiSplit" << std::endl;

    for (int split = 0; split < 3; split++) {
        omnistream::VectorBatch* vb = createSourceOpTestVectorBatch(fzd.loopCount, fzd.value1 + split * 1000, fzd.value2);
        std::cout << "SourceOperator split " << split << " created" << std::endl;
        delete vb;
    }
}

int GlobalSourceOperatorFuzz(struct SourceOperatorFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "SourceOperatorFuzz: chooseFunc=" << chooseFunc
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestSourceOperatorBasic(fzd); break;
        case 2: TestSourceOperatorMultiField(fzd); break;
        case 3: TestSourceOperatorMultiSplit(fzd); break;
        default:
            TestSourceOperatorBasic(fzd);
            TestSourceOperatorMultiField(fzd);
            TestSourceOperatorMultiSplit(fzd);
            break;
    }
    return 0;
}
