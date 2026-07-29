#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "test/core/operators/OutputTest.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createCoProcessTestVectorBatch(int rowCount, int64_t keyValue, int64_t value1, int64_t value2)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col1 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col2 = new omniruntime::vec::Vector<int64_t>(rowCount);

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, keyValue + i);
        col1->SetValue(i, value1 + i);
        col2->SetValue(i, value2 + i);
        vb->setRowKind(i, RowKind::INSERT);
    }

    vb->Append(col0);
    vb->Append(col1);
    vb->Append(col2);

    return vb;
}

void TestCoProcessBasic(const CoProcessFuzzData& fzd)
{
    std::cout << "TestCoProcessBasic" << std::endl;

    std::string configStr = R"JSON({
        "name": "KeyedCoProcess",
        "description": {
            "inputTypes1": ["BIGINT", "BIGINT", "BIGINT"],
            "inputTypes2": ["BIGINT", "BIGINT", "BIGINT"],
            "outputTypes": ["BIGINT", "BIGINT", "BIGINT"]
        },
        "id": "org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator"
    })JSON";

    json parsedJson = json::parse(configStr);
    std::cout << "CoProcess config parsed: " << parsedJson["name"] << std::endl;

    omnistream::VectorBatch* vb1 = createCoProcessTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.value1, fzd.value2);
    omnistream::VectorBatch* vb2 = createCoProcessTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.value1 + 100, fzd.value2 + 100);

    std::cout << "CoProcess VectorBatches created: input1=" << fzd.loopCount << " rows, input2=" << fzd.loopCount << " rows" << std::endl;

    delete vb1;
    delete vb2;
}

void TestCoProcessDualInput(const CoProcessFuzzData& fzd)
{
    std::cout << "TestCoProcessDualInput" << std::endl;

    omnistream::VectorBatch* vb1 = createCoProcessTestVectorBatch(fzd.loopCount, fzd.keyValue, fzd.value1, fzd.value2);
    omnistream::VectorBatch* vb2 = createCoProcessTestVectorBatch(fzd.loopCount, fzd.keyValue + 50, fzd.value1, fzd.value2);

    std::cout << "CoProcess dual-input test with key offset=50" << std::endl;

    delete vb1;
    delete vb2;
}

void TestCoProcessMultiBatch(const CoProcessFuzzData& fzd)
{
    std::cout << "TestCoProcessMultiBatch" << std::endl;

    for (int batch = 0; batch < 3; batch++) {
        omnistream::VectorBatch* vb1 = createCoProcessTestVectorBatch(fzd.loopCount, fzd.keyValue + batch, fzd.value1, fzd.value2);
        omnistream::VectorBatch* vb2 = createCoProcessTestVectorBatch(fzd.loopCount, fzd.keyValue + batch, fzd.value1, fzd.value2);
        std::cout << "CoProcess batch " << batch << " created" << std::endl;
        delete vb1;
        delete vb2;
    }
}

int GlobalCoProcessFuzz(struct CoProcessFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "CoProcessFuzz: chooseFunc=" << chooseFunc
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestCoProcessBasic(fzd); break;
        case 2: TestCoProcessDualInput(fzd); break;
        case 3: TestCoProcessMultiBatch(fzd); break;
        default:
            TestCoProcessBasic(fzd);
            TestCoProcessDualInput(fzd);
            TestCoProcessMultiBatch(fzd);
            break;
    }
    return 0;
}
