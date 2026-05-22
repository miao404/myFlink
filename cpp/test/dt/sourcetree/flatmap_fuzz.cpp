#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "test/core/operators/OutputTest.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createFlatMapTestVectorBatch(int rowCount, int64_t value1, int64_t value2)
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

void TestFlatMapBasic(const FlatMapFuzzData& fzd)
{
    std::cout << "TestFlatMapBasic" << std::endl;

    std::string configStr = R"JSON({
        "name": "StreamFlatMap",
        "description": {
            "inputTypes": ["BIGINT", "BIGINT"],
            "outputTypes": ["BIGINT", "BIGINT"]
        },
        "id": "StreamFlatMap"
    })JSON";

    json parsedJson = json::parse(configStr);
    std::cout << "FlatMap config parsed: " << parsedJson["name"] << std::endl;

    omnistream::VectorBatch* vb = createFlatMapTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2);
    std::cout << "FlatMap VectorBatch created with " << fzd.loopCount << " rows" << std::endl;

    delete vb;
}

void TestFlatMapMultiOutput(const FlatMapFuzzData& fzd)
{
    std::cout << "TestFlatMapMultiOutput" << std::endl;

    for (int out = 0; out < fzd.outputCount; out++) {
        omnistream::VectorBatch* vb = createFlatMapTestVectorBatch(fzd.loopCount, fzd.value1 + out * 10, fzd.value2);
        std::cout << "FlatMap output " << out << " VectorBatch created" << std::endl;
        delete vb;
    }
}

void TestFlatMapMultiBatch(const FlatMapFuzzData& fzd)
{
    std::cout << "TestFlatMapMultiBatch" << std::endl;

    for (int batch = 0; batch < 3; batch++) {
        omnistream::VectorBatch* vb = createFlatMapTestVectorBatch(fzd.loopCount, fzd.value1 + batch * 100, fzd.value2);
        std::cout << "FlatMap batch " << batch << " created" << std::endl;
        delete vb;
    }
}

int GlobalFlatMapFuzz(struct FlatMapFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "FlatMapFuzz: chooseFunc=" << chooseFunc
              << ", outputCount=" << fzd.outputCount
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestFlatMapBasic(fzd); break;
        case 2: TestFlatMapMultiOutput(fzd); break;
        case 3: TestFlatMapMultiBatch(fzd); break;
        default:
            TestFlatMapBasic(fzd);
            TestFlatMapMultiOutput(fzd);
            TestFlatMapMultiBatch(fzd);
            break;
    }
    return 0;
}
