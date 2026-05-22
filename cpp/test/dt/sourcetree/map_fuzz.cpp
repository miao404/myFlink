#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "test/core/operators/OutputTest.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createMapTestVectorBatch(int rowCount, int64_t value1, int64_t value2)
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

static const std::string MAP_DESC = R"JSON({"name":"StreamMap","description":{"inputTypes":["BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT"],"udfClassName":"TestMapFunction"},"id":"StreamMap"})JSON";

void TestMapBasic(const MapFuzzData& fzd)
{
    std::cout << "TestMapBasic" << std::endl;

    json parsedJson = json::parse(MAP_DESC);
    std::cout << "Map config parsed: " << parsedJson["name"] << std::endl;

    omnistream::VectorBatch* vb = createMapTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2);
    std::cout << "Map VectorBatch created with " << fzd.loopCount << " rows" << std::endl;

    delete vb;
}

void TestMapConfigValidation(const MapFuzzData& fzd)
{
    std::cout << "TestMapConfigValidation" << std::endl;

    json parsedJson = json::parse(MAP_DESC);

    bool hasInputTypes = parsedJson["description"].contains("inputTypes");
    bool hasOutputTypes = parsedJson["description"].contains("outputTypes");
    std::cout << "Map config validation: inputTypes=" << hasInputTypes
              << ", outputTypes=" << hasOutputTypes << std::endl;

    omnistream::VectorBatch* vb = createMapTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2);
    delete vb;
}

void TestMapMultiBatch(const MapFuzzData& fzd)
{
    std::cout << "TestMapMultiBatch" << std::endl;

    for (int batch = 0; batch < 3; batch++) {
        omnistream::VectorBatch* vb = createMapTestVectorBatch(fzd.loopCount, fzd.value1 + batch * 100, fzd.value2);
        std::cout << "Map batch " << batch << " created" << std::endl;
        delete vb;
    }
}

int GlobalMapFuzz(struct MapFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "MapFuzz: chooseFunc=" << chooseFunc
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestMapBasic(fzd); break;
        case 2: TestMapConfigValidation(fzd); break;
        case 3: TestMapMultiBatch(fzd); break;
        default:
            TestMapBasic(fzd);
            TestMapConfigValidation(fzd);
            TestMapMultiBatch(fzd);
            break;
    }
    return 0;
}
