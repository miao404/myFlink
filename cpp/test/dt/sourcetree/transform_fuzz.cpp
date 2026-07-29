#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "test/core/operators/OutputTest.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createTransformTestVectorBatch(int rowCount, int64_t value1, int64_t value2, int64_t value3, int32_t rowKindVal)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col1 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col2 = new omniruntime::vec::Vector<int64_t>(rowCount);

    RowKind rk = RowKind::INSERT;
    if (rowKindVal == 1) rk = RowKind::UPDATE_AFTER;
    else if (rowKindVal == 2) rk = RowKind::UPDATE_BEFORE;
    else if (rowKindVal == 3) rk = RowKind::DELETE;

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, value1 + i);
        col1->SetValue(i, value2 + i);
        col2->SetValue(i, value3 + i);
        vb->setRowKind(i, rk);
    }

    vb->Append(col0);
    vb->Append(col1);
    vb->Append(col2);

    return vb;
}

void TestTransformFilter(const TransformFuzzData& fzd)
{
    std::cout << "TestTransformFilter" << std::endl;

    std::string configStr = R"JSON({
        "name": "StreamFilter",
        "description": {
            "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
            "outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
            "filterCondition": "col0 > 0"
        },
        "id": "StreamFilter"
    })JSON";

    json parsedJson = json::parse(configStr);
    std::cout << "Transform-Filter config parsed: " << parsedJson["name"] << std::endl;

    omnistream::VectorBatch* vb = createTransformTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2, fzd.value3, fzd.rowKind);
    std::cout << "Transform-Filter VectorBatch created with " << fzd.loopCount << " rows" << std::endl;

    delete vb;
}

void TestTransformMap(const TransformFuzzData& fzd)
{
    std::cout << "TestTransformMap" << std::endl;

    std::string configStr = R"JSON({
        "name": "StreamMap",
        "description": {
            "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
            "outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
            "udfClassName": "TestMapFunction"
        },
        "id": "StreamMap"
    })JSON";

    json parsedJson = json::parse(configStr);
    std::cout << "Transform-Map config parsed: " << parsedJson["name"] << std::endl;

    omnistream::VectorBatch* vb = createTransformTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2, fzd.value3, fzd.rowKind);
    std::cout << "Transform-Map VectorBatch created with " << fzd.loopCount << " rows" << std::endl;

    delete vb;
}

void TestTransformFlatMap(const TransformFuzzData& fzd)
{
    std::cout << "TestTransformFlatMap" << std::endl;

    std::string configStr = R"JSON({
        "name": "StreamFlatMap",
        "description": {
            "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
            "outputTypes": ["BIGINT", "BIGINT", "BIGINT"]
        },
        "id": "StreamFlatMap"
    })JSON";

    json parsedJson = json::parse(configStr);
    std::cout << "Transform-FlatMap config parsed: " << parsedJson["name"] << std::endl;

    omnistream::VectorBatch* vb = createTransformTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2, fzd.value3, fzd.rowKind);
    std::cout << "Transform-FlatMap VectorBatch created with " << fzd.loopCount << " rows" << std::endl;

    delete vb;
}

int GlobalTransformFuzz(struct TransformFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "TransformFuzz: chooseFunc=" << chooseFunc
              << ", transformType=" << fzd.transformType
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestTransformFilter(fzd); break;
        case 2: TestTransformMap(fzd); break;
        case 3: TestTransformFlatMap(fzd); break;
        default:
            TestTransformFilter(fzd);
            TestTransformMap(fzd);
            TestTransformFlatMap(fzd);
            break;
    }
    return 0;
}
