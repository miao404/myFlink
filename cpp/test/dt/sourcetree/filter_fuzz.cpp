#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "test/core/operators/OutputTest.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createFilterTestVectorBatch(int rowCount, int64_t value1, int64_t value2, int64_t value3, int32_t rowKindVal)
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

void TestFilterBasic(const FilterFuzzData& fzd)
{
    std::cout << "TestFilterBasic" << std::endl;

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
    std::cout << "Filter config parsed: " << parsedJson["name"] << std::endl;

    omnistream::VectorBatch* vb = createFilterTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2, fzd.value3, fzd.rowKind);
    std::cout << "Filter VectorBatch created with " << fzd.loopCount << " rows" << std::endl;

    delete vb;
}

void TestFilterWithRowKind(const FilterFuzzData& fzd)
{
    std::cout << "TestFilterWithRowKind" << std::endl;

    for (int rk = 0; rk < 4; rk++) {
        omnistream::VectorBatch* vb = createFilterTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2, fzd.value3, rk);
        std::cout << "Filter rowKind=" << rk << " VectorBatch created" << std::endl;
        delete vb;
    }
}

void TestFilterMultiBatch(const FilterFuzzData& fzd)
{
    std::cout << "TestFilterMultiBatch" << std::endl;

    for (int batch = 0; batch < 3; batch++) {
        omnistream::VectorBatch* vb = createFilterTestVectorBatch(fzd.loopCount, fzd.value1 + batch * 100, fzd.value2, fzd.value3, fzd.rowKind);
        std::cout << "Filter batch " << batch << " created" << std::endl;
        delete vb;
    }
}

int GlobalFilterFuzz(struct FilterFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "FilterFuzz: chooseFunc=" << chooseFunc
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestFilterBasic(fzd); break;
        case 2: TestFilterWithRowKind(fzd); break;
        case 3: TestFilterMultiBatch(fzd); break;
        default:
            TestFilterBasic(fzd);
            TestFilterWithRowKind(fzd);
            TestFilterMultiBatch(fzd);
            break;
    }
    return 0;
}
