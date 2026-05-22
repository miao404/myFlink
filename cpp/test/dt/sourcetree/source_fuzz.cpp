#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "test/core/operators/OutputTest.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createSourceTestVectorBatch(int rowCount, int64_t value1, int64_t value2)
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

void TestSourceBasic(const SourceFuzzData& fzd)
{
    std::cout << "TestSourceBasic" << std::endl;

    std::string configStr = R"JSON({
        "name": "CsvTableSource",
        "description": {
            "inputTypes": ["BIGINT", "BIGINT"],
            "outputTypes": ["BIGINT", "BIGINT"],
            "filePath": "/tmp/test_source.csv",
            "fieldDelimiter": ","
        },
        "id": "org.apache.flink.table.runtime.operators.source.SourceOperator"
    })JSON";

    json parsedJson = json::parse(configStr);
    std::cout << "Source config parsed: " << parsedJson["name"] << std::endl;

    omnistream::VectorBatch* vb = createSourceTestVectorBatch(fzd.loopCount, fzd.value1, fzd.value2);
    std::cout << "Source VectorBatch created with " << fzd.loopCount << " rows" << std::endl;

    delete vb;
}

void TestSourceMultiField(const SourceFuzzData& fzd)
{
    std::cout << "TestSourceMultiField" << std::endl;

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(fzd.loopCount);
    for (int f = 0; f < fzd.fieldCount; f++) {
        auto col = new omniruntime::vec::Vector<int64_t>(fzd.loopCount);
        for (int i = 0; i < fzd.loopCount; i++) {
            col->SetValue(i, fzd.value1 + f * 100 + i);
        }
        vb->Append(col);
    }

    std::cout << "Source multi-field VectorBatch created: " << fzd.fieldCount << " fields" << std::endl;

    delete vb;
}

void TestSourceBatch(const SourceFuzzData& fzd)
{
    std::cout << "TestSourceBatch" << std::endl;

    for (int batch = 0; batch < 3; batch++) {
        omnistream::VectorBatch* vb = createSourceTestVectorBatch(fzd.loopCount, fzd.value1 + batch * 100, fzd.value2);
        std::cout << "Source batch " << batch << " created" << std::endl;
        delete vb;
    }
}

int GlobalSourceFuzz(struct SourceFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "SourceFuzz: chooseFunc=" << chooseFunc
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestSourceBasic(fzd); break;
        case 2: TestSourceMultiField(fzd); break;
        case 3: TestSourceBatch(fzd); break;
        default:
            TestSourceBasic(fzd);
            TestSourceMultiField(fzd);
            TestSourceBatch(fzd);
            break;
    }
    return 0;
}
