#include "fuzz_wrapper.h"
#include "streaming/api/operators/StreamMap.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "test/core/operators/OutputTest.h"
#include "test/core/operators/test_utils/Mocks.h"
#include "test/core/operators/test_udf/MockMapFunction.h"
#include "streaming/api/watermark/Watermark.h"
#include "runtime/watermark/WatermarkStatus.h"
#include <nlohmann/json.hpp>
#include <iostream>

using json = nlohmann::json;

// Testable subclass: exposes userFunction setter for processElement testing
// without requiring external .so loading
template<typename F, typename K>
class TestableStreamMap : public omnistream::datastream::StreamMap<F, K> {
public:
    TestableStreamMap(Output *output, MapFunction<F>* func, bool isStream = true)
        : omnistream::datastream::StreamMap<F, K>(output, isStream)
    {
        this->userFunction = func;
    }
};

// --- Test 1: Constructor + getName ---
// Covers: StreamMapTest::Constructor_ValidPath, StreamMapTest::GetName
void TestMapConstructor(const MapFuzzData& fzd)
{
    std::cout << "TestMapConstructor" << std::endl;

    MockOutput output;
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output, true);

    std::cout << "  output set: " << (streamMap.GetOutput() == &output) << std::endl;
    std::cout << "  getName: " << streamMap.getName() << std::endl;
}

// --- Test 2: processElement ---
// Covers: StreamMapTest::ProcessElement_Valid (commented out in UT)
// Exercises: userFunction->map(), output->collect(), input->putRefCount()
void TestMapProcessElement(const MapFuzzData& fzd)
{
    std::cout << "TestMapProcessElement" << std::endl;

    MockOutput *output = new MockOutput();
    MockMapFunction *mapFunc = new MockMapFunction();
    TestableStreamMap<Object, Object*> streamMap(output, mapFunc, true);

    // Single element
    MockObject *input = new MockObject(static_cast<int>(fzd.value1));
    StreamRecord *record = new StreamRecord(input);
    streamMap.processElement(record);

    std::vector<StreamRecord*> collected = output->getCollectedRecords();
    std::cout << "  collected: " << collected.size() << std::endl;
    if (!collected.empty()) {
        MockObject *result = dynamic_cast<MockObject*>(collected[0]->getValue());
        if (result) {
            std::cout << "  result value: " << result->getValue()
                      << " (expected " << (static_cast<int>(fzd.value1) + 1) << ")" << std::endl;
        }
    }

    // Multiple elements driven by loopCount
    int count = (fzd.loopCount > 0 && fzd.loopCount < 100) ? fzd.loopCount : 5;
    for (int i = 1; i < count; i++) {
        MockObject *obj = new MockObject(static_cast<int>(fzd.value2 + i));
        StreamRecord *rec = new StreamRecord(obj);
        streamMap.processElement(rec);
    }
    std::cout << "  total collected: " << output->getCollectedRecords().size() << std::endl;

    delete output;
}

// --- Test 3: loadUdf error path ---
// Covers: StreamMapTest::Constructor_InvalidPath
void TestMapLoadUdfInvalid(const MapFuzzData& fzd)
{
    std::cout << "TestMapLoadUdfInvalid" << std::endl;

    MockOutput output;
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output);

    nlohmann::json config;
    config["udf_so"] = "nonexistent_path.so";
    config["udf_obj"] = "{}";

    bool threw = false;
    try {
        streamMap.loadUdf(config);
    } catch (const std::out_of_range&) {
        threw = true;
    }
    std::cout << "  loadUdf(invalid) threw out_of_range: " << threw << std::endl;
}

// --- Test 4: open / close lifecycle ---
// Covers: StreamMapTest::Open_NotImplemented, StreamMapTest::Close_NotImplemented
void TestMapOpenClose(const MapFuzzData& fzd)
{
    std::cout << "TestMapOpenClose" << std::endl;

    MockOutput output;
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output, true);

    streamMap.open();
    std::cout << "  open() ok" << std::endl;

    streamMap.close();
    std::cout << "  close() ok" << std::endl;
}

// --- Test 5: canBeStreamOperator ---
// Not covered in StreamMapTest UT, exercises isStream flag
void TestMapCanBeStreamOperator(const MapFuzzData& fzd)
{
    std::cout << "TestMapCanBeStreamOperator" << std::endl;

    MockOutput output;

    omnistream::datastream::StreamMap<Object, Object*> asStream(&output, true);
    std::cout << "  isStream=true  -> " << asStream.canBeStreamOperator() << std::endl;

    omnistream::datastream::StreamMap<Object, Object*> notStream(&output, false);
    std::cout << "  isStream=false -> " << notStream.canBeStreamOperator() << std::endl;
}

// --- Test 6: ProcessWatermark + processWatermarkStatus ---
// Not covered in StreamMapTest UT, exercises watermark forwarding path
void TestMapWatermarkHandling(const MapFuzzData& fzd)
{
    std::cout << "TestMapWatermarkHandling" << std::endl;

    MockOutput *output = new MockOutput();
    omnistream::datastream::StreamMap<Object, Object*> streamMap(output, true);
    streamMap.setup();

    Watermark *wm = new Watermark(fzd.value1);
    streamMap.ProcessWatermark(wm);
    std::cout << "  ProcessWatermark(" << fzd.value1 << ") ok" << std::endl;

    streamMap.processWatermarkStatus(WatermarkStatus::idle());
    std::cout << "  processWatermarkStatus(IDLE) ok" << std::endl;

    streamMap.processWatermarkStatus(WatermarkStatus::active());
    std::cout << "  processWatermarkStatus(ACTIVE) ok" << std::endl;

    delete output;
}

int GlobalMapFuzz(struct MapFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "MapFuzz: chooseFunc=" << chooseFunc
              << ", value1=" << fzd.value1
              << ", value2=" << fzd.value2
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestMapConstructor(fzd); break;
        case 2: TestMapProcessElement(fzd); break;
        case 3: TestMapLoadUdfInvalid(fzd); break;
        case 4: TestMapOpenClose(fzd); break;
        case 5: TestMapCanBeStreamOperator(fzd); break;
        case 6: TestMapWatermarkHandling(fzd); break;
        default:
            TestMapConstructor(fzd);
            TestMapProcessElement(fzd);
            TestMapLoadUdfInvalid(fzd);
            TestMapOpenClose(fzd);
            TestMapCanBeStreamOperator(fzd);
            TestMapWatermarkHandling(fzd);
            break;
    }

    return 0;
}
