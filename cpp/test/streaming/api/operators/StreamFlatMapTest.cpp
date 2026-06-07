/*
 * Unit tests for StreamFlatMap<F, K>
 *
 * StreamFlatMap requires a UDF shared library (.so) for its primary
 * constructor. We cannot test that path without actual .so files.
 * However, we can test the interfaces through AbstractUdfStreamOperator
 * by creating a StreamFlatMap with a manually injected FlatMapFunction.
 *
 * Since StreamFlatMap does not have a constructor that accepts a raw
 * FlatMapFunction pointer (unlike StreamFilter), we can only test
 * the aspects accessible through its parent class or verify compilation
 * and explain the untestable interfaces.
 */
#include <gtest/gtest.h>
#include "streaming/api/operators/StreamFlatMap.h"
#include "test/core/operators/OutputTest.h"
#include "test/core/operators/test_utils/Mocks.h"

// Concrete FlatMapFunction for testing
class DuplicatingFlatMap : public FlatMapFunction<Object> {
public:
    void flatMap(Object* value, Collector* out) override {
        // Emit the value twice
        out->collect(value);
        auto* clone = value->clone();
        out->collect(clone);
    }
};

/*
 * All StreamFlatMap interfaces and testability analysis:
 *
 * 1. StreamFlatMap(Output*, json, bool) constructor:
 *    NOT TESTABLE - Requires config["udf_so"] pointing to a valid shared
 *    library and config["udf_obj"] with a UDF object JSON. The constructor
 *    calls UDFLoader::LoadFlatMapFunction which dlopen's the .so file.
 *    Without a real .so file this will throw.
 *
 * 2. ~StreamFlatMap():
 *    Deletes the TimestampedCollector. Cannot test independently without
 *    constructing the object first.
 *
 * 3. open():
 *    Calls AbstractUdfStreamOperator::open() and creates TimestampedCollector.
 *    Requires the operator to be constructed and userFunction to be set.
 *
 * 4. processElement(StreamRecord*):
 *    Sets timestamp on collector, extracts Object from record, calls
 *    userFunction->flatMap(). Requires open() to have been called (for
 *    collector creation) and a valid userFunction.
 *
 * 5. initializeState(StreamTaskStateInitializerImpl*, TypeSerializer*):
 *    No-op implementation. Would be testable but needs construction first.
 *
 * 6. ProcessWatermark(Watermark*):
 *    Delegates to AbstractStreamOperator::ProcessWatermark. Testable
 *    if operator is constructed.
 *
 * 7. processWatermarkStatus(WatermarkStatus*):
 *    Delegates to AbstractStreamOperator::processWatermarkStatus.
 *
 * 8. canBeStreamOperator():
 *    Returns this->isStream, set in constructor.
 *
 * 9. getName():
 *    Returns "StreamFlatMap". Testable if operator is constructed.
 *
 * ROOT CAUSE: Unlike StreamFilter which has a constructor accepting a raw
 * FilterFunction pointer, StreamFlatMap only has one constructor that
 * requires UDF loading from a shared library. Without either:
 * (a) A test .so file with a FlatMapFunction symbol, or
 * (b) A second constructor that accepts a FlatMapFunction* directly,
 * we cannot instantiate StreamFlatMap in tests.
 *
 * The pattern used in production code (StreamOperatorFactory::CreateFlatMapOp)
 * also uses the .so-loading constructor.
 */

// Verify that DuplicatingFlatMap works standalone (the UDF itself)
TEST(StreamFlatMapTest, FlatMapFunctionDuplicates) {
    DuplicatingFlatMap flatMap;
    OutputTest out;

    auto* obj = new MockStringObject("hello");
    obj->getRefCount();
    flatMap.flatMap(obj, &out);

    auto& collected = out.getAll();
    EXPECT_EQ(collected.size(), 2u);
    // Clean up
    delete obj;
}

// Verify FlatMapFunction interface contract
TEST(StreamFlatMapTest, FlatMapFunctionIsVirtual) {
    FlatMapFunction<Object>* base = new DuplicatingFlatMap();
    OutputTest out;

    auto* obj = new MockStringObject("test");
    obj->getRefCount();
    base->flatMap(obj, &out);

    EXPECT_EQ(out.getAll().size(), 2u);
    delete base;
    delete obj;
}
