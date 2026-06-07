/*
 * Unit tests for StreamFilter<F, K>
 */
#include <gtest/gtest.h>
#include "streaming/api/operators/StreamFilter.h"
#include "test/core/operators/OutputTest.h"
#include "test/core/operators/test_utils/Mocks.h"

// ---- Concrete FilterFunction implementations for testing ----

class AlwaysTrueFilter : public FilterFunction<Object> {
public:
    bool filter(Object* input) override { return true; }
};

class AlwaysFalseFilter : public FilterFunction<Object> {
public:
    bool filter(Object* input) override { return false; }
};

class EvenFilter : public FilterFunction<Object> {
public:
    bool filter(Object* input) override {
        auto* mock = dynamic_cast<MockObject*>(input);
        return mock && (mock->getValue() % 2 == 0);
    }
};

using StreamFilterObj = omnistream::datastream::StreamFilter<Object, Object*>;

// ---------- Construction with FilterFunction ----------

TEST(StreamFilterTest, ConstructWithFilterFunction) {
    OutputTest out;
    auto* filter = new AlwaysTrueFilter();
    StreamFilterObj op(&out, filter, false);
    EXPECT_EQ(op.GetOutput(), &out);
    EXPECT_EQ(std::string(op.getName()), "StreamFilter");
}

// ---------- processElement: filter passes ----------

TEST(StreamFilterTest, ProcessElementFilterPasses) {
    OutputTest out;
    auto* filter = new AlwaysTrueFilter();
    StreamFilterObj op(&out, filter, false);

    auto* obj = new MockObject(42);
    obj->getRefCount();
    StreamRecord record(obj);
    op.processElement(&record);

    auto& collected = out.getAll();
    EXPECT_EQ(collected.size(), 1u);
}

// ---------- processElement: filter rejects ----------

TEST(StreamFilterTest, ProcessElementFilterRejects) {
    OutputTest out;
    auto* filter = new AlwaysFalseFilter();
    StreamFilterObj op(&out, filter, false);

    auto* obj = new MockObject(42);
    obj->getRefCount();
    StreamRecord record(obj);
    op.processElement(&record);

    auto& collected = out.getAll();
    EXPECT_EQ(collected.size(), 0u);
}

// ---------- processElement: selective filter ----------

TEST(StreamFilterTest, ProcessElementSelectiveFilter) {
    OutputTest out;
    auto* filter = new EvenFilter();
    StreamFilterObj op(&out, filter, false);

    // Even value - should pass
    auto* obj1 = new MockObject(4);
    obj1->getRefCount();
    StreamRecord record1(obj1);
    op.processElement(&record1);

    // Odd value - should be rejected
    auto* obj2 = new MockObject(3);
    obj2->getRefCount();
    StreamRecord record2(obj2);
    op.processElement(&record2);

    auto& collected = out.getAll();
    EXPECT_EQ(collected.size(), 1u);
}

// ---------- getName ----------

TEST(StreamFilterTest, GetName) {
    OutputTest out;
    auto* filter = new AlwaysTrueFilter();
    StreamFilterObj op(&out, filter, false);
    EXPECT_STREQ(op.getName(), "StreamFilter");
}

// ---------- getTypeName ----------

TEST(StreamFilterTest, GetTypeNameContainsStreamFilter) {
    OutputTest out;
    auto* filter = new AlwaysTrueFilter();
    StreamFilterObj op(&out, filter, false);
    auto typeName = op.getTypeName();
    EXPECT_TRUE(typeName.find("StreamFilter") != std::string::npos);
}

// ---------- canBeStreamOperator ----------

TEST(StreamFilterTest, CanBeStreamOperatorFalse) {
    OutputTest out;
    auto* filter = new AlwaysTrueFilter();
    StreamFilterObj op(&out, filter, false);
    EXPECT_FALSE(op.canBeStreamOperator());
}

TEST(StreamFilterTest, CanBeStreamOperatorTrue) {
    OutputTest out;
    auto* filter = new AlwaysTrueFilter();
    StreamFilterObj op(&out, filter, true);
    EXPECT_TRUE(op.canBeStreamOperator());
}

// ---------- open / close ----------

TEST(StreamFilterTest, OpenDoesNotCrash) {
    OutputTest out;
    auto* filter = new AlwaysTrueFilter();
    StreamFilterObj op(&out, filter, false);
    EXPECT_NO_THROW(op.open());
}

TEST(StreamFilterTest, CloseDoesNotCrash) {
    OutputTest out;
    auto* filter = new AlwaysTrueFilter();
    StreamFilterObj op(&out, filter, false);
    EXPECT_NO_THROW(op.close());
}

// ---------- initializeState ----------

TEST(StreamFilterTest, InitializeStateDoesNothing) {
    OutputTest out;
    auto* filter = new AlwaysTrueFilter();
    StreamFilterObj op(&out, filter, false);
    // initializeState with nullptr should be a no-op
    EXPECT_NO_THROW(op.initializeState(nullptr, nullptr));
}

// ---------- ProcessWatermark ----------

TEST(StreamFilterTest, ProcessWatermarkForwarded) {
    OutputTest out;
    auto* filter = new AlwaysTrueFilter();
    StreamFilterObj op(&out, filter, false);
    op.setup();
    Watermark wm(99999);
    op.ProcessWatermark(&wm);
    ASSERT_NE(out.getWatermark(), nullptr);
    EXPECT_EQ(out.getWatermark()->getTimestamp(), 99999);
}

// ---------- processWatermarkStatus ----------

TEST(StreamFilterTest, ProcessWatermarkStatusForwarded) {
    OutputTest out;
    auto* filter = new AlwaysTrueFilter();
    StreamFilterObj op(&out, filter, false);
    op.setup();
    WatermarkStatus status(WatermarkStatus::IDLE_STATUS);
    EXPECT_NO_THROW(op.processWatermarkStatus(&status));
}

/*
 * Interfaces NOT tested and reasons:
 *
 * 1. StreamFilter(Output*, json, bool) constructor with UDF loading:
 *    Requires a valid shared library (.so) path in config["udf_so"] and a
 *    UDF object JSON. The UDFLoader::LoadFilterFunction dynamically loads
 *    a symbol from the .so file, which is not available in test env.
 *
 * 2. loadUdf(json):
 *    Same as above - requires runtime UDF shared library loading.
 *
 * 3. processBatch(StreamRecord*):
 *    Requires a VectorBatch with proper omniruntime vectors. The method
 *    calls userFunction->filterBatch() and then processes column vectors
 *    by type (OMNI_LONG, OMNI_VARCHAR, OMNI_BOOLEAN). Creating valid
 *    VectorBatch objects requires the omniruntime vector library which
 *    may not be fully available in test environment.
 *
 * 4. applyFilterResult(...):
 *    Private helper called by processBatch. Handles column-level filtering
 *    by vector type. Cannot be tested independently and requires valid
 *    omniruntime vectors.
 */
