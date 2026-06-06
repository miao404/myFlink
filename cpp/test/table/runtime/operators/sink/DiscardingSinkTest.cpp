#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include "table/runtime/operators/sink/DiscardingSink.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"

using json = nlohmann::json;

TEST(DiscardingSinkTest, ConstructionEmpty) {
    json desc = json::object();
    DiscardingSink sink(desc);
    SUCCEED();
}

TEST(DiscardingSinkTest, ConstructionWithInputTypes) {
    json desc = {
        {"inputTypes", {"BIGINT", "VARCHAR(100)", "INT"}}
    };
    DiscardingSink sink(desc);
    SUCCEED();
}

TEST(DiscardingSinkTest, ConstructionWithDecimalType) {
    json desc = {
        {"inputTypes", {"DECIMAL64(10, 2)", "BIGINT"}}
    };
    DiscardingSink sink(desc);
    SUCCEED();
}

TEST(DiscardingSinkTest, ConstructionWithOutputFile) {
    json desc = {
        {"outputfile", "/tmp/test_output.csv"}
    };
    DiscardingSink sink(desc);
    SUCCEED();
}

TEST(DiscardingSinkTest, ConstructionWithTimeZone) {
    json desc = {
        {"timeZone", "UTC"}
    };
    DiscardingSink sink(desc);
    SUCCEED();
}

TEST(DiscardingSinkTest, InvokeVecBatchDiscards) {
    json desc = json::object();
    DiscardingSink sink(desc);

    auto* vb = new omnistream::VectorBatch(1);
    auto* record = new StreamRecord(vb);
    sink.invoke(record, SinkInputValueType::VEC_BATCH);
    SUCCEED();
}

TEST(DiscardingSinkTest, WriteWatermark) {
    json desc = json::object();
    DiscardingSink sink(desc);

    Watermark* wm = new Watermark(1000);
    sink.writeWatermark(wm);
    SUCCEED();
}

TEST(DiscardingSinkTest, Finish) {
    json desc = json::object();
    DiscardingSink sink(desc);
    sink.finish();
    SUCCEED();
}
