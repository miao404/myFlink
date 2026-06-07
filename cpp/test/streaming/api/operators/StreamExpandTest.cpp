#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include "streaming/api/operators/StreamExpand.h"
#include "test/core/operators/OutputTest.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"

using json = nlohmann::json;

TEST(StreamExpandTest, Construction) {
    json desc = json::object();
    BatchOutputTest output;
    StreamExpand expand(desc, &output);
    SUCCEED();
}

TEST(StreamExpandTest, Open) {
    json desc = json::object();
    BatchOutputTest output;
    StreamExpand expand(desc, &output);
    expand.open();
    SUCCEED();
}

TEST(StreamExpandTest, Close) {
    json desc = json::object();
    BatchOutputTest output;
    StreamExpand expand(desc, &output);
    expand.close();
    SUCCEED();
}

TEST(StreamExpandTest, GetName) {
    json desc = json::object();
    BatchOutputTest output;
    StreamExpand expand(desc, &output);
    auto name = expand.getName();
    EXPECT_NE(name, nullptr);
}

TEST(StreamExpandTest, GetTypeName) {
    json desc = json::object();
    BatchOutputTest output;
    StreamExpand expand(desc, &output);
    auto typeName = expand.getTypeName();
    EXPECT_FALSE(typeName.empty());
    EXPECT_NE(typeName.find("StreamExpand"), std::string::npos);
}

TEST(StreamExpandTest, InitializeState) {
    json desc = json::object();
    BatchOutputTest output;
    StreamExpand expand(desc, &output);
    expand.initializeState(nullptr, nullptr);
    SUCCEED();
}

TEST(StreamExpandTest, ProcessElementThrows) {
    json desc = json::object();
    BatchOutputTest output;
    StreamExpand expand(desc, &output);
    StreamRecord* record = new StreamRecord(nullptr);
    EXPECT_ANY_THROW(expand.processElement(record));
    delete record;
}

TEST(StreamExpandTest, ProcessWatermarkWithOutput) {
    json desc = json::object();
    BatchOutputTest output;
    StreamExpand expand(desc, &output);
    Watermark* wm = new Watermark(5000);
    expand.ProcessWatermark(wm);
    auto* received = output.getWatermark();
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->getTimestamp(), 5000);
}

TEST(StreamExpandTest, ProcessWatermarkStatus) {
    json desc = json::object();
    BatchOutputTest output;
    StreamExpand expand(desc, &output);
    WatermarkStatus status(WatermarkStatus::idleStatus);
    expand.processWatermarkStatus(&status);
    SUCCEED();
}

TEST(StreamExpandTest, ProcessBatchNoProjects) {
    json desc = json::object();
    BatchOutputTest output;
    StreamExpand expand(desc, &output);
    expand.open();

    auto* vb = new omnistream::VectorBatch(1);
    auto* vec = new omniruntime::vec::Vector<int64_t>(1);
    vec->SetValue(0, 42);
    vb->Append(vec);

    StreamRecord* record = new StreamRecord(vb);
    expand.processBatch(record);
    SUCCEED();
}
