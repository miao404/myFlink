#include <gtest/gtest.h>
#include "streaming/api/operators/operatorconstants.h"
#include <string>

TEST(OperatorConstantsTest, MapOperatorName) {
    EXPECT_EQ(std::string(omnistream::datastream::OPERATOR_NAME_MAP),
              "org.apache.flink.streaming.api.operators.StreamMap");
}

TEST(OperatorConstantsTest, ReduceOperatorName) {
    EXPECT_EQ(std::string(omnistream::datastream::OPERATOR_NAME_REDUCE),
              "org.apache.flink.streaming.api.operators.StreamGroupedReduceOperator");
}

TEST(OperatorConstantsTest, FilterOperatorName) {
    EXPECT_EQ(std::string(omnistream::datastream::OPERATOR_NAME_FILTER),
              "org.apache.flink.streaming.api.operators.StreamFilter");
}

TEST(OperatorConstantsTest, AddSourceOperatorName) {
    EXPECT_EQ(std::string(omnistream::datastream::OPERATOR_NAME_ADDSOURCE),
              "org.apache.flink.streaming.api.operators.StreamSource");
}

TEST(OperatorConstantsTest, FromSourceOperatorName) {
    EXPECT_EQ(std::string(omnistream::datastream::OPERATOR_NAME_FROMSOURCE),
              "org.apache.flink.streaming.api.operators.SourceOperator");
}

TEST(OperatorConstantsTest, SinkWriterOperatorName) {
    EXPECT_EQ(std::string(omnistream::datastream::OPERATOR_NAME_SINK_WRITER),
              "org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator");
}

TEST(OperatorConstantsTest, CommitOperatorName) {
    EXPECT_EQ(std::string(omnistream::datastream::OPERATOR_NAME_COMMIT_OPERATOR),
              "org.apache.flink.streaming.runtime.operators.sink.CommitterOperator");
}

TEST(OperatorConstantsTest, FlatMapOperatorName) {
    EXPECT_EQ(std::string(omnistream::datastream::OPERATOR_NAME_FLATMAP),
              "org.apache.flink.streaming.api.operators.StreamFlatMap");
}

TEST(OperatorConstantsTest, GroupAggOperatorName) {
    EXPECT_EQ(std::string(omnistream::datastream::OPERATOR_NAME_GROUP_AGG),
              "org.apache.flink.streaming.api.operators.KeyedProcessOperator");
}
