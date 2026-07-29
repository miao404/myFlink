#include <gtest/gtest.h>
#include "table/runtime/operators/TableOperatorConstants.h"
#include <string>

TEST(TableOperatorConstantsTest, StreamCalcName) {
    EXPECT_EQ(omnistream::OPERATOR_NAME_STREAM_CALC, "StreamExecCalc");
}

TEST(TableOperatorConstantsTest, StreamExpandName) {
    EXPECT_EQ(omnistream::OPERATOR_NAME_STREAM_EXPAND, "StreamExecExpand");
}

TEST(TableOperatorConstantsTest, KeyedProcessOperatorName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_KEYED_PROCESS_OPERATOR),
              "org.apache.flink.streaming.api.operators.KeyedProcessOperator");
}

TEST(TableOperatorConstantsTest, StreamJoinName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_STREAM_JOIN),
              "org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator");
}

TEST(TableOperatorConstantsTest, WatermarkAssignerName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_WATERMARK_ASSIGNER),
              "org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperator");
}

TEST(TableOperatorConstantsTest, SinkName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_SINK),
              "org.apache.flink.table.runtime.operators.sink.SinkOperator");
}

TEST(TableOperatorConstantsTest, StreamSourceName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_STREAM_SOURCE),
              "org.apache.flink.streaming.api.operators.StreamSource");
}

TEST(TableOperatorConstantsTest, ConstraintEnforcerName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_CONSTRAINTENFORCER),
              "org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer");
}

TEST(TableOperatorConstantsTest, FilterName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_FILTER),
              "org.apache.flink.streaming.api.operators.StreamFilter");
}

TEST(TableOperatorConstantsTest, InputConversionName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_INPUT_CONVERSION),
              "org.apache.flink.table.runtime.operators.source.InputConversionOperator");
}

TEST(TableOperatorConstantsTest, SinkWriterName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_SINK_WRITER),
              "org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator");
}

TEST(TableOperatorConstantsTest, CommitOperatorName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_COMMIT_OPERATOR),
              "org.apache.flink.streaming.runtime.operators.sink.CommitterOperator");
}

TEST(TableOperatorConstantsTest, LocalWindowAggName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_LOCAL_WINDOW_AGG),
              "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator");
}

TEST(TableOperatorConstantsTest, GlobalWindowAggName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_GLOBAL_WINDOW_AGG),
              "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator");
}

TEST(TableOperatorConstantsTest, WindowInnerJoinName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_WINDOW_INNER_JOIN),
              "org.apache.flink.table.runtime.operators.join.window.WindowJoinOperator.InnerJoinOperator");
}

TEST(TableOperatorConstantsTest, OutputConversionName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_OUTPUT_CONVERSION),
              "org.apache.flink.table.runtime.operators.sink.OutputConversionOperator");
}

TEST(TableOperatorConstantsTest, KeyedCoProcessName) {
    EXPECT_EQ(std::string(omnistream::OPERATOR_NAME_KEYED_CO_PROCESS),
              "org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator");
}
