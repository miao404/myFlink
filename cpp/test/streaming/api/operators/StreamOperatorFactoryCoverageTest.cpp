/*
 * Comprehensive unit tests for StreamOperatorFactory
 * Tests all CreateXxxOp methods to maximize code coverage.
 */
#include <gtest/gtest.h>
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "table/runtime/operators/TableOperatorConstants.h"
#include "streaming/api/operators/operatorconstants.h"
#include "test/core/operators/test_utils/Mocks.h"

using namespace omnistream;

// ============================================================================
// Helper: create OperatorPOD with specific id, description, operatorType
// ============================================================================
static OperatorPOD makeOpPOD(const std::string& id, const std::string& description,
                              Type_o opType = Type_o::SQL, const std::string& name = "")
{
    return OperatorPOD(name, id, description, {}, {}, "test-operator-id", 0,
                       static_cast<int>(Type_o::INVALID),
                       static_cast<int>(Type_o::INVALID),
                       static_cast<int>(opType));
}

// ============================================================================
// Helper: create OperatorConfig (for the first overload)
// ============================================================================
static OperatorConfig makeOpConfig(const std::string& uniqueName, const nlohmann::json& desc,
                                    const std::string& name = "")
{
    return OperatorConfig(uniqueName, name, {}, {}, desc);
}

// ============================================================================
// Tests for createOperatorAndCollector(OperatorPOD&, ...) — second overload
// ============================================================================

// --- CreateConstraintEnforcerOp (line 496-501) ---
TEST(StreamOperatorFactoryCoverage, CreateConstraintEnforcerOp)
{
    MockOutput out;
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_CONSTRAINTENFORCER), "{}", Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreatePartitionCommitterOp (line 605-611) ---
TEST(StreamOperatorFactoryCoverage, CreatePartitionCommitterOp)
{
    MockOutput out;
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_PARTITION_COMMITTER), "{}", Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateTimestampInserterOp (line 462-471) ---
TEST(StreamOperatorFactoryCoverage, CreateTimestampInserterOp)
{
    MockOutput out;
    nlohmann::json desc;
    desc["timestampIndex"] = 0;
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_STREAMRECORDTIMESTAMPINSERTER),
                         desc.dump(), Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateSinkOp (line 349-356) ---
TEST(StreamOperatorFactoryCoverage, CreateSinkOp)
{
    MockOutput out;
    nlohmann::json desc;
    desc["outputfile"] = "/tmp/test_sink.txt";
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_SINK), desc.dump(), Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateSinkOp with OPERATOR_NAME_COLLECT_SINK ---
TEST(StreamOperatorFactoryCoverage, CreateCollectSinkOp)
{
    MockOutput out;
    nlohmann::json desc;
    desc["outputfile"] = "";
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_COLLECT_SINK), desc.dump(), Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateSinkOp with OPERATOR_NAME_STREAM_SINK ---
TEST(StreamOperatorFactoryCoverage, CreateStreamSinkOp)
{
    MockOutput out;
    nlohmann::json desc;
    desc["outputfile"] = "/tmp/stream_sink.txt";
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_STREAM_SINK), desc.dump(), Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateStreamExpandOp (line 451-459) ---
TEST(StreamOperatorFactoryCoverage, CreateStreamExpandOp)
{
    MockOutput out;
    nlohmann::json desc;
    desc["projects"] = nlohmann::json::array({{{"exprType", "FIELD_REFERENCE"}, {"colVal", 0}, {"dataType", 1}}});
    desc["inputTypes"] = nlohmann::json::array({"INT", "BIGINT"});
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_STREAM_EXPAND), desc.dump(), Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateStreamCalcOp (line 229-238) ---
TEST(StreamOperatorFactoryCoverage, CreateStreamCalcOp)
{
    MockOutput out;
    nlohmann::json desc;
    desc["indices"] = nlohmann::json::array({{{"exprType", "FIELD_REFERENCE"}, {"colVal", 0}, {"dataType", 1}}});
    desc["condition"] = nullptr;
    desc["inputTypes"] = nlohmann::json::array({"INT", "BIGINT"});
    desc["outputTypes"] = nlohmann::json::array({"INT"});
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_STREAM_CALC), desc.dump(), Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateStreamJoinOp (line 241-249) ---
TEST(StreamOperatorFactoryCoverage, CreateStreamJoinOp)
{
    MockOutput out;
    nlohmann::json desc;
    desc["leftJoinKey"] = nlohmann::json::array({0});
    desc["rightJoinKey"] = nlohmann::json::array({0});
    desc["leftInputTypes"] = nlohmann::json::array({"INT", "BIGINT"});
    desc["rightInputTypes"] = nlohmann::json::array({"INT", "BIGINT"});
    desc["outputTypes"] = nlohmann::json::array({"INT", "BIGINT", "INT", "BIGINT"});
    desc["joinType"] = "InnerJoin";
    desc["leftIsOuter"] = false;
    desc["rightIsOuter"] = false;
    desc["stateRetentionTime"] = 0;
    desc["nonEquiCondition"] = nullptr;
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_STREAM_JOIN), desc.dump(), Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateStreamingFileWriterOp (line 585-602) ---
TEST(StreamOperatorFactoryCoverage, CreateStreamingFileWriterOp)
{
    MockOutput out;
    nlohmann::json desc;
    desc["basePath"] = "/tmp/test_output";
    desc["partitionKeys"] = nlohmann::json::array();
    desc["formatType"] = "csv";
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_STREAMING_FILE_WRITER), desc.dump(), Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateWindowInnerJoinOp (line 504-512) ---
TEST(StreamOperatorFactoryCoverage, CreateWindowInnerJoinOp)
{
    MockOutput out;
    nlohmann::json desc;
    desc["originDescription"] = nullptr;
    desc["leftInputTypes"] = nlohmann::json::array({"INT", "BIGINT", "INT"});
    desc["rightInputTypes"] = nlohmann::json::array({"INT", "BIGINT", "INT"});
    desc["outputTypes"] = nlohmann::json::array({"INT", "BIGINT", "INT", "INT", "BIGINT", "INT"});
    desc["leftJoinKey"] = nlohmann::json::array({0});
    desc["rightJoinKey"] = nlohmann::json::array({0});
    desc["leftWindowEndIndex"] = 1;
    desc["rightWindowEndIndex"] = 1;
    desc["nonEquiCondition"] = nullptr;
    desc["joinType"] = "InnerJoin";
    desc["leftWindowing"] = "TUMBLE(size=[10 s])";
    desc["leftTimeAttributeType"] = 2;
    desc["rightWindowing"] = "TUMBLE(size=[10 s])";
    desc["rightTimeAttributeType"] = 2;
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_WINDOW_INNER_JOIN), desc.dump(), Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateProcessOp with STREAM type (line 480-484) ---
TEST(StreamOperatorFactoryCoverage, CreateProcessOpStream)
{
    MockOutput out;
    nlohmann::json desc;
    desc["operatorType"] = "stream";
    desc["udf_so"] = "";
    desc["udfClassName"] = "TestProcess";
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_PROCESS_OPERATOR), desc.dump(), Type_o::STREAM);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateProcessOp with SQL type (line 485-490) ---
TEST(StreamOperatorFactoryCoverage, CreateProcessOpSql)
{
    MockOutput out;
    nlohmann::json desc;
    desc["operatorType"] = "sql";
    desc["tableName"] = "test_table";
    desc["lookupKeys"] = nlohmann::json::array({0});
    desc["joinConditions"] = nlohmann::json::array();
    desc["joinType"] = "InnerJoin";
    desc["asyncLookup"] = false;
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_PROCESS_OPERATOR), desc.dump(), Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateKeyedProcessOp with Deduplicate (name starts with 'D') (line 310-319) ---
TEST(StreamOperatorFactoryCoverage, CreateKeyedProcessOpDeduplicate)
{
    MockOutput out;
    nlohmann::json desc;
    desc["rowtimeFieldIndex"] = 1;
    desc["keepLastRow"] = true;
    desc["generateUpdateBefore"] = false;
    desc["inputTypes"] = nlohmann::json::array({"INT", "BIGINT"});
    desc["stateRetentionTime"] = 0;
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_KEYED_PROCESS_OPERATOR), desc.dump(),
                         Type_o::SQL, "Deduplicate");
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateKeyedProcessOp with AppendOnlyTopNFunction (line 320-328) ---
TEST(StreamOperatorFactoryCoverage, CreateKeyedProcessOpAppendOnlyTopN)
{
    MockOutput out;
    nlohmann::json desc;
    desc["processFunction"] = "AppendOnlyTopNFunction";
    desc["inputTypes"] = nlohmann::json::array({"INT", "BIGINT"});
    desc["sortKeyIndices"] = nlohmann::json::array({1});
    desc["sortOrders"] = nlohmann::json::array({true});
    desc["rankStart"] = 1;
    desc["rankEnd"] = 10;
    desc["outputRankNumber"] = false;
    desc["generateUpdateBefore"] = true;
    desc["stateRetentionTime"] = 0;
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_KEYED_PROCESS_OPERATOR), desc.dump(),
                         Type_o::SQL, "AppendOnlyTopN");
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateKeyedProcessOp with FastTop1Function (line 329-336) ---
TEST(StreamOperatorFactoryCoverage, CreateKeyedProcessOpFastTop1)
{
    MockOutput out;
    nlohmann::json desc;
    desc["processFunction"] = "FastTop1Function";
    desc["inputTypes"] = nlohmann::json::array({"INT", "BIGINT"});
    desc["sortKeyIndices"] = nlohmann::json::array({1});
    desc["sortOrders"] = nlohmann::json::array({true});
    desc["rankStart"] = 1;
    desc["rankEnd"] = 1;
    desc["outputRankNumber"] = false;
    desc["generateUpdateBefore"] = true;
    desc["stateRetentionTime"] = 0;
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_KEYED_PROCESS_OPERATOR), desc.dump(),
                         Type_o::SQL, "FastTop1");
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateKeyedProcessOp with GroupAggFunction (default else) (line 337-344) ---
TEST(StreamOperatorFactoryCoverage, CreateKeyedProcessOpGroupAgg)
{
    MockOutput out;
    nlohmann::json desc;
    desc["inputTypes"] = nlohmann::json::array({"INT", "BIGINT"});
    desc["aggCalls"] = nlohmann::json::array({{{"aggFunction", "SUM"}, {"argIndices", {1}}, {"filterArg", -1}}});
    desc["grouping"] = nlohmann::json::array({0});
    desc["generateUpdateBefore"] = true;
    desc["stateRetentionTime"] = 0;
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_KEYED_PROCESS_OPERATOR), desc.dump(),
                         Type_o::SQL, "GroupAgg");
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateLocalWindowAggOp (line 252-260) ---
TEST(StreamOperatorFactoryCoverage, CreateLocalWindowAggOp)
{
    MockOutput out;
    nlohmann::json desc;
    desc["inputTypes"] = nlohmann::json::array({"INT", "BIGINT", "INT"});
    desc["aggCalls"] = nlohmann::json::array({{{"aggFunction", "SUM"}, {"argIndices", {2}}, {"filterArg", -1}}});
    desc["grouping"] = nlohmann::json::array({0});
    desc["windowType"] = "TUMBLE";
    desc["windowSize"] = 10000;
    desc["windowSlide"] = 10000;
    desc["rowtimeFieldIndex"] = 1;
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_LOCAL_WINDOW_AGG), desc.dump(), Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateMapOp with STREAM type (line 515-528) ---
TEST(StreamOperatorFactoryCoverage, CreateMapOpStream)
{
    MockOutput out;
    nlohmann::json desc;
    desc["udf_so"] = "";
    desc["udfClassName"] = "TestMapFunction";
    auto pod = makeOpPOD(std::string(datastream::OPERATOR_NAME_MAP), desc.dump(), Type_o::STREAM);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateFilterOp with STREAM type (line 531-545) ---
TEST(StreamOperatorFactoryCoverage, CreateFilterOpStream)
{
    MockOutput out;
    nlohmann::json desc;
    desc["udf_so"] = "";
    desc["udfClassName"] = "TestFilterFunction";
    auto pod = makeOpPOD(std::string(datastream::OPERATOR_NAME_FILTER), desc.dump(), Type_o::STREAM);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateFlatMapOp with STREAM type (line 548-562) ---
TEST(StreamOperatorFactoryCoverage, CreateFlatMapOpStream)
{
    MockOutput out;
    nlohmann::json desc;
    desc["udf_so"] = "";
    desc["udfClassName"] = "TestFlatMapFunction";
    auto pod = makeOpPOD(std::string(datastream::OPERATOR_NAME_FLATMAP), desc.dump(), Type_o::STREAM);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateReduceOp with STREAM type (line 614-669) ---
TEST(StreamOperatorFactoryCoverage, CreateReduceOpStream)
{
    MockOutput out;
    nlohmann::json desc;
    desc["udf_so"] = "";
    desc["udfClassName"] = "TestReduceFunction";
    auto pod = makeOpPOD(std::string(datastream::OPERATOR_NAME_REDUCE), desc.dump(), Type_o::STREAM);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateReduceOp with STREAM type and "basic" input kind (line 630-631) ---
TEST(StreamOperatorFactoryCoverage, CreateReduceOpStreamBasicInput)
{
    MockOutput out;
    nlohmann::json desc;
    desc["udf_so"] = "";
    desc["udfClassName"] = "TestReduceFunction";
    TypeDescriptionPOD input;
    input.kind = "basic";
    input.type = "String";
    std::vector<TypeDescriptionPOD> inputs = {input};
    auto pod = OperatorPOD("", std::string(datastream::OPERATOR_NAME_REDUCE), desc.dump(),
                           inputs, {}, "test-op-id", 0,
                           static_cast<int>(Type_o::INVALID),
                           static_cast<int>(Type_o::INVALID),
                           static_cast<int>(Type_o::STREAM));
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateReduceOp with STREAM type and "Tuple" input kind (line 632-634) ---
TEST(StreamOperatorFactoryCoverage, CreateReduceOpStreamTupleInput)
{
    MockOutput out;
    nlohmann::json desc;
    desc["udf_so"] = "";
    desc["udfClassName"] = "TestReduceFunction";
    nlohmann::json tupleType = nlohmann::json::array({"String", "Integer"});
    TypeDescriptionPOD input;
    input.kind = "Tuple";
    input.type = tupleType.dump();
    std::vector<TypeDescriptionPOD> inputs = {input};
    auto pod = OperatorPOD("", std::string(datastream::OPERATOR_NAME_REDUCE), desc.dump(),
                           inputs, {}, "test-op-id", 0,
                           static_cast<int>(Type_o::INVALID),
                           static_cast<int>(Type_o::INVALID),
                           static_cast<int>(Type_o::STREAM));
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateReduceOp with unsupported input kind (line 636-637) ---
TEST(StreamOperatorFactoryCoverage, CreateReduceOpStreamUnsupportedInput)
{
    MockOutput out;
    nlohmann::json desc;
    desc["udf_so"] = "";
    desc["udfClassName"] = "TestReduceFunction";
    TypeDescriptionPOD input;
    input.kind = "unsupported_kind";
    input.type = "SomeType";
    std::vector<TypeDescriptionPOD> inputs = {input};
    auto pod = OperatorPOD("", std::string(datastream::OPERATOR_NAME_REDUCE), desc.dump(),
                           inputs, {}, "test-op-id", 0,
                           static_cast<int>(Type_o::INVALID),
                           static_cast<int>(Type_o::INVALID),
                           static_cast<int>(Type_o::STREAM));
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- CreateKeyedCoProcessOp with STREAM type (line 565-582) ---
TEST(StreamOperatorFactoryCoverage, CreateKeyedCoProcessOpStream)
{
    MockOutput out;
    nlohmann::json desc;
    desc["udf_so"] = "";
    desc["udfClassName"] = "TestCoProcessFunction";
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_KEYED_CO_PROCESS), desc.dump(), Type_o::STREAM);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- Unknown operator returns nullptr (line 224-226) ---
TEST(StreamOperatorFactoryCoverage, UnknownOperatorPODReturnsNullptr)
{
    MockOutput out;
    auto pod = makeOpPOD("com.unknown.operator", "{}", Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    EXPECT_EQ(op, nullptr);
}

// ============================================================================
// Tests for createOperatorAndCollector(OperatorConfig&, ...) — first overload
// ============================================================================

// --- StreamExpand via OperatorConfig (line 70-74) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigStreamExpand)
{
    MockOutput out;
    nlohmann::json desc;
    desc["projects"] = nlohmann::json::array({{{"exprType", "FIELD_REFERENCE"}, {"colVal", 0}, {"dataType", 1}}});
    auto config = makeOpConfig(std::string(OPERATOR_NAME_STREAM_EXPAND), desc);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- StreamCalcBatch via OperatorConfig (line 75-79) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigStreamCalc)
{
    MockOutput out;
    nlohmann::json desc;
    desc["indices"] = nlohmann::json::array({{{"exprType", "FIELD_REFERENCE"}, {"colVal", 0}, {"dataType", 1}}});
    desc["condition"] = nullptr;
    desc["inputTypes"] = nlohmann::json::array({"INT"});
    desc["outputTypes"] = nlohmann::json::array({"INT"});
    auto config = makeOpConfig(std::string(OPERATOR_NAME_STREAM_CALC), desc);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- StreamingJoinOperator via OperatorConfig (line 80-85) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigStreamJoin)
{
    MockOutput out;
    nlohmann::json desc;
    desc["leftJoinKey"] = nlohmann::json::array({0});
    desc["rightJoinKey"] = nlohmann::json::array({0});
    desc["leftInputTypes"] = nlohmann::json::array({"INT", "BIGINT"});
    desc["rightInputTypes"] = nlohmann::json::array({"INT", "BIGINT"});
    desc["outputTypes"] = nlohmann::json::array({"INT", "BIGINT", "INT", "BIGINT"});
    desc["joinType"] = "InnerJoin";
    desc["leftIsOuter"] = false;
    desc["rightIsOuter"] = false;
    desc["stateRetentionTime"] = 0;
    desc["nonEquiCondition"] = nullptr;
    auto config = makeOpConfig(std::string(OPERATOR_NAME_STREAM_JOIN), desc);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- WatermarkAssignerOperator via OperatorConfig (line 86-93) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigWatermarkAssigner)
{
    MockOutput out;
    nlohmann::json desc;
    desc["rowtimeFieldIndex"] = 1;
    desc["idleTimeout"] = 0;
    auto config = makeOpConfig(std::string(OPERATOR_NAME_WATERMARK_ASSIGNER), desc);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- KeyedProcessOperator with Deduplicate via OperatorConfig (line 94-101) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigKeyedProcessDeduplicate)
{
    MockOutput out;
    nlohmann::json desc;
    desc["rowtimeFieldIndex"] = 1;
    desc["keepLastRow"] = true;
    desc["generateUpdateBefore"] = false;
    desc["inputTypes"] = nlohmann::json::array({"INT", "BIGINT"});
    desc["stateRetentionTime"] = 0;
    auto config = makeOpConfig(std::string(OPERATOR_NAME_KEYED_PROCESS_OPERATOR), desc, "Deduplicate");
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- KeyedProcessOperator with GroupAgg via OperatorConfig (line 102-108) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigKeyedProcessGroupAgg)
{
    MockOutput out;
    nlohmann::json desc;
    desc["inputTypes"] = nlohmann::json::array({"INT", "BIGINT"});
    desc["aggCalls"] = nlohmann::json::array({{{"aggFunction", "SUM"}, {"argIndices", {1}}, {"filterArg", -1}}});
    desc["grouping"] = nlohmann::json::array({0});
    desc["generateUpdateBefore"] = true;
    desc["stateRetentionTime"] = 0;
    auto config = makeOpConfig(std::string(OPERATOR_NAME_KEYED_PROCESS_OPERATOR), desc, "GroupAgg");
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- Sink via OperatorConfig with non-empty name (line 110-119) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigSinkWithName)
{
    MockOutput out;
    nlohmann::json desc = nlohmann::json::object();
    auto config = makeOpConfig(std::string(OPERATOR_NAME_SINK), desc, "testSink");
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- Sink via OperatorConfig with empty name (line 115-116) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigSinkEmptyName)
{
    MockOutput out;
    nlohmann::json desc = nlohmann::json::object();
    auto config = makeOpConfig(std::string(OPERATOR_NAME_SINK), desc, "");
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- Sink via OperatorConfig with COLLECT_SINK (line 110-111) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigCollectSink)
{
    MockOutput out;
    nlohmann::json desc = nlohmann::json::object();
    auto config = makeOpConfig(std::string(OPERATOR_NAME_COLLECT_SINK), desc, "collectSink");
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- ProcessOperator with stream type via OperatorConfig (line 120-130) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigProcessOperatorStream)
{
    MockOutput out;
    nlohmann::json desc;
    desc["operatorType"] = "stream";
    desc["udf_so"] = "";
    desc["udfClassName"] = "TestProcess";
    auto config = makeOpConfig(std::string(OPERATOR_NAME_PROCESS_OPERATOR), desc);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- ProcessOperator with sql type via OperatorConfig (line 131-137) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigProcessOperatorSql)
{
    MockOutput out;
    nlohmann::json desc;
    desc["operatorType"] = "sql";
    desc["tableName"] = "test_table";
    desc["lookupKeys"] = nlohmann::json::array({0});
    desc["joinConditions"] = nlohmann::json::array();
    desc["joinType"] = "InnerJoin";
    desc["asyncLookup"] = false;
    auto config = makeOpConfig(std::string(OPERATOR_NAME_PROCESS_OPERATOR), desc);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- LocalSlicingWindowAggOperator via OperatorConfig (line 139-143) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigLocalWindowAgg)
{
    MockOutput out;
    nlohmann::json desc;
    desc["inputTypes"] = nlohmann::json::array({"INT", "BIGINT", "INT"});
    desc["aggCalls"] = nlohmann::json::array({{{"aggFunction", "SUM"}, {"argIndices", {2}}, {"filterArg", -1}}});
    desc["grouping"] = nlohmann::json::array({0});
    desc["windowType"] = "TUMBLE";
    desc["windowSize"] = 10000;
    desc["windowSlide"] = 10000;
    desc["rowtimeFieldIndex"] = 1;
    auto config = makeOpConfig(std::string(OPERATOR_NAME_LOCAL_WINDOW_AGG), desc);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- SlicingWindowOperator (global) via OperatorConfig (line 144-149) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigGlobalWindowAgg)
{
    MockOutput out;
    nlohmann::json desc;
    desc["inputTypes"] = nlohmann::json::array({"INT", "BIGINT", "INT"});
    desc["aggCalls"] = nlohmann::json::array({{{"aggFunction", "SUM"}, {"argIndices", {2}}, {"filterArg", -1}}});
    desc["grouping"] = nlohmann::json::array({0});
    desc["windowType"] = "TUMBLE";
    desc["windowSize"] = 10000;
    desc["windowSlide"] = 10000;
    desc["rowtimeFieldIndex"] = 1;
    auto config = makeOpConfig(std::string(OPERATOR_NAME_GLOBAL_WINDOW_AGG), desc);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- AggregateWindowOperator (group window agg) via OperatorConfig (line 150-154) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigGroupWindowAgg)
{
    MockOutput out;
    nlohmann::json desc;
    desc["inputTypes"] = nlohmann::json::array({"INT", "BIGINT", "INT"});
    desc["aggCalls"] = nlohmann::json::array({{{"aggFunction", "SUM"}, {"argIndices", {2}}, {"filterArg", -1}}});
    desc["grouping"] = nlohmann::json::array({0});
    desc["windowType"] = "TUMBLE";
    desc["windowSize"] = 10000;
    desc["windowSlide"] = 10000;
    desc["rowtimeFieldIndex"] = 1;
    auto config = makeOpConfig(std::string(OPERATOR_NAME_GROUP_WINDOW_AGG), desc);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- WindowJoinOperator via OperatorConfig (line 155-159) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigWindowInnerJoin)
{
    MockOutput out;
    nlohmann::json desc;
    desc["originDescription"] = nullptr;
    desc["leftInputTypes"] = nlohmann::json::array({"INT", "BIGINT", "INT"});
    desc["rightInputTypes"] = nlohmann::json::array({"INT", "BIGINT", "INT"});
    desc["outputTypes"] = nlohmann::json::array({"INT", "BIGINT", "INT", "INT", "BIGINT", "INT"});
    desc["leftJoinKey"] = nlohmann::json::array({0});
    desc["rightJoinKey"] = nlohmann::json::array({0});
    desc["leftWindowEndIndex"] = 1;
    desc["rightWindowEndIndex"] = 1;
    desc["nonEquiCondition"] = nullptr;
    desc["joinType"] = "InnerJoin";
    desc["leftWindowing"] = "TUMBLE(size=[10 s])";
    desc["leftTimeAttributeType"] = 2;
    desc["rightWindowing"] = "TUMBLE(size=[10 s])";
    desc["rightTimeAttributeType"] = 2;
    auto config = makeOpConfig(std::string(OPERATOR_NAME_WINDOW_INNER_JOIN), desc);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(config, &out);
    ASSERT_NE(op, nullptr);
    delete op;
}

// --- Unknown operator via OperatorConfig throws (line 160-162) ---
TEST(StreamOperatorFactoryCoverage, OperatorConfigUnknownThrows)
{
    MockOutput out;
    nlohmann::json desc = nlohmann::json::object();
    auto config = makeOpConfig("UnknownOperator", desc);
    EXPECT_THROW(StreamOperatorFactory::createOperatorAndCollector(config, &out), std::logic_error);
}

// ============================================================================
// Code paths that MAY NOT be fully coverable:
//
// 1. CreateSourceOp (all branches):
//    - kafka format: Requires KafkaSource connecting to real Kafka broker
//    - csv format: Requires valid file path, CsvInputFormat schema
//    - nexmark format: Requires NexmarkConfiguration with valid batchSize
//    - joinSource format: Requires JoinSource with valid config
//    All branches need a valid OmniStreamTask (task->createProcessingTimeService())
//
// 2. CreateWatermarkAssignerOp (OperatorPOD overload, line 289-300):
//    Requires task->createProcessingTimeService() — needs real OmniStreamTask
//
// 3. CreateGlobalWindowAggOp (OperatorPOD overload, line 263-274):
//    Requires task->createProcessingTimeService()
//
// 4. CreateGroupWindowAggOp (OperatorPOD overload, line 277-286):
//    Requires task->createProcessingTimeService()
//
// 5. CreateSinkWriterOp (line 672-725):
//    Requires Kafka producer config, RdKafka::Conf, and ProcessingTimeService
//
// 6. CreateCommitOp (line 728-741):
//    Requires task->createProcessingTimeService()
//
// 7. UDF-dependent operators (Map, Filter, FlatMap, Reduce):
//    If the udf_so path is empty or invalid, the operator still constructs
//    but open() would fail when trying to dlopen. The factory call itself
//    should still succeed and cover the factory dispatch code.
// ============================================================================
