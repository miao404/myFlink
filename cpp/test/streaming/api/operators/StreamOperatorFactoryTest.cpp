/*
 * Unit tests for StreamOperatorFactory
 */
#include <gtest/gtest.h>
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "table/runtime/operators/TableOperatorConstants.h"
#include "streaming/api/operators/operatorconstants.h"
#include "test/core/operators/test_utils/Mocks.h"

using namespace omnistream;

// Helper to create an OperatorPOD with given id/description/type
static OperatorPOD makeOpPOD(const std::string& id, const std::string& description,
                              Type_o opType = Type_o::SQL)
{
    OperatorPOD pod;
    pod.setId(id);
    pod.setName("");
    pod.setDescription(description);
    pod.setOperatorId("");
    // Set operator type through the full constructor workaround
    return OperatorPOD("", id, description, {}, {}, "", 0,
                       static_cast<int>(Type_o::INVALID),
                       static_cast<int>(Type_o::INVALID),
                       static_cast<int>(opType));
}

// ---------- createOperatorAndCollector (OperatorPOD overload) - unknown operator ----------

TEST(StreamOperatorFactoryTest, UnknownOperatorReturnsNullptr) {
    MockOutput out;
    auto pod = makeOpPOD("UnknownOperatorName", "{}", Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    EXPECT_EQ(op, nullptr);
}

// ---------- CreateConstraintEnforcerOp ----------

TEST(StreamOperatorFactoryTest, CreateConstraintEnforcer) {
    MockOutput out;
    auto pod = makeOpPOD(std::string(OPERATOR_NAME_CONSTRAINTENFORCER), "{}", Type_o::SQL);
    auto* op = StreamOperatorFactory::createOperatorAndCollector(pod, &out, nullptr);
    ASSERT_NE(op, nullptr);
    delete op;
}

// ---------- createOperatorAndCollector (OperatorConfig overload) - unknown operator ----------

TEST(StreamOperatorFactoryTest, OperatorConfigUnknownThrows) {
    MockOutput out;
    nlohmann::json desc = {};
    OperatorConfig config("UnknownOp", "test", {}, {}, desc);
    EXPECT_THROW(
        StreamOperatorFactory::createOperatorAndCollector(config, &out),
        std::logic_error);
}

/*
 * Interfaces tested:
 * - createOperatorAndCollector(OperatorPOD&, ...) with unknown ID → nullptr
 * - createOperatorAndCollector(OperatorConfig&, ...) with unknown name → throws
 * - CreateConstraintEnforcerOp via the OperatorPOD dispatch (no external deps)
 *
 * Interfaces NOT tested and reasons:
 *
 * 1. CreateStreamCalcOp:
 *    StreamCalcBatch constructor parses a complex JSON description containing
 *    projection/condition schemas and omniruntime type info. Requires valid
 *    omniruntime type IDs and column definitions.
 *
 * 2. CreateStreamJoinOp:
 *    StreamingJoinOperator needs a detailed join description JSON with
 *    left/right key indices, join type, state TTL configuration, etc.
 *
 * 3. CreateLocalWindowAggOp / CreateGlobalWindowAggOp / CreateGroupWindowAggOp:
 *    Window operators need window definitions (size, slide, offset),
 *    aggregation function configs, and a ProcessingTimeService from
 *    OmniStreamTask. Cannot create without a running task.
 *
 * 4. CreateWatermarkAssignerOp:
 *    Requires JSON with "rowtimeFieldIndex", "intervalSecond" and a
 *    ProcessingTimeService from a valid OmniStreamTask.
 *
 * 5. CreateKeyedProcessOp:
 *    Complex dispatch based on operator name prefix ('D' for deduplicate,
 *    'AppendOnlyTopNFunction', 'FastTop1Function', or GroupAggFunction).
 *    Each requires specific JSON config and OmniStreamTask setup().
 *
 * 6. CreateSinkOp:
 *    Creates SinkOperator with outputfile from JSON/env. Simple but
 *    SinkOperator may have file I/O dependencies.
 *
 * 7. CreateSourceOp:
 *    Dispatches on format ("kafka", "csv", "nexmark", "joinSource").
 *    - kafka: needs KafkaSource + ProcessingTimeService + broker config
 *    - csv: needs CsvInputFormat + file path
 *    - nexmark: needs NexmarkConfiguration
 *    All require OmniStreamTask for setup().
 *
 * 8. CreateStreamExpandOp:
 *    StreamExpand constructor parses expand description JSON.
 *    Requires valid expand projection config.
 *
 * 9. CreateTimestampInserterOp:
 *    Creates TimeStampInserterSinkOperator. Needs description JSON
 *    and output configuration.
 *
 * 10. CreateProcessOp:
 *     Dispatches on operatorType (STREAM/SQL). Stream creates
 *     ProcessOperator, SQL creates LookupJoinRunner. Both need
 *     complex JSON configs and OmniStreamTask.
 *
 * 11. CreateWindowInnerJoinOp:
 *     InnerJoinOperator needs join window definition, key/value
 *     serializers, and valid JSON config.
 *
 * 12. CreateMapOp:
 *     StreamMap requires UDF loading from .so file (udf_so in JSON).
 *
 * 13. CreateFilterOp:
 *     StreamFilter requires UDF loading from .so file.
 *
 * 14. CreateFlatMapOp:
 *     StreamFlatMap requires UDF loading from .so file.
 *
 * 15. CreateReduceOp:
 *     StreamGroupedReduceOperator requires UDF loading (.so) and
 *     OmniStreamTask for setup(). Also needs TypeSerializer building
 *     from input type info.
 *
 * 16. CreateSinkWriterOp:
 *     Requires Kafka producer config, delivery guarantee, topic, and
 *     ProcessingTimeService.
 *
 * 17. CreateCommitOp:
 *     Creates CommitterOperator with processing time service from task.
 *
 * 18. CreateStreamingFileWriterOp:
 *     Creates StreamingFileWriter with BulkFormatBuilder from JSON config.
 *
 * 19. CreatePartitionCommitterOp:
 *     Creates PartitionCommitter. Simple but may have dependencies.
 *
 * 20. CreateKeyedCoProcessOp:
 *     KeyedCoProcessOperator requires UDF loading and OmniStreamTask.
 *
 * 21. CreateInputConversionOperator:
 *     Input conversion operator. Requires specific input/output type config.
 *
 * 22. CreateBatchFilterOp:
 *     Batch filter requires UDF loading.
 *
 * ROOT CAUSE for most untestable methods: The factory methods create
 * operators that require either:
 * (a) UDF shared library loading (StreamMap, StreamFilter, StreamFlatMap,
 *     StreamGroupedReduceOperator)
 * (b) External service connections (Kafka, file system)
 * (c) OmniStreamTask with full runtime environment for setup()
 * (d) Complex JSON configurations with omniruntime type definitions
 *
 * CreateConstraintEnforcerOp is the only method that requires no
 * JSON config, no UDF, no external services, and no OmniStreamTask,
 * making it the only fully testable factory method.
 */
