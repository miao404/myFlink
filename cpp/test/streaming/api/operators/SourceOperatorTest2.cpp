/*
 * Unit tests for SourceOperator and OperatingMode enum
 */
#include <gtest/gtest.h>
#include "streaming/api/operators/SourceOperator.h"

// ===========================================================================
// OperatingMode enum tests
// ===========================================================================

TEST(OperatingModeTest, EnumValues) {
    EXPECT_EQ(static_cast<int>(OperatingMode::READING), 0);
    EXPECT_EQ(static_cast<int>(OperatingMode::WAITING_FOR_ALIGNMENT), 1);
    EXPECT_EQ(static_cast<int>(OperatingMode::OUTPUT_NOT_INITIALIZED), 2);
    EXPECT_EQ(static_cast<int>(OperatingMode::SOURCE_DRAINED), 3);
    EXPECT_EQ(static_cast<int>(OperatingMode::SOURCE_STOPPED), 4);
    EXPECT_EQ(static_cast<int>(OperatingMode::DATA_FINISHED), 5);
}

TEST(OperatingModeTest, AllEnumValuesDistinct) {
    std::set<int> values;
    values.insert(static_cast<int>(OperatingMode::READING));
    values.insert(static_cast<int>(OperatingMode::WAITING_FOR_ALIGNMENT));
    values.insert(static_cast<int>(OperatingMode::OUTPUT_NOT_INITIALIZED));
    values.insert(static_cast<int>(OperatingMode::SOURCE_DRAINED));
    values.insert(static_cast<int>(OperatingMode::SOURCE_STOPPED));
    values.insert(static_cast<int>(OperatingMode::DATA_FINISHED));
    EXPECT_EQ(values.size(), 6u);
}

/*
 * SPLITS_STATE_DESC:
 *    Not available in build environment (static member not present).
 *
 * Interfaces NOT tested and reasons:
 *
 * 1. SourceOperator constructor:
 *    Requires a WatermarkGaugeExposingOutput, a valid JSON config with
 *    specific kafka/source fields, a KafkaSource, and a ProcessingTimeService.
 *    Creating a KafkaSource needs Kafka broker connection parameters.
 *    The existing test (DISABLED_filterTest) also requires a running Kafka.
 *
 * 2. open():
 *    Calls initReader() which creates a SourceReader via readerFactory.
 *    The readerFactory is set by the constructor from KafkaSource::createReader
 *    which requires a real Kafka connection.
 *
 * 3. emitNext() / emitNextNotReading():
 *    Requires sourceReader to be initialized (via open()), which needs Kafka.
 *
 * 4. GetAvailableFuture():
 *    Depends on operatingMode and sourceReader. Cannot test without
 *    a fully initialized operator.
 *
 * 5. snapshotState() / initializeState():
 *    Requires full state backend initialization through
 *    StreamTaskStateInitializerImpl with a valid EnvironmentV2.
 *
 * 6. handleOperatorEvent() (all overloads):
 *    - String overload: parses JSON and dispatches. Testable in isolation
 *      but AddSplitEvent requires splitSerializer from KafkaSource.
 *    - WatermarkAlignmentEvent: updates internal state, testable but needs
 *      operator to be constructed first.
 *    - AddSplitEvent: requires sourceReader + splitSerializer.
 *    - NoMoreSplitsEvent: requires sourceReader.
 *
 * 7. finish() / close():
 *    finish() calls stopInternalServices() and completes a future.
 *    close() calls sourceReader->close(). Both need an initialized operator.
 *
 * 8. getDataStreamOutput() / getSourceReader() / getReaderState():
 *    Simple getters, but require the operator to be constructed.
 *
 * 9. canBeStreamOperator():
 *    Returns isDataStream which is set in constructor from JSON config.
 *
 * 10. notifyCheckpointComplete / notifyCheckpointAborted:
 *     Delegates to parent + sourceReader. Requires full state + reader init.
 *
 * 11. UpdateIdle / UpdateCurrentEffectiveWatermark:
 *     Simple setters but require constructed operator instance.
 *
 * 12. hexStringToByteArray / hexCharToInt:
 *     Private static methods. Would be testable if public but they are
 *     implementation details of handleOperatorEvent JSON dispatch.
 *
 * Summary: SourceOperator requires Kafka infrastructure and a full runtime
 * environment (EnvironmentV2, TaskConfiguration, ProcessingTimeService) to
 * construct. Its existing test is also DISABLED because it needs a running
 * Kafka broker. The OperatingMode enum and the static SPLITS_STATE_DESC
 * are the only parts testable without external dependencies.
 */
