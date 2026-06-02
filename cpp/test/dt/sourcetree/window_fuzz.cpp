#include "fuzz_wrapper.h"
#include "table/runtime/operators/window/AggregateWindowOperator.h"
#include "table/runtime/operators/window/assigners/SessionWindowAssigner.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "table/runtime/operators/window/internal/MergingWindowProcessFunction.h"
#include "table/runtime/generated/function/TimeWindowCountWindowAggFunction.h"
#include "table/data/binary/BinaryRowData.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "test/core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/TaskStateManager.h"
#include "core/api/common/TaskInfoImpl.h"
#include "core/graph/OperatorConfig.h"
#include "test/util/test_util.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

// NexmarkQ12 JSON description - reference from AggregateWindowOperatorTest
static const std::string WINDOW_Q12_DESC = R"DELIM({
    "partition": {
        "partitionName": "none",
        "channelNumber": 1
    },
    "operators": [{
        "output": {
            "kind": "Row",
            "type": [{"kind": "logical", "isNull": true, "type": "BIGINT"},
                     {"kind": "logical", "isNull": true, "type": "BIGINT"}]
        },
        "inputs": [{
            "kind": "Row",
            "type": [{"kind": "logical", "isNull": true, "type": "BIGINT"},
                     {"kind": "logical", "isNull": true, "precision": 3, "type": "TIMESTAMP", "timestampKind": 1}]
        }],
        "name": "GroupWindowAggregate(groupBy=[bidder], window=[SessionGroupWindow('w$, dateTime, 10000)], select=[bidder, COUNT(*) AS bid_count])",
        "description": {
            "originDescription": null,
            "inputTypes": ["BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"],
            "outputTypes": ["BIGINT", "BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITH_LOCAL_TIME_ZONE"],
            "windowPropertyTypes": ["TIMESTAMP(3) NOT NULL", "TIMESTAMP(3) NOT NULL", "TIMESTAMP(3) *ROWTIME*", "TIMESTAMP_LTZ(3) *PROCTIME*"],
            "grouping": [0],
            "aggInfoList": {
                "aggregateCalls": [{
                    "name": "COUNT()",
                    "aggregationFunction": "Count1AggFunction",
                    "argIndexes": [],
                    "consumeRetraction": "false",
                    "filterArg": -1
                }],
                "AccTypes": ["BIGINT"],
                "aggValueTypes": ["BIGINT"],
                "indexOfCountStar": -1
            },
            "generateUpdateBefore": false,
            "allowedLateness": 0,
            "windowType": "SessionGroupWindow('w$, dateTime, 10000)",
            "countType": "time",
            "timeType": "event",
            "actualSize": 10000,
            "inputTimeFieldIndex": 1
        },
        "id": "org.apache.flink.table.runtime.operators.window.AggregateWindowOperator"
    }]
})DELIM";

// Helper: create AggregateWindowOperator from Q12 JSON - reference from AggregateWindowOperatorTest
static AggregateWindowOperator<RowData *, TimeWindow>* createWindowOperator(BatchOutputTest* output)
{
    std::string uniqueName = "org.apache.flink.table.runtime.operators.window.AggregateWindowOperator";
    json parsedJson = json::parse(WINDOW_Q12_DESC);
    OperatorConfig opConfig(
        uniqueName,
        "LocalWindowAgg_By_Simple",
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]
    );
    auto *windowAggOperator = dynamic_cast<AggregateWindowOperator<RowData *, TimeWindow> *>(
        StreamOperatorFactory::createOperatorAndCollector(opConfig, output));
    return windowAggOperator;
}

// Helper: initialize state for window operator - reference from AggregateWindowOperatorTest
static void initWindowOperatorState(AggregateWindowOperator<RowData *, TimeWindow>* op)
{
    auto env2 = new RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    op->initializeState(initializer, new LongSerializer());
    op->open();
}

// Helper: create VectorBatch with bidder + timestamp columns - reference from AggregateWindowOperatorTest::Q12VectorBatchInput
static VectorBatch* createWindowVectorBatch(int rowCount, const std::vector<int64_t>& bidders,
                                             const std::vector<int64_t>& timestamps)
{
    auto *vbatch = new VectorBatch(rowCount);
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCount, bidders.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCount, timestamps.data()));
    for (int i = 0; i < rowCount; i++) {
        vbatch->setRowKind(i, RowKind::INSERT);
    }
    return vbatch;
}

// Test 1: Window state operations - reference from AggregateWindowOperatorTest::WindowStateTest
// Covers: setCurrentKey, setCurrentNamespace, update, value on windowState
void TestWindowState(const WindowFuzzData& fzd)
{
    std::cout << "TestWindowState" << std::endl;

    auto *output = new BatchOutputTest();
    auto *windowAggOperator = createWindowOperator(output);
    initWindowOperatorState(windowAggOperator);

    // Set key and namespace, then update and read state
    BinaryRowData *bidIdRow = BinaryRowData::createBinaryRowDataWithMem(1);
    bidIdRow->setLong(0, fzd.bidderValue);
    TimeWindow timeWindow(fzd.timestampValue, fzd.timestampValue + fzd.windowSize);
    windowAggOperator->setCurrentKey(bidIdRow);
    windowAggOperator->windowState->setCurrentNamespace(timeWindow);

    // Update state with stateValue
    BinaryRowData *countRow = BinaryRowData::createBinaryRowDataWithMem(1);
    countRow->setLong(0, fzd.stateValue > 0 ? fzd.stateValue : 1);
    windowAggOperator->windowState->update(countRow);
    RowData *windowResult = windowAggOperator->windowState->value();
    std::cout << "  state arity=" << windowResult->getArity()
              << ", value=" << *windowResult->getLong(0) << std::endl;

    // Update with new value on same key+window
    BinaryRowData *countRow2 = BinaryRowData::createBinaryRowDataWithMem(1);
    countRow2->setLong(0, (fzd.stateValue > 0 ? fzd.stateValue : 1) + 1);
    windowAggOperator->windowState->update(countRow2);
    windowResult = windowAggOperator->windowState->value();
    std::cout << "  updated value=" << *windowResult->getLong(0) << std::endl;

    // Switch key, verify state is null for new key
    BinaryRowData *newKeyRow = BinaryRowData::createBinaryRowDataWithMem(1);
    newKeyRow->setLong(0, fzd.bidderValue + 10);
    windowAggOperator->setCurrentKey(newKeyRow);
    windowResult = windowAggOperator->windowState->value();
    std::cout << "  new key state is null=" << (windowResult == nullptr) << std::endl;

    // Update state for new key
    BinaryRowData *newCountRow = BinaryRowData::createBinaryRowDataWithMem(1);
    newCountRow->setLong(0, fzd.stateValue > 0 ? fzd.stateValue : 2);
    windowAggOperator->windowState->update(newCountRow);

    // Switch to a different time window on same key
    TimeWindow timeWindow2(fzd.timestampValue + fzd.windowSize, fzd.timestampValue + 2 * fzd.windowSize);
    windowAggOperator->windowState->setCurrentNamespace(timeWindow2);
    windowResult = windowAggOperator->windowState->value();
    std::cout << "  new window state is null=" << (windowResult == nullptr) << std::endl;

    delete output;
}

// Test 2: MergingWindowProcessFunction AssignStateNamespace - reference from AggregateWindowOperatorTest::TimeWindowTest
// Covers: MergingWindowProcessFunction.AssignStateNamespace, session window merging
void TestWindowTimeAssign(const WindowFuzzData& fzd)
{
    std::cout << "TestWindowTimeAssign" << std::endl;

    auto *output = new BatchOutputTest();
    auto *windowAggOperator = createWindowOperator(output);
    initWindowOperatorState(windowAggOperator);

    // Set current key
    BinaryRowData *key = BinaryRowData::createBinaryRowDataWithMem(1);
    key->setLong(0, fzd.bidderValue);
    windowAggOperator->setCurrentKey(key);

    // First AssignStateNamespace call
    BinaryRowData *inputRow = BinaryRowData::createBinaryRowDataWithMem(2);
    inputRow->setLong(0, fzd.bidderValue);
    long ts1 = fzd.timestampValue;
    const std::vector<TimeWindow> assignResult1 = windowAggOperator->windowFunction->AssignStateNamespace(inputRow, ts1);
    std::cout << "  assign1 size=" << assignResult1.size();
    if (!assignResult1.empty()) {
        std::cout << ", start=" << assignResult1[0].getStart()
                  << ", end=" << assignResult1[0].getEnd();
    }
    std::cout << std::endl;

    // Second AssignStateNamespace with nearby timestamp (should merge for session window)
    BinaryRowData *inputRow2 = BinaryRowData::createBinaryRowDataWithMem(2);
    inputRow2->setLong(0, fzd.bidderValue);
    long ts2 = fzd.timestamp2 > 0 ? fzd.timestamp2 : (fzd.timestampValue + 1000);
    const std::vector<TimeWindow> assignResult2 = windowAggOperator->windowFunction->AssignStateNamespace(inputRow2, ts2);
    std::cout << "  assign2 size=" << assignResult2.size();
    if (!assignResult2.empty()) {
        std::cout << ", start=" << assignResult2[0].getStart()
                  << ", end=" << assignResult2[0].getEnd();
    }
    std::cout << std::endl;

    delete output;
}

// Test 3: Full processBatch + watermark advancement - reference from AggregateWindowOperatorTest::JsonTest
// Covers: processBatch, advanceWatermark, window result emission
void TestWindowProcessBatch(const WindowFuzzData& fzd)
{
    std::cout << "TestWindowProcessBatch" << std::endl;

    auto *output = new BatchOutputTest();
    auto *windowAggOperator = createWindowOperator(output);
    initWindowOperatorState(windowAggOperator);

    int rowCount = fzd.loopCount > 0 ? fzd.loopCount : 5;
    if (rowCount > 1000) rowCount = 1000;

    // Build bidder and timestamp arrays
    std::vector<int64_t> bidders(rowCount);
    std::vector<int64_t> timestamps(rowCount);
    for (int i = 0; i < rowCount; i++) {
        bidders[i] = fzd.bidderValue + (i % 3);
        timestamps[i] = fzd.timestampValue + i * 5000;
    }

    VectorBatch *vBatch = createWindowVectorBatch(rowCount, bidders, timestamps);
    auto *streamRecord = new StreamRecord(vBatch);
    windowAggOperator->processBatch(streamRecord);

    // Advance watermark to trigger window emissions
    long watermarkTs = fzd.timestampValue + rowCount * 5000 + fzd.windowSize + 100000;
    windowAggOperator->internalTimerService->advanceWatermark(watermarkTs);

    delete streamRecord;
    delete output;
}

// Test 4: SessionWindowAssigner direct test
// Covers: SessionWindowAssigner constructor, AssignWindows, IsEventTime, WithEventTime, WithProcessingTime, WithGap
void TestSessionWindowAssigner(const WindowFuzzData& fzd)
{
    std::cout << "TestSessionWindowAssigner" << std::endl;

    long sessionGap = fzd.windowSize > 0 ? fzd.windowSize : 10000;

    // Test WithGap factory
    auto *assigner = SessionWindowAssigner::WithGap(sessionGap);
    std::cout << "  isEventTime(default)=" << assigner->IsEventTime() << std::endl;

    // Test WithEventTime
    auto *eventAssigner = assigner->WithEventTime();
    std::cout << "  isEventTime(event)=" << eventAssigner->IsEventTime() << std::endl;

    // Test WithProcessingTime
    auto *procAssigner = assigner->WithProcessingTime();
    std::cout << "  isEventTime(proc)=" << procAssigner->IsEventTime() << std::endl;

    // Test AssignWindows
    BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(1);
    row->setLong(0, fzd.bidderValue);
    long ts = fzd.timestampValue;
    std::vector<TimeWindow> windows = eventAssigner->AssignWindows(row, ts);
    std::cout << "  assigned windows=" << windows.size();
    if (!windows.empty()) {
        std::cout << ", window=[" << windows[0].getStart() << ", " << windows[0].getEnd() << ")";
    }
    std::cout << std::endl;

    // Test MergeWindows
    std::set<TimeWindow> sortedWindows;
    sortedWindows.insert(TimeWindow(ts, ts + sessionGap));
    long ts2 = fzd.timestamp2 > 0 ? fzd.timestamp2 : (ts + sessionGap / 2);
    TimeWindow newWindow(ts2, ts2 + sessionGap);
    MergingWindowAssigner<TimeWindow>::MergeResultCollector mergeResult;
    eventAssigner->MergeWindows(newWindow, &sortedWindows, mergeResult);
    std::cout << "  merge result size=" << mergeResult.size() << std::endl;

    delete assigner;
    delete eventAssigner;
    delete procAssigner;
}

// Test 5: TimeWindow basic operations
// Covers: TimeWindow construction, intersects, cover, getStart, getEnd, maxTimestamp, equality, ordering
void TestTimeWindowOps(const WindowFuzzData& fzd)
{
    std::cout << "TestTimeWindowOps" << std::endl;

    long start1 = fzd.timestampValue;
    long size = fzd.windowSize > 0 ? fzd.windowSize : 10000;
    long end1 = start1 + size;

    TimeWindow w1(start1, end1);
    std::cout << "  w1: start=" << w1.getStart() << ", end=" << w1.getEnd()
              << ", maxTimestamp=" << w1.maxTimestamp() << std::endl;

    // Second window: overlapping
    long start2 = fzd.timestamp2 > 0 ? fzd.timestamp2 : (start1 + size / 2);
    long end2 = start2 + size;
    TimeWindow w2(start2, end2);
    std::cout << "  w2: start=" << w2.getStart() << ", end=" << w2.getEnd() << std::endl;

    // intersects
    bool intersects = w1.intersects(w2);
    std::cout << "  intersects=" << intersects << std::endl;

    // cover
    TimeWindow covered = w1.cover(w2);
    std::cout << "  cover: start=" << covered.getStart() << ", end=" << covered.getEnd() << std::endl;

    // equality
    TimeWindow w1copy(start1, end1);
    std::cout << "  w1==w1copy=" << (w1 == w1copy) << std::endl;
    std::cout << "  w1==w2=" << (w1 == w2) << std::endl;

    // ordering
    std::cout << "  w1<w2=" << (w1 < w2) << std::endl;

    // getWindowStartWithOffset
    long windowStart = TimeWindow::getWindowStartWithOffset(fzd.timestampValue, 0, size);
    std::cout << "  windowStartWithOffset=" << windowStart << std::endl;
}

int GlobalWindowFuzz(struct WindowFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "WindowFuzz: chooseFunc=" << chooseFunc
              << ", windowSize=" << fzd.windowSize
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestWindowState(fzd); break;
        case 2: TestWindowTimeAssign(fzd); break;
        case 3: TestWindowProcessBatch(fzd); break;
        case 4: TestSessionWindowAssigner(fzd); break;
        case 5: TestTimeWindowOps(fzd); break;
        default:
            TestWindowState(fzd);
            TestWindowTimeAssign(fzd);
            TestWindowProcessBatch(fzd);
            TestSessionWindowAssigner(fzd);
            TestTimeWindowOps(fzd);
            break;
    }
    return 0;
}
