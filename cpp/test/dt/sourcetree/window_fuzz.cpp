#include "fuzz_wrapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "test/core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/TaskStateManager.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"
#include "table/types/logical/LogicalType.h"
#include "table/data/RowKind.h"
#include <nlohmann/json.hpp>
#include <iostream>

using namespace omnistream;
using json = nlohmann::json;

omnistream::VectorBatch* createWindowTestVectorBatch(int rowCount, int64_t bidderValue, int64_t timestampValue)
{
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    auto col0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto col1 = new omniruntime::vec::Vector<int64_t>(rowCount);

    for (int i = 0; i < rowCount; i++) {
        col0->SetValue(i, bidderValue + i);
        col1->SetValue(i, timestampValue + i * 1000);
        vb->setRowKind(i, RowKind::INSERT);
    }

    vb->Append(col0);
    vb->Append(col1);

    return vb;
}

static const std::string WINDOW_DESC = R"JSON({"input_channels":[0],"operators":[{"description":{"windowType":"TumblingWindow","windowSize":5000,"aggInfoList":{"accTypes":["BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"LongSumAggFunction","argIndexes":[0],"consumeRetraction":"false","name":"SUM($0)","filterArg":-1}],"indexOfCountStar":-1},"inputTypes":["BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT","BIGINT"]},"id":"AggregateWindowOperator","name":"AggregateWindow[tumbling]"}],"partition":{"channelNumber":1,"partitionName":"forward"}})JSON";

void TestWindowBasic(const WindowFuzzData& fzd)
{
    std::cout << "TestWindowBasic" << std::endl;

    json parsedJson = json::parse(WINDOW_DESC);

    std::string uniqueName = "AggregateWindowOperator";
    OperatorConfig opConfig(uniqueName, "AggregateWindow",
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;
    auto *windowOp = streamOperatorFactory.createOperatorAndCollector(opConfig, output);

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    windowOp->initializeState(initializer);
    windowOp->open();

    omnistream::VectorBatch* vb = createWindowTestVectorBatch(fzd.loopCount, fzd.bidderValue, fzd.timestampValue);
    windowOp->processBatch(new StreamRecord(vb));
}

void TestWindowMultiBatch(const WindowFuzzData& fzd)
{
    std::cout << "TestWindowMultiBatch" << std::endl;

    json parsedJson = json::parse(WINDOW_DESC);

    std::string uniqueName = "AggregateWindowOperator";
    OperatorConfig opConfig(uniqueName, "AggregateWindow",
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;
    auto *windowOp = streamOperatorFactory.createOperatorAndCollector(opConfig, output);

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    windowOp->initializeState(initializer);
    windowOp->open();

    for (int batch = 0; batch < 3; batch++) {
        omnistream::VectorBatch* vb = createWindowTestVectorBatch(fzd.loopCount, fzd.bidderValue, fzd.timestampValue + batch * fzd.windowSize);
        windowOp->processBatch(new StreamRecord(vb));
    }
}

void TestWindowLargeScale(const WindowFuzzData& fzd)
{
    std::cout << "TestWindowLargeScale" << std::endl;

    json parsedJson = json::parse(WINDOW_DESC);

    std::string uniqueName = "AggregateWindowOperator";
    OperatorConfig opConfig(uniqueName, "AggregateWindow",
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;
    auto *windowOp = streamOperatorFactory.createOperatorAndCollector(opConfig, output);

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(env2);
    windowOp->initializeState(initializer);
    windowOp->open();

    int scaleCount = fzd.loopCount > 0 ? fzd.loopCount : 100;
    omnistream::VectorBatch* vb = createWindowTestVectorBatch(scaleCount, fzd.bidderValue, fzd.timestampValue);
    windowOp->processBatch(new StreamRecord(vb));
}

int GlobalWindowFuzz(struct WindowFuzzData fzd, std::string filterExpr, int32_t chooseFunc)
{
    std::cout << "WindowFuzz: chooseFunc=" << chooseFunc
              << ", windowSize=" << fzd.windowSize
              << ", loopCount=" << fzd.loopCount << std::endl;

    switch (chooseFunc) {
        case 1: TestWindowBasic(fzd); break;
        case 2: TestWindowMultiBatch(fzd); break;
        case 3: TestWindowLargeScale(fzd); break;
        default:
            TestWindowBasic(fzd);
            TestWindowMultiBatch(fzd);
            TestWindowLargeScale(fzd);
            break;
    }
    return 0;
}
