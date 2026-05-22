/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for StreamingJoinOperator covering InnerJoin and LeftOuterJoin
 *              with BIGINT key types and various data distributions.
 */

#include "table_fuzz_wrapper.h"
#include "dt_fuzz_data.h"
#include "dt_fuzz_factory_util.h"
#include "runtime_env_util.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "table/runtime/operators/join/StreamingJoinOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/RowDataSerializer.h"
#include "core/operators/OutputTest.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include <test/util/test_util.h>

using json = nlohmann::json;
using namespace DtFuzzFactoryUtil;
using namespace DtRuntimeEnvUtil;

static void TestInnerJoin(const TableFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "JoinFuzz: InnerJoin" << std::endl;

    json config = CreateJoinConfig(
        "InnerJoin",
        {"BIGINT", "BIGINT"},
        {"BIGINT", "BIGINT"},
        {0}, {0},
        {true, true});

    auto *output = new BatchOutputTest();
    StreamingJoinOperator<long> *joinOp = new StreamingJoinOperator<long>(config, output);

    auto rowFields = CreateRowFields({"BIGINT", "BIGINT"});
    auto *ctx = CreateRuntimeEnvWithOperatorId("HashMapStateBackend", rowFields,
                                                "deadbeefdeadbeefdeadbeefdeadbeef");

    joinOp->initializeState(ctx->initializer, ctx->serializer);
    joinOp->open();

    int rowCnt = (loopCount % 10) + 2;
    std::vector<long> leftCol0(rowCnt);
    std::vector<long> leftCol1(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        leftCol0[i] = (fzd.longValue + i) % 5;
        leftCol1[i] = fzd.longValue2 + i * 100;
    }

    omnistream::VectorBatch *leftVb = new omnistream::VectorBatch(rowCnt);
    leftVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, leftCol0.data()));
    leftVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, leftCol1.data()));

    StreamRecord *leftRecord = new StreamRecord(leftVb);
    joinOp->processBatch1(leftRecord);

    std::vector<long> rightCol0(rowCnt);
    std::vector<long> rightCol1(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        rightCol0[i] = (fzd.longValue + i) % 5;
        rightCol1[i] = fzd.longValue3 + i * 200;
    }

    omnistream::VectorBatch *rightVb = new omnistream::VectorBatch(rowCnt);
    rightVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, rightCol0.data()));
    rightVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, rightCol1.data()));

    StreamRecord *rightRecord = new StreamRecord(rightVb);
    joinOp->processBatch2(rightRecord);

    delete joinOp;
    delete ctx;
}

static void TestLeftOuterJoin(const TableFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "JoinFuzz: LeftOuterJoin" << std::endl;

    json config = CreateJoinConfig(
        "LeftOuterJoin",
        {"BIGINT", "BIGINT"},
        {"BIGINT", "BIGINT"},
        {0}, {0},
        {false, false});

    auto *output = new BatchOutputTest();
    StreamingJoinOperator<long> *joinOp = new StreamingJoinOperator<long>(config, output);

    auto rowFields = CreateRowFields({"BIGINT", "BIGINT"});
    auto *ctx = CreateRuntimeEnvWithOperatorId("HashMapStateBackend", rowFields,
                                                "deadbeefdeadbeefdeadbeefdeadbeef");

    joinOp->initializeState(ctx->initializer, ctx->serializer);
    joinOp->open();

    int rowCnt = (loopCount % 8) + 2;
    std::vector<long> leftCol0(rowCnt);
    std::vector<long> leftCol1(rowCnt);
    for (int i = 0; i < rowCnt; ++i) {
        leftCol0[i] = (fzd.longValue + i) % 8;
        leftCol1[i] = fzd.longValue2 + i * 50;
    }

    omnistream::VectorBatch *leftVb = new omnistream::VectorBatch(rowCnt);
    leftVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, leftCol0.data()));
    leftVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, leftCol1.data()));

    StreamRecord *leftRecord = new StreamRecord(leftVb);
    joinOp->processBatch1(leftRecord);

    int rightRowCnt = (loopCount % 5) + 1;
    std::vector<long> rightCol0(rightRowCnt);
    std::vector<long> rightCol1(rightRowCnt);
    for (int i = 0; i < rightRowCnt; ++i) {
        rightCol0[i] = (fzd.longValue + i) % 3;
        rightCol1[i] = fzd.longValue3 + i * 75;
    }

    omnistream::VectorBatch *rightVb = new omnistream::VectorBatch(rightRowCnt);
    rightVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rightRowCnt, rightCol0.data()));
    rightVb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rightRowCnt, rightCol1.data()));

    StreamRecord *rightRecord = new StreamRecord(rightVb);
    joinOp->processBatch2(rightRecord);

    delete joinOp;
    delete ctx;
}

int JoinFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseJoinType)
{
    try {
        switch (chooseJoinType % 2) {
            case 0:
                TestInnerJoin(fzd, loopCount);
                break;
            case 1:
                TestLeftOuterJoin(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "JoinFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
