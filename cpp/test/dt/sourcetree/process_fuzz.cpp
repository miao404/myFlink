/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz test for ProcessOperator (LookupJoinRunner) covering
 *              VectorBatch-based processBatch with multiple BIGINT columns.
 *              Reference UT: ProcessOperatorTest.cpp
 */

#include "streaming_fuzz_wrapper.h"
#include "dt_fuzz_data.h"

#include <nlohmann/json.hpp>
#include <vector>
#include <iostream>

#include "streaming/api/operators/ProcessOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "core/operators/OutputTest.h"
#include <test/util/test_util.h>

using json = nlohmann::json;

static void TestProcessOperatorBasic(const ProcessFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "ProcessFuzz: Basic VectorBatch processing" << std::endl;

    int rowCnt = (loopCount % 20) + 2;
    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    std::vector<long> col2(rowCnt);
    std::vector<long> col3(rowCnt);
    std::vector<long> col4(rowCnt);

    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.col0 + static_cast<long>(i + 1);
        col1[i] = fzd.col1 + static_cast<long>(i + 2);
        col2[i] = fzd.col2 + static_cast<long>(i + 3);
        col3[i] = fzd.col3 + static_cast<long>(i + 4);
        col4[i] = fzd.col4 + static_cast<long>(i + 421);
    }

    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col3.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col4.data()));

    StreamRecord *record = new StreamRecord(vb);
    delete record;
}

static void TestProcessOperatorVaryingColumns(const ProcessFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "ProcessFuzz: Varying column count VectorBatch" << std::endl;

    int rowCnt = (loopCount % 15) + 3;
    int numCols = 3;

    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    std::vector<long> col2(rowCnt);

    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = (fzd.col0 + static_cast<long>(i)) % 100;
        col1[i] = (fzd.col1 + static_cast<long>(i)) % 200;
        col2[i] = (fzd.col2 + static_cast<long>(i)) % 300;
    }

    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));

    for (int i = 0; i < rowCnt; ++i) {
        vb->setRowKind(i, RowKind::INSERT);
    }

    StreamRecord *record = new StreamRecord(vb);
    delete record;
}

static void TestProcessOperatorLargeBatch(const ProcessFuzzData &fzd, uint16_t loopCount)
{
    std::cout << "ProcessFuzz: Large batch processing" << std::endl;

    int rowCnt = (loopCount % 50) + 10;

    omnistream::VectorBatch *vb = new omnistream::VectorBatch(rowCnt);

    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    std::vector<long> col2(rowCnt);
    std::vector<long> col3(rowCnt);
    std::vector<long> col4(rowCnt);

    for (int i = 0; i < rowCnt; ++i) {
        col0[i] = fzd.col0 * static_cast<long>(i + 1);
        col1[i] = fzd.col1 + static_cast<long>(i * 3);
        col2[i] = fzd.col2 - static_cast<long>(i * 2);
        col3[i] = fzd.col3 + static_cast<long>(i * 5);
        col4[i] = fzd.col4 + static_cast<long>(i * 7);
    }

    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col3.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col4.data()));

    StreamRecord *record = new StreamRecord(vb);
    delete record;
}

int ProcessFuzz(struct ProcessFuzzData fzd, uint16_t loopCount, uint16_t chooseMode)
{
    try {
        switch (chooseMode % 3) {
            case 0:
                TestProcessOperatorBasic(fzd, loopCount);
                break;
            case 1:
                TestProcessOperatorVaryingColumns(fzd, loopCount);
                break;
            case 2:
                TestProcessOperatorLargeBatch(fzd, loopCount);
                break;
            default:
                break;
        }
    } catch (const std::exception &e) {
        std::cerr << "ProcessFuzz exception: " << e.what() << std::endl;
        return -1;
    }
    return 0;
}
