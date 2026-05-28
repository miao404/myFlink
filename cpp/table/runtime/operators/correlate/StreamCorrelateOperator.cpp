/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "StreamCorrelateOperator.h"
#include "NativeTableFunctionFactory.h"
#include <vector/vector_helper.h>

StreamCorrelateOperator::StreamCorrelateOperator(const nlohmann::json& description, Output* output)
    : description_(description)
{
    this->setOutput(output);

    functionClass_ = description["functionClass"].get<std::string>();
    std::string joinType = description["joinType"].get<std::string>();
    isLeftJoin_ = (joinType == "LeftOuterJoin" || joinType == "LEFT");

    if (description.contains("functionArgIndices")) {
        for (const auto& idx : description["functionArgIndices"]) {
            argIndices_.push_back(idx.get<int>());
        }
    }

    tableFunction_ = NativeTableFunctionFactory::create(functionClass_);
    if (!tableFunction_) {
        THROW_LOGIC_EXCEPTION("Unsupported table function class: " + functionClass_);
    }

    timestampedCollector_ = new TimestampedCollector(output);

    LOG("StreamCorrelateOperator created, functionClass=" << functionClass_
        << ", joinType=" << joinType << ", isLeftJoin=" << isLeftJoin_)
}

StreamCorrelateOperator::~StreamCorrelateOperator()
{
    delete timestampedCollector_;
}

void StreamCorrelateOperator::open()
{
}

void StreamCorrelateOperator::close()
{
}

void StreamCorrelateOperator::processBatch(StreamRecord* input)
{
    auto* batch = reinterpret_cast<omnistream::VectorBatch*>(input->getValue());
    int32_t rowCount = batch->GetRowCount();
    int32_t inputColCount = batch->GetVectorCount();

    if (argIndices_.empty() || rowCount == 0) {
        delete batch;
        return;
    }

    int argCol = argIndices_[0];

    // Collect UDTF results and build expansion indices
    std::vector<int> inputRowIndices;
    std::vector<std::string> udtfResults;
    std::vector<int> nullPaddedRows;

    auto* argVector = reinterpret_cast<omniruntime::vec::Vector<
        omniruntime::vec::LargeStringContainer<std::string_view>>*>(batch->Get(argCol));

    for (int32_t row = 0; row < rowCount; row++) {
        if (argVector->IsNull(row)) {
            if (isLeftJoin_) {
                nullPaddedRows.push_back(row);
            }
            continue;
        }
        std::string_view sv = argVector->GetValue(row);
        std::string argValue(sv.data(), sv.size());

        std::vector<std::string> results = tableFunction_->eval(argValue);

        if (results.empty()) {
            if (isLeftJoin_) {
                nullPaddedRows.push_back(row);
            }
            continue;
        }

        for (const auto& result : results) {
            inputRowIndices.push_back(row);
            udtfResults.push_back(result);
        }
    }

    int totalOutputRows = static_cast<int>(inputRowIndices.size() + nullPaddedRows.size());
    if (totalOutputRows == 0) {
        delete batch;
        return;
    }

    // Build position array for expanding input rows
    std::vector<int> positions;
    positions.reserve(totalOutputRows);
    for (int idx : inputRowIndices) {
        positions.push_back(idx);
    }
    for (int idx : nullPaddedRows) {
        positions.push_back(idx);
    }

    // Create output VectorBatch
    auto* outputBatch = new omnistream::VectorBatch(totalOutputRows);

    // Copy and expand input columns
    for (int col = 0; col < inputColCount; col++) {
        omniruntime::vec::BaseVector* expandedVec =
            omniruntime::vec::VectorHelper::CopyPositionsVector(
                batch->Get(col), positions.data(), 0, totalOutputRows);
        outputBatch->Append(expandedVec);
    }

    // Create UDTF result column (VARCHAR)
    int resultCount = static_cast<int>(udtfResults.size());
    int nullCount = static_cast<int>(nullPaddedRows.size());

    auto* resultVector = new omniruntime::vec::Vector<
        omniruntime::vec::LargeStringContainer<std::string_view>>(totalOutputRows);
    for (int i = 0; i < resultCount; i++) {
        resultVector->SetValue(i, std::string_view(udtfResults[i]));
    }
    for (int i = 0; i < nullCount; i++) {
        resultVector->SetNull(resultCount + i);
    }
    outputBatch->Append(resultVector);

    // Copy timestamps and rowKinds
    for (int i = 0; i < totalOutputRows; i++) {
        outputBatch->setTimestamp(i, batch->getTimestamp(positions[i]));
        outputBatch->setRowKind(i, batch->getRowKind(positions[i]));
    }

    delete batch;

    timestampedCollector_->collect(outputBatch);
}

const char* StreamCorrelateOperator::getName()
{
    return OneInputStreamOperator::getName();
}
