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
#include <nlohmann/json.hpp>

using namespace omniruntime::type;
using namespace omniruntime::vec;
using VarcharVector = Vector<LargeStringContainer<std::string_view>>;

StreamCorrelateOperator::StreamCorrelateOperator(
        const nlohmann::json& description, Output* output)
        : description_(description)
{
    this->setOutput(output);
    parseDescription(description);
    LOG("StreamCorrelateOperator description: " << description.dump())
}

StreamCorrelateOperator::~StreamCorrelateOperator()
{
    delete timestampedCollector_;
}

void StreamCorrelateOperator::open()
{
    parseDescription(description_);
    timestampedCollector_ = new TimestampedCollector(this->output);
}

void StreamCorrelateOperator::close()
{
    if (timestampedCollector_) {
        timestampedCollector_->close();
    }
}

void StreamCorrelateOperator::parseDescription(const nlohmann::json& desc)
{
    functionName_ = desc.at("functionName").get<std::string>();
    functionClass_ =  desc.at("functionClass").get<std::string>();
    joinType_ = desc.at("joinType").get<std::string>();
    isLeftJoin_ = (joinType_ == "LeftOuterJoin");

    functionArgIndices_ = desc.at("functionArgIndices").get<std::vector<int>>();
    inputTypes_ = desc.at("inputTypes").get<std::vector<std::string>>();
    outputTypes_ = desc.at("outputTypes").get<std::vector<std::string>>();
    functionResultTypes_ = desc.at("functionResultTypes").get<std::vector<std::string>>();

    inputColumnCount_ = static_cast<int>(inputTypes_.size());
    outputColumnCount_ = static_cast<int>(outputTypes_.size());

    inputTypeIds_.clear();
    for (const auto& typeStr : inputTypes_) {
        inputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    tableFunction_ = NativeTableFunctionFactory::create(functionName_);
    if (!tableFunction_) {
        THROW_LOGIC_EXCEPTION("Unsupported table function class: " + functionClass_ + ", function name: " + functionName_);
    }

    INFO_RELEASE("StreamCorrelateOperator parsed: functionName=" << functionName_
                                                        << ", joinType=" << joinType_
                                                        << ", argIndices=" << functionArgIndices_.size()
                                                        << ", inputCols=" << inputColumnCount_
                                                        << ", outputCols=" << outputColumnCount_
                                                        << ", isLeftJoin=" << isLeftJoin_)
}

void StreamCorrelateOperator::processBatch(StreamRecord* input)
{
    auto* inputBatch = reinterpret_cast<omnistream::VectorBatch*>(input->getValue());
    int inputRowCount = inputBatch->GetRowCount();

    if (inputRowCount == 0) {
        delete inputBatch;
        delete input;
        return;
    }

    // ========== Step 1: Call UDTF for each row, collect results ==========
    std::vector<int> inputRowIndices;
    std::vector<std::string> udtfResults;
    std::vector<bool> hasOutput(inputRowCount, false);

    int argColIndex = functionArgIndices_[0];
    auto* argVec = inputBatch->Get(argColIndex);

    for (int row = 0; row < inputRowCount; row++) {
        std::string argValue;
        if (!argVec->IsNull(row)) {
            if (argVec->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                auto* castedVec = reinterpret_cast<VarcharVector*>(argVec);
                std::string_view sv = castedVec->GetValue(row);
                argValue = std::string(sv.data(), sv.size());
            } else {
                using DictVarcharVec = Vector<DictionaryContainer<
                        std::string_view, LargeStringContainer>>;
                auto* castedVec = reinterpret_cast<DictVarcharVec*>(argVec);
                std::string_view sv = castedVec->GetValue(row);
                argValue = std::string(sv.data(), sv.size());
            }
        }

        std::vector<std::string> results = tableFunction_->eval(argValue);

        if (!results.empty()) {
            hasOutput[row] = true;
            for (auto& r : results) {
                inputRowIndices.push_back(row);
                udtfResults.push_back(std::move(r));
            }
        }
    }

    // ========== Step 2: Handle LEFT JOIN (null-padded rows) ==========
    std::vector<int> leftNullRows;
    if (isLeftJoin_) {
        for (int row = 0; row < inputRowCount; row++) {
            if (!hasOutput[row]) {
                leftNullRows.push_back(row);
            }
        }
    }

    int totalOutputRows = static_cast<int>(inputRowIndices.size())
                          + static_cast<int>(leftNullRows.size());

    if (totalOutputRows == 0) {
        delete inputBatch;
        delete input;
        return;
    }

    // ========== Step 3: Build output VectorBatch ==========
    auto* outputBatch = new omnistream::VectorBatch(totalOutputRows);

    // --- 3a. Copy input columns (expanded by row indices) ---
    std::vector<int> allInputRowIndices;
    allInputRowIndices.reserve(totalOutputRows);
    allInputRowIndices.insert(allInputRowIndices.end(),
                              inputRowIndices.begin(), inputRowIndices.end());
    allInputRowIndices.insert(allInputRowIndices.end(),
                              leftNullRows.begin(), leftNullRows.end());

    for (int col = 0; col < inputColumnCount_; col++) {
        BaseVector* srcVec = inputBatch->Get(col);
        DataTypeId typeId = srcVec->GetTypeId();

        if ((typeId == DataTypeId::OMNI_CHAR || typeId == DataTypeId::OMNI_VARCHAR)
                && srcVec->GetEncoding() == omniruntime::vec::OMNI_DICTIONARY) {
            outputBatch->Append(omnistream::VectorBatch::CopyPositionsAndFlatten(
                    srcVec, allInputRowIndices.data(), 0, totalOutputRows));
        } else {
            outputBatch->Append(VectorHelper::CopyPositionsVector(
                    srcVec, allInputRowIndices.data(), 0, totalOutputRows));
        }
    }

    // --- 3b. Build UDTF output columns ---
    int udtfResultCount = static_cast<int>(functionResultTypes_.size());
    for (int udtfCol = 0; udtfCol < udtfResultCount; udtfCol++) {
        auto* vec = new VarcharVector(totalOutputRows);

        int normalRows = static_cast<int>(udtfResults.size());
        for (int i = 0; i < normalRows; i++) {
            std::string_view sv(udtfResults[i].data(), udtfResults[i].size());
            vec->SetValue(i, sv);
        }

        for (int i = normalRows; i < totalOutputRows; i++) {
            vec->SetNull(i);
        }

        outputBatch->Append(vec);
    }

    // --- 3c. Set timestamp and RowKind ---
    auto* oldTimestamps = inputBatch->getTimestamps();
    auto* oldRowKinds = inputBatch->getRowKinds();
    for (int i = 0; i < totalOutputRows; i++) {
        int srcRow = allInputRowIndices[i];
        outputBatch->setTimestamp(i, oldTimestamps[srcRow]);
        outputBatch->setRowKind(i, oldRowKinds[srcRow]);
    }

    // ========== Step 4: Output and cleanup ==========
    timestampedCollector_->collect(outputBatch);

    delete inputBatch;
    delete input;
}

const char* StreamCorrelateOperator::getName()
{
    return OneInputStreamOperator::getName();
}
