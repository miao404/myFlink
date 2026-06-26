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
        : description_(description),
          selectedRowsBuffer_(1024),
          executionContext_(std::make_unique<omniruntime::op::ExecutionContext>())
{
    this->setOutput(output);
    parseDescription(description);
    LOG("StreamCorrelateOperator description: " << description.dump())
}

StreamCorrelateOperator::~StreamCorrelateOperator()
{
    delete timestampedCollector_;
    delete argEvaluator_;
}

void StreamCorrelateOperator::open()
{
    parseDescription(description_);

    // Build expression evaluator for functionArgs (nested expressions)
    if (hasFunctionArgs_) {
        JSONParser parser = JSONParser();
        for (const auto& argJson : functionArgsJson_) {
            auto expr = parser.ParseJSON(argJson);
            if (expr == nullptr) {
                omniruntime::expressions::Expr::DeleteExprs(argExprs_);
                argExprs_.clear();
                THROW_LOGIC_EXCEPTION(
                    "StreamCorrelateOperator: failed to parse functionArgs expression: "
                    + argJson.dump());
            }
            argExprs_.push_back(expr);
        }
        auto ofConfig = new omniruntime::op::OverflowConfig();
        argEvaluator_ = new omniruntime::codegen::ExpressionEvaluator(
                argExprs_, argInputTypes_, ofConfig);
        argEvaluator_->ProjectFuncGeneration();
    }

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

    // Parse functionArgs (new path) or functionArgIndices (legacy path)
    if (desc.contains("functionArgs") && desc["functionArgs"].is_array()
            && !desc["functionArgs"].empty()) {
        hasFunctionArgs_ = true;
        functionArgsJson_ = desc["functionArgs"].get<std::vector<nlohmann::json>>();
    }

    if (desc.contains("functionArgIndices")) {
        functionArgIndices_ = desc.at("functionArgIndices").get<std::vector<int>>();
    }

    inputTypes_ = desc.at("inputTypes").get<std::vector<std::string>>();
    outputTypes_ = desc.at("outputTypes").get<std::vector<std::string>>();
    functionResultTypes_ = desc.at("functionResultTypes").get<std::vector<std::string>>();

    inputColumnCount_ = static_cast<int>(inputTypes_.size());
    outputColumnCount_ = static_cast<int>(outputTypes_.size());

    // 预解析输入列的 OmniTypeId
    inputTypeIds_.clear();
    for (const auto& typeStr : inputTypes_) {
        inputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }

    // Build argInputTypes_ for expression evaluator
    if (hasFunctionArgs_) {
        std::vector<omniruntime::type::DataTypePtr> types;
        for (const auto& typeStr : inputTypes_) {
            auto omniType = LogicalType::flinkTypeToOmniTypeId(typeStr);
            types.push_back(std::make_shared<omniruntime::type::DataType>(omniType));
        }
        argInputTypes_ = omniruntime::type::DataTypes(types);
    }

    tableFunction_ = NativeTableFunctionFactory::create(functionName_);
    if (!tableFunction_) {
        THROW_LOGIC_EXCEPTION("Unsupported table function class: " + functionClass_ + ", function name: " + functionName_);
    }

    INFO_RELEASE("StreamCorrelateOperator parsed: functionName=" << functionName_
                                                        << ", joinType=" << joinType_
                                                        << ", hasFunctionArgs=" << hasFunctionArgs_
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

    // ========== 第一步：对每行调用 UDTF，收集结果 ==========
    // inputRowIndices[i] = 该输出行对应的输入行号
    // udtfResults[i]     = 该输出行的 UDTF 输出字符串
    std::vector<int> inputRowIndices;
    std::vector<std::string> udtfResults;
    // 记录哪些输入行没有产生输出（用于 LEFT JOIN）
    std::vector<bool> hasOutput(inputRowCount, false);

    // Evaluate UDTF argument: expression-based or direct column reference
    omniruntime::vec::VectorBatch* argEvalBatch = nullptr;
    omniruntime::vec::BaseVector* argVec = nullptr;

    if (hasFunctionArgs_ && argEvaluator_ != nullptr) {
        // Evaluate nested expression to produce argument column(s)
        argEvalBatch = argEvaluator_->Evaluate(
                inputBatch, executionContext_.get(), &selectedRowsBuffer_);
        if (argEvalBatch == nullptr || argEvalBatch->GetVectorCount() == 0) {
            delete inputBatch;
            delete input;
            delete argEvalBatch;
            return;
        }
        argVec = argEvalBatch->Get(0);
    } else {
        // Legacy path: direct column index
        if (functionArgIndices_.empty()) {
            THROW_LOGIC_EXCEPTION(
                "StreamCorrelateOperator: functionArgIndices is empty and no functionArgs provided");
        }
        int argColIndex = functionArgIndices_[0];
        argVec = inputBatch->Get(argColIndex);
    }

    for (int row = 0; row < inputRowCount; row++) {
        std::string argValue;
        if (!argVec->IsNull(row)) {
            // 读取 VARCHAR 列的值
            if (argVec->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                auto* castedVec = reinterpret_cast<VarcharVector*>(argVec);
                std::string_view sv = castedVec->GetValue(row);
                argValue = std::string(sv.data(), sv.size());
            } else {
                // Dictionary 编码的 VARCHAR
                using DictVarcharVec = Vector<DictionaryContainer<
                        std::string_view, LargeStringContainer>>;
                auto* castedVec = reinterpret_cast<DictVarcharVec*>(argVec);
                std::string_view sv = castedVec->GetValue(row);
                argValue = std::string(sv.data(), sv.size());
            }
        }

        // 调用 native JsonSplit
        std::vector<std::string> results = tableFunction_->eval(argValue);

        if (!results.empty()) {
            hasOutput[row] = true;
            for (auto& r : results) {
                inputRowIndices.push_back(row);
                udtfResults.push_back(std::move(r));
            }
        }
    }

    // ========== 第二步：处理 LEFT JOIN（补 null 行） ==========
    // leftNullRows 记录需要补 null 的输入行号
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
        delete argEvalBatch;
        delete inputBatch;
        delete input;
        return;
    }

    // ========== 第三步：构建输出 VectorBatch ==========
    // 输出列 = 输入列（按 inputRowIndices 展开） + UDTF 输出列
    // 对于 LEFT JOIN null 行：输入列正常复制，UDTF 输出列设为 null

    auto* outputBatch = new omnistream::VectorBatch(totalOutputRows);

    // --- 3a. 复制输入列（按展开后的行索引） ---
    // 构建完整的行索引数组：先是正常展开行，再是 LEFT JOIN null 行
    std::vector<int> allInputRowIndices;
    allInputRowIndices.reserve(totalOutputRows);
    allInputRowIndices.insert(allInputRowIndices.end(),
                              inputRowIndices.begin(), inputRowIndices.end());
    allInputRowIndices.insert(allInputRowIndices.end(),
                              leftNullRows.begin(), leftNullRows.end());

    for (int col = 0; col < inputColumnCount_; col++) {
        BaseVector* srcVec = inputBatch->Get(col);
        // 使用 CopyPositionsVector 按指定行索引复制列
        // 处理 Dictionary 编码的 VARCHAR
        BaseVector* dstVec;
        if ((srcVec->GetTypeId() == OMNI_VARCHAR || srcVec->GetTypeId() == OMNI_CHAR)
            && srcVec->GetEncoding() == omniruntime::vec::OMNI_DICTIONARY) {
            dstVec = omnistream::VectorBatch::CopyPositionsAndFlatten(
                    srcVec, allInputRowIndices.data(), 0, totalOutputRows);
        } else {
            dstVec = VectorHelper::CopyPositionsVector(
                    srcVec, allInputRowIndices.data(), 0, totalOutputRows);
        }
        outputBatch->Append(dstVec);
    }

    // --- 3b. 构建 UDTF 输出列 ---
    // JsonSplit 只输出一个 VARCHAR 列
    int udtfResultCount = static_cast<int>(functionResultTypes_.size());
    for (int udtfCol = 0; udtfCol < udtfResultCount; udtfCol++) {
        auto* vec = new VarcharVector(totalOutputRows);

        // 正常展开行：设置 UDTF 结果值
        int normalRows = static_cast<int>(udtfResults.size());
        for (int i = 0; i < normalRows; i++) {
            std::string_view sv(udtfResults[i].data(), udtfResults[i].size());
            vec->SetValue(i, sv);
        }

        // LEFT JOIN null 行：设置 null
        for (int i = normalRows; i < totalOutputRows; i++) {
            vec->SetNull(i);
        }

        outputBatch->Append(vec);
    }

    // --- 3c. 设置 timestamp 和 RowKind ---
    auto* oldTimestamps = inputBatch->getTimestamps();
    auto* oldRowKinds = inputBatch->getRowKinds();
    for (int i = 0; i < totalOutputRows; i++) {
        int srcRow = allInputRowIndices[i];
        outputBatch->setTimestamp(i, oldTimestamps[srcRow]);
        outputBatch->setRowKind(i, oldRowKinds[srcRow]);
    }

    // ========== 第四步：输出并清理 ==========
    delete argEvalBatch;
    delete inputBatch;
    delete input;

    timestampedCollector_->collect(outputBatch);
}