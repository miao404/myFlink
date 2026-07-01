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

#ifndef OMNISTREAM_STREAMCORRELATEOPERATOR_H
#define OMNISTREAM_STREAMCORRELATEOPERATOR_H


#include <functional>
#include <iostream>
#include <nlohmann/json.hpp>
#include <vector>
#include <string>

#include "streaming/api/operators/Output.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/types/logical/LogicalType.h"
#include "core/include/common.h"
#include "table/runtime/generated/function/tablefunction/NativeTableFunctionFactory.h"

#include "OmniOperatorJIT/core/src/type/data_types.h"
#include "OmniOperatorJIT/core/src/expression/expressions.h"
#include "OmniOperatorJIT/core/src/expression/jsonparser/jsonparser.h"
#include "OmniOperatorJIT/core/src/codegen/expr_evaluator.h"
#include "OmniOperatorJIT/core/src/operator/execution_context.h"
#include "OmniOperatorJIT/core/src/operator/config/operator_config.h"
#include "OmniOperatorJIT/core/src/memory/aligned_buffer.h"

class StreamCorrelateOperator : public OneInputStreamOperator,
                                public AbstractStreamOperator<int> {
public:
    explicit StreamCorrelateOperator(const nlohmann::json& description, Output* output);
    ~StreamCorrelateOperator() override;

    void processBatch(StreamRecord* input) override;

    void processElement(StreamRecord* record) override {
        NOT_IMPL_EXCEPTION
    }

    void open() override;
    void close() override;

    const char* getName() override { return "StreamCorrelateOperator"; }

    void initializeState(StreamTaskStateInitializerImpl* initializer,
                         TypeSerializer* keySerializer) override {}

    void ProcessWatermark(Watermark* watermark) override {
        output->emitWatermark(watermark);
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override {
        output->emitWatermarkStatus(watermarkStatus);
    }

    std::string getTypeName() override {
        return "StreamCorrelateOperator";
    }

    /**
     * Evaluate json_query on a single input string.
     * Behavior aligned with operatoromni's JsonQueryRetNull:
     *  - Supports dotted paths ($.a.b.c) and array subscripts ($.a[0].b)
     *  - Returns the JSON-serialized result only for objects/arrays; scalars → null
     *  - Null/empty/invalid input → null
     * @param input  The raw JSON string to query
     * @param path   JSON path expression (must start with "$.")
     * @param isNull [out] set to true if result is null
     * @return The extracted JSON fragment, or empty string if isNull
     */
    static std::string evalJsonQuery(std::string_view input, const std::string& path, bool& isNull);

    /**
     * Parse a JSON path expression into a list of navigation keys.
     * Supports: $.key, $.key1.key2, $.arr[0], $.arr[0].nested, $['key'], $[0]
     * Aligned with operatoromni's ParseJsonPath.
     */
    static std::vector<std::string> parseJsonPath(const std::string& path);

private:
    void parseDescription(const nlohmann::json& desc);

    // JsonSplit 的 native 实现：解析 JSON 数组字符串，返回各元素
    std::vector<std::string> evalJsonSplit(const std::string& input);

    nlohmann::json description_;
    TimestampedCollector* timestampedCollector_ = nullptr;

    // 从 description 解析出的元信息
    std::unique_ptr<NativeTableFunction> tableFunction_;
    std::string functionName_;
    std::string functionClass_;
    std::string joinType_;           // "InnerJoin" or "LeftOuterJoin"
    std::vector<int> functionArgIndices_;  // UDTF 参数对应的输入列索引
    std::vector<std::string> inputTypes_;
    std::vector<std::string> outputTypes_;
    std::vector<std::string> functionResultTypes_;
    int inputColumnCount_ = 0;
    int outputColumnCount_ = 0;
    bool isLeftJoin_ = false;

    // 输入列的 OmniTypeId（用于按行索引复制列）
    std::vector<omniruntime::type::DataTypeId> inputTypeIds_;

    // Expression-based argument evaluation (for nested expressions like JSON_QUERY)
    bool hasFunctionArgs_ = false;
    std::vector<nlohmann::json> functionArgsJson_;
    std::vector<omniruntime::expressions::Expr*> argExprs_;
    omniruntime::codegen::ExpressionEvaluator* argEvaluator_ = nullptr;
    omniruntime::type::DataTypes argInputTypes_;
    std::unique_ptr<omniruntime::op::ExecutionContext> executionContext_;
    omniruntime::mem::AlignedBuffer<int32_t> selectedRowsBuffer_;

    // Manual evaluation for recognized expressions (avoids JIT evaluator issues)
    enum class ArgEvalMode { EVALUATOR, FIELD_REF, JSON_QUERY };
    ArgEvalMode argEvalMode_ = ArgEvalMode::EVALUATOR;
    int manualArgColIndex_ = -1;
    std::string manualJsonPath_;
};


#endif //OMNISTREAM_STREAMCORRELATEOPERATOR_H