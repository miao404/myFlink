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

#ifndef FLINK_TNEL_STREAMCORRELATEOPERATOR_H
#define FLINK_TNEL_STREAMCORRELATEOPERATOR_H

#include <memory>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

#include "Output.h"
#include "AbstractUdfStreamOperator.h"
#include "OneInputStreamOperator.h"
#include "TimestampedCollector.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/types/logical/LogicalType.h"
#include "NativeTableFunction.h"
#include "NativeTableFunctionFactory.h"

class StreamCorrelateOperator : public OneInputStreamOperator, public AbstractStreamOperator<int> {
public:
    explicit StreamCorrelateOperator(const nlohmann::json& description, Output* output);
    ~StreamCorrelateOperator() override;

    void processBatch(StreamRecord* input) override;

    void processElement(StreamRecord* record) override
    {
        NOT_IMPL_EXCEPTION
    }

    void open() override;
    void close() override;

    const char* getName() override;

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        LOG("StreamCorrelateOperator initializeState()")
    }

    void ProcessWatermark(Watermark* watermark) override
    {
        output->emitWatermark(watermark);
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        output->emitWatermarkStatus(watermarkStatus);
    }

    std::string getTypeName() override
    {
        std::string typeName = "StreamCorrelateOperator";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }

    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        return AbstractStreamOperator::GetMectrics();
    }

private:
    void parseDescription(const nlohmann::json& desc);

    std::shared_ptr<NativeTableFunction> tableFunction_;
    std::string functionName_;
    std::string functionClass_;
    std::string joinType_;
    bool isLeftJoin_ = false;
    std::vector<int> functionArgIndices_;
    std::vector<std::string> inputTypes_;
    std::vector<std::string> outputTypes_;
    std::vector<std::string> functionResultTypes_;
    std::vector<omniruntime::type::DataTypeId> inputTypeIds_;
    int inputColumnCount_ = 0;
    int outputColumnCount_ = 0;
    nlohmann::json description_;
    TimestampedCollector* timestampedCollector_ = nullptr;
};

#endif // FLINK_TNEL_STREAMCORRELATEOPERATOR_H
