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

#include "NativeTableFunctionFactory.h"
#include "JsonSplitFunction.h"

std::shared_ptr<NativeTableFunction> NativeTableFunctionFactory::create(const std::string& functionIdentifier)
{
    if (functionIdentifier == "com.ctrip.ops.rtp.flink.example.sql.udf.tablefunction.JsonSplit"
        || functionIdentifier == "org.example.sql.JsonSplit"
        || functionIdentifier == "JsonSplit"
        || functionIdentifier == "jsontest"
        || functionIdentifier == "jsonsplit") {
        return std::make_shared<JsonSplitFunction>();
    }
    return nullptr;
}
