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

#ifndef FLINK_TNEL_JSONSPLITFUNCTION_H
#define FLINK_TNEL_JSONSPLITFUNCTION_H

#include <nlohmann/json.hpp>
#include "NativeTableFunction.h"

class JsonSplitFunction : public NativeTableFunction {
public:
    std::vector<std::string> eval(const std::string& input) override
    {
        std::vector<std::string> results;
        try {
            nlohmann::json jsonArray = nlohmann::json::parse(input);
            if (jsonArray.is_array()) {
                for (const auto& element : jsonArray) {
                    if (element.is_string()) {
                        results.push_back(element.get<std::string>());
                    } else {
                        results.push_back(element.dump());
                    }
                }
            }
        } catch (const nlohmann::json::parse_error&) {
            // return empty vector on parse failure
        }
        return results;
    }

    std::string name() const override
    {
        return "JsonSplit";
    }
};

#endif // FLINK_TNEL_JSONSPLITFUNCTION_H
