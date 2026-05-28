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

#ifndef FLINK_TNEL_NATIVETABLEFUNCTION_H
#define FLINK_TNEL_NATIVETABLEFUNCTION_H

#include <string>
#include <vector>

class NativeTableFunction {
public:
    virtual std::vector<std::string> eval(const std::string& input) = 0;
    virtual std::string name() const = 0;
    virtual ~NativeTableFunction() = default;
};

#endif // FLINK_TNEL_NATIVETABLEFUNCTION_H
