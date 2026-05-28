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

package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidateCorrelateOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateCorrelateOPStrategy.class);

    private static final Set<String> SUPPORT_JOIN_TYPE = new HashSet<>(Arrays.asList(
            "InnerJoin", "LeftOuterJoin"));

    private static final Set<String> NATIVE_SUPPORTED_FUNCTION_CLASSES = new HashSet<>(Arrays.asList(
            "com.ctrip.ops.rtp.flink.example.sql.udf.tablefunction.JsonSplit"));

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        // validate joinType
        String joinType = (String) operatorInfoMap.get("joinType");
        if (joinType == null || !SUPPORT_JOIN_TYPE.contains(joinType)) {
            LOG.info("Unsupported correlate join type: {}", joinType);
            return false;
        }

        // validate functionClass
        String functionClass = (String) operatorInfoMap.get("functionClass");
        if (functionClass == null || !NATIVE_SUPPORTED_FUNCTION_CLASSES.contains(functionClass)) {
            LOG.info("Unsupported correlate function class: {}", functionClass);
            return false;
        }

        // validate data types
        if (!operatorInfoMap.containsKey("inputTypes") || !operatorInfoMap.containsKey("outputTypes")) {
            LOG.info("Missing inputTypes or outputTypes for Correlate operator.");
            return false;
        }

        return validateDataTypes(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
    }
}
