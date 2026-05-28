/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

package org.apache.flink.table.planner.plan.nodes.exec.common;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.CorrelateCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

/**
 * Base {@link ExecNode} which matches along with join a Java/Scala user defined table function.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class CommonExecCorrelate extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(CommonExecCorrelate.class);

    public static final String CORRELATE_TRANSFORMATION = "correlate";

    public static final String FIELD_NAME_JOIN_TYPE = "joinType";
    public static final String FIELD_NAME_FUNCTION_CALL = "functionCall";
    public static final String FIELD_NAME_CONDITION = "condition";

    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType joinType;

    @JsonProperty(FIELD_NAME_FUNCTION_CALL)
    private final RexCall invocation;

    @JsonProperty(FIELD_NAME_CONDITION)
    @Nullable
    private final RexNode condition;

    @JsonIgnore
    private final Class<?> operatorBaseClass;
    @JsonIgnore
    private final boolean retainHeader;

    protected CommonExecCorrelate(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            FlinkJoinType joinType,
            RexCall invocation,
            @Nullable RexNode condition,
            Class<?> operatorBaseClass,
            boolean retainHeader,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.joinType = checkNotNull(joinType);
        this.invocation = checkNotNull(invocation);
        this.condition = condition;
        this.operatorBaseClass = checkNotNull(operatorBaseClass);
        this.retainHeader = retainHeader;
    }

    private String getExtraDescription(String oldDescription, Transformation<RowData> inputTransform) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        HashMap<Integer, Integer> accessIndexMap = new HashMap<>();

        // get inputType info
        List<String> inputTypeList = new ArrayList<>();
        List<RowType.RowField> inputFields = ((InternalTypeInfo) inputTransform.getOutputType()).toRowType().getFields();
        int currentIndex = 0;
        for (int oldIndex = 0; oldIndex < inputFields.size(); oldIndex++) {
            RowType.RowField field = inputFields.get(oldIndex);
            LogicalType fieldType = field.getType();
            inputTypeList.add(DescriptionUtil.getFieldType(fieldType));
            accessIndexMap.put(oldIndex, currentIndex);
            currentIndex++;
        }

        // get outputTypes info
        List<String> outputTypeList = new ArrayList<>();
        List<RowType.RowField> outputFields = ((RowType) getOutputType()).getFields();
        for (RowType.RowField field : outputFields) {
            LogicalType fieldType = field.getType();
            outputTypeList.add(DescriptionUtil.getFieldType(fieldType));
        }

        // extract function info from invocation
        String functionName = "";
        String functionClass = "";
        SqlOperator operator = invocation.getOperator();
        if (operator instanceof BridgingSqlFunction) {
            BridgingSqlFunction bridgingFunc = (BridgingSqlFunction) operator;
            functionName = bridgingFunc.getName();
            functionClass = bridgingFunc.getDefinition().getClass().getName();
        } else {
            functionName = operator.getName();
            functionClass = operator.getClass().getName();
        }

        // extract function argument indices from invocation operands
        List<Integer> functionArgIndices = new ArrayList<>();
        for (RexNode operand : invocation.getOperands()) {
            if (operand instanceof RexFieldAccess) {
                RexFieldAccess fieldAccess = (RexFieldAccess) operand;
                if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
                    functionArgIndices.add(fieldAccess.getField().getIndex());
                }
            } else if (operand instanceof org.apache.calcite.rex.RexInputRef) {
                functionArgIndices.add(((org.apache.calcite.rex.RexInputRef) operand).getIndex());
            }
        }

        // get function result types
        List<String> functionResultTypes = new ArrayList<>();
        RowType outputType = (RowType) getOutputType();
        int inputFieldCount = inputFields.size();
        for (int i = inputFieldCount; i < outputType.getFieldCount(); i++) {
            functionResultTypes.add(DescriptionUtil.getFieldType(outputType.getTypeAt(i)));
        }

        // build condition map
        RexNodeUtil.accessIndexMap = accessIndexMap;
        Map<String, Object> conditionMap = null;
        if (condition != null) {
            conditionMap = RexNodeUtil.buildJsonMap(condition);
        }

        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("originDescription", oldDescription);
        jsonMap.put("joinType", joinType.toString());
        jsonMap.put("functionName", functionName);
        jsonMap.put("functionClass", functionClass);
        jsonMap.put("functionArgIndices", functionArgIndices);
        jsonMap.put("inputTypes", inputTypeList);
        jsonMap.put("outputTypes", outputTypeList);
        jsonMap.put("functionResultTypes", functionResultTypes);
        jsonMap.put("condition", conditionMap);

        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            LOG.warn("getExtraDescription error", e);
        }
        RexNodeUtil.accessIndexMap.clear();
        return jsonString;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(operatorBaseClass);
        Transformation<RowData> transformation = CorrelateCodeGenerator.generateCorrelateTransformation(
                config,
                ctx,
                inputTransform,
                (RowType) inputEdge.getOutputType(),
                invocation,
                JavaScalaConversionUtil.toScala(Optional.ofNullable(condition)),
                (RowType) getOutputType(),
                joinType,
                inputTransform.getParallelism(),
                retainHeader,
                getClass().getSimpleName(),
                createTransformationMeta(CORRELATE_TRANSFORMATION, config));
        String oldDescription = transformation.getDescription();
        transformation.setDescription(getExtraDescription(oldDescription, inputTransform));
        return transformation;
    }
}
