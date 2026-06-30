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

#include "table/runtime/operators/correlate/StreamCorrelateOperator.h"
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include "OutputTest.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "OmniOperatorJIT/core/src/vector/vector_helper.h"

using json = nlohmann::json;
using VarcharVec = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;

/**
 * Test StreamCorrelateOperator with functionArgs containing a simple FIELD_REFERENCE.
 * This simulates LATERAL TABLE(jsontest(col0)) where col0 is directly referenced.
 */
TEST(StreamCorrelateOperatorTest, SimpleFieldRefFunctionArgs) {
    // Description with functionArgs containing a simple field reference to column 0
    std::string desc = R"DELIM({
        "originDescription": "Correlate(invocation=[jsontest($cor0.f0)])",
        "joinType": "InnerJoin",
        "functionName": "jsontest",
        "functionClass": "com.example.udf.JsonTest",
        "functionArgs": [
            {
                "exprType": "FIELD_REFERENCE",
                "dataType": 15,
                "width": 2147483647,
                "colVal": 0
            }
        ],
        "functionArgIndices": [0],
        "inputTypes": ["VARCHAR(2147483647)"],
        "outputTypes": ["VARCHAR(2147483647)", "VARCHAR(2147483647)"],
        "functionResultTypes": ["VARCHAR(2147483647)"],
        "condition": null
    })DELIM";

    int nrow = 2;
    std::vector<std::string> col0 = {
        "[\"a\",\"b\",\"c\"]",
        "[\"x\",\"y\"]"
    };

    auto* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col0.data(), nrow));

    json parsedJson = json::parse(desc);
    auto* output = new OutputTestVectorBatch();
    StreamCorrelateOperator op(parsedJson, output);
    op.open();

    auto* record = new StreamRecord(vb);
    op.processBatch(record);

    auto& results = output->getAll();
    ASSERT_EQ(results.size(), 1);
    auto* result = results[0];

    // 2 input rows: first produces 3 results, second produces 2 = 5 total rows
    EXPECT_EQ(result->GetRowCount(), 5);
    // 1 input column + 1 UDTF output column = 2
    EXPECT_EQ(result->GetVectorCount(), 2);

    op.close();
    delete output;
}

/**
 * Test StreamCorrelateOperator with functionArgs containing a nested json_query expression.
 * This simulates: LATERAL TABLE(jsontest(JSON_QUERY(room_data, '$.roomRate')))
 * The key scenario that was causing coredump before the fix.
 */
TEST(StreamCorrelateOperatorTest, NestedJsonQueryFunctionArgs) {
    // Description with functionArgs containing a nested json_query FUNCTION expression
    // This is what Java would produce for: jsontest(JSON_QUERY(col0, '$.rooms'))
    std::string desc = R"DELIM({
        "originDescription": "Correlate(invocation=[jsontest(JSON_QUERY($cor0.f0, '$.rooms'))])",
        "joinType": "InnerJoin",
        "functionName": "jsontest",
        "functionClass": "com.example.udf.JsonTest",
        "functionArgs": [
            {
                "exprType": "FUNCTION",
                "function_name": "json_query",
                "width": 2147483647,
                "arguments": [
                    {
                        "exprType": "FIELD_REFERENCE",
                        "dataType": 15,
                        "width": 2147483647,
                        "colVal": 0
                    },
                    {
                        "exprType": "LITERAL",
                        "dataType": 16,
                        "isNull": false,
                        "width": 8,
                        "value": "$.rooms"
                    }
                ],
                "returnType": 15
            }
        ],
        "functionArgIndices": [],
        "inputTypes": ["VARCHAR(2147483647)"],
        "outputTypes": ["VARCHAR(2147483647)", "VARCHAR(2147483647)"],
        "functionResultTypes": ["VARCHAR(2147483647)"],
        "condition": null
    })DELIM";

    // Input: a JSON object with a "rooms" field containing a JSON array
    int nrow = 1;
    std::string jsonInput = R"({"rooms":["room1","room2","room3"],"other":"data"})";

    auto* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(&jsonInput, nrow));

    json parsedJson = json::parse(desc);
    auto* output = new OutputTestVectorBatch();
    StreamCorrelateOperator op(parsedJson, output);
    op.open();

    auto* record = new StreamRecord(vb);
    op.processBatch(record);

    auto& results = output->getAll();
    ASSERT_EQ(results.size(), 1);
    auto* result = results[0];

    // json_query extracts '$.rooms' -> '["room1","room2","room3"]'
    // jsontest (JsonSplitFunction) splits this array -> 3 elements
    EXPECT_EQ(result->GetRowCount(), 3);
    // 1 input column + 1 UDTF output column = 2
    EXPECT_EQ(result->GetVectorCount(), 2);

    op.close();
    delete output;
}

/**
 * Test legacy path: functionArgIndices without functionArgs.
 * Ensures backward compatibility.
 */
TEST(StreamCorrelateOperatorTest, LegacyFunctionArgIndices) {
    std::string desc = R"DELIM({
        "originDescription": "Correlate(invocation=[jsontest($cor0.f0)])",
        "joinType": "InnerJoin",
        "functionName": "jsontest",
        "functionClass": "com.example.udf.JsonTest",
        "functionArgIndices": [0],
        "inputTypes": ["VARCHAR(2147483647)"],
        "outputTypes": ["VARCHAR(2147483647)", "VARCHAR(2147483647)"],
        "functionResultTypes": ["VARCHAR(2147483647)"],
        "condition": null
    })DELIM";

    int nrow = 1;
    std::string jsonInput = "[\"a\",\"b\"]";

    auto* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(&jsonInput, nrow));

    json parsedJson = json::parse(desc);
    auto* output = new OutputTestVectorBatch();
    StreamCorrelateOperator op(parsedJson, output);
    op.open();

    auto* record = new StreamRecord(vb);
    op.processBatch(record);

    auto& results = output->getAll();
    ASSERT_EQ(results.size(), 1);
    auto* result = results[0];

    EXPECT_EQ(result->GetRowCount(), 2);
    EXPECT_EQ(result->GetVectorCount(), 2);

    op.close();
    delete output;
}

/**
 * Test LeftOuterJoin with functionArgs: rows with no UDTF output should emit null.
 */
TEST(StreamCorrelateOperatorTest, LeftJoinWithFunctionArgs) {
    std::string desc = R"DELIM({
        "originDescription": "Correlate(invocation=[jsontest($cor0.f0)])",
        "joinType": "LeftOuterJoin",
        "functionName": "jsontest",
        "functionClass": "com.example.udf.JsonTest",
        "functionArgs": [
            {
                "exprType": "FIELD_REFERENCE",
                "dataType": 15,
                "width": 2147483647,
                "colVal": 0
            }
        ],
        "functionArgIndices": [0],
        "inputTypes": ["VARCHAR(2147483647)"],
        "outputTypes": ["VARCHAR(2147483647)", "VARCHAR(2147483647)"],
        "functionResultTypes": ["VARCHAR(2147483647)"],
        "condition": null
    })DELIM";

    int nrow = 2;
    std::vector<std::string> col0 = {
        "[\"a\"]",
        ""
    };

    auto* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col0.data(), nrow));
    // Set second row to null to test LEFT JOIN null padding
    vb->Get(0)->SetNull(1);

    json parsedJson = json::parse(desc);
    auto* output = new OutputTestVectorBatch();
    StreamCorrelateOperator op(parsedJson, output);
    op.open();

    auto* record = new StreamRecord(vb);
    op.processBatch(record);

    auto& results = output->getAll();
    ASSERT_EQ(results.size(), 1);
    auto* result = results[0];

    // Row 0: ["a"] -> 1 result row
    // Row 1: null -> no UDTF output, but LEFT JOIN keeps it with null UDTF columns
    // Total: 2 rows
    EXPECT_EQ(result->GetRowCount(), 2);
    EXPECT_EQ(result->GetVectorCount(), 2);

    op.close();
    delete output;
}

/**
 * Test that empty functionArgIndices with no functionArgs throws an exception.
 */
TEST(StreamCorrelateOperatorTest, EmptyArgIndicesThrows) {
    std::string desc = R"DELIM({
        "originDescription": "Correlate(invocation=[jsontest()])",
        "joinType": "InnerJoin",
        "functionName": "jsontest",
        "functionClass": "com.example.udf.JsonTest",
        "functionArgIndices": [],
        "inputTypes": ["VARCHAR(2147483647)"],
        "outputTypes": ["VARCHAR(2147483647)", "VARCHAR(2147483647)"],
        "functionResultTypes": ["VARCHAR(2147483647)"],
        "condition": null
    })DELIM";

    int nrow = 1;
    std::string jsonInput = "[\"a\"]";

    auto* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(&jsonInput, nrow));

    json parsedJson = json::parse(desc);
    auto* output = new OutputTestVectorBatch();
    StreamCorrelateOperator op(parsedJson, output);
    op.open();

    auto* record = new StreamRecord(vb);
    EXPECT_THROW(op.processBatch(record), std::exception);

    op.close();
    delete output;
}
