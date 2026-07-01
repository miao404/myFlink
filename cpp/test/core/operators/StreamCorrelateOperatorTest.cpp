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

// ============================================================================
// New tests for evalJsonQuery refactoring and array subscript support
// ============================================================================

/**
 * Test null input rows (simulating LEFT JOIN null padding). Verifies no crash
 * and that null rows do not produce UDTF output.
 */
TEST(StreamCorrelateOperatorTest, NullInputRowsNoCrash) {
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

    int nrow = 3;
    std::vector<std::string> col0 = {
        R"({"rooms":["r1","r2"]})",
        "",  // will be set null
        R"({"rooms":["r3"]})"
    };

    auto* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col0.data(), nrow));
    // Set second row as null (simulating LEFT JOIN null input)
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

    // Row 0: ["r1","r2"] -> 2 results; Row 1: null -> 0; Row 2: ["r3"] -> 1
    EXPECT_EQ(result->GetRowCount(), 3);

    op.close();
    delete output;
}

/**
 * Test invalid/malformed JSON string input does not crash, treated as null.
 */
TEST(StreamCorrelateOperatorTest, InvalidJsonInputNoCrash) {
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

    int nrow = 3;
    std::vector<std::string> col0 = {
        "not valid json {{{",
        R"({"rooms":["a"]})",
        "{truncated"
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

    // Only row 1 has valid JSON with extractable array -> 1 result
    EXPECT_EQ(result->GetRowCount(), 1);

    op.close();
    delete output;
}

/**
 * Test multi-level nested dot path: $.a.b.c
 */
TEST(StreamCorrelateOperatorTest, MultiLevelNestedDotPath) {
    std::string desc = R"DELIM({
        "originDescription": "Correlate(invocation=[jsontest(JSON_QUERY($cor0.f0, '$.a.b.c'))])",
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
                        "value": "$.a.b.c"
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

    int nrow = 1;
    // $.a.b.c should extract the array ["x","y","z"]
    std::string jsonInput = R"({"a":{"b":{"c":["x","y","z"]}}})";

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

    // $.a.b.c -> ["x","y","z"], jsontest splits -> 3 elements
    EXPECT_EQ(result->GetRowCount(), 3);

    op.close();
    delete output;
}

/**
 * Test array subscript path: $.roomInfos[0].attrs
 * This is the key new capability after refactoring.
 */
TEST(StreamCorrelateOperatorTest, ArraySubscriptPath) {
    std::string desc = R"DELIM({
        "originDescription": "Correlate(invocation=[jsontest(JSON_QUERY($cor0.f0, '$.roomInfos[0].attrs'))])",
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
                        "value": "$.roomInfos[0].attrs"
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

    int nrow = 1;
    // $.roomInfos[0].attrs -> ["tag1","tag2"]
    std::string jsonInput = R"({"roomInfos":[{"id":1,"attrs":["tag1","tag2"]},{"id":2,"attrs":["tag3"]}]})";

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

    // $.roomInfos[0].attrs -> ["tag1","tag2"], jsontest splits -> 2 elements
    EXPECT_EQ(result->GetRowCount(), 2);

    op.close();
    delete output;
}

/**
 * Test path that does not exist in the document -> null result, no output.
 */
TEST(StreamCorrelateOperatorTest, PathNotExistsReturnsNull) {
    std::string desc = R"DELIM({
        "originDescription": "Correlate(invocation=[jsontest(JSON_QUERY($cor0.f0, '$.missing.path'))])",
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
                        "value": "$.missing.path"
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

    int nrow = 1;
    std::string jsonInput = R"({"rooms":["a","b"]})";

    auto* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(&jsonInput, nrow));

    json parsedJson = json::parse(desc);
    auto* output = new OutputTestVectorBatch();
    StreamCorrelateOperator op(parsedJson, output);
    op.open();

    auto* record = new StreamRecord(vb);
    op.processBatch(record);

    auto& results = output->getAll();
    // Path does not exist -> json_query returns null -> UDTF gets empty string
    // -> no output rows from UDTF, and InnerJoin means no row emitted
    EXPECT_EQ(results.size(), 0);

    op.close();
    delete output;
}

/**
 * Test path extracting a scalar value (not object/array) -> null per json_query spec.
 */
TEST(StreamCorrelateOperatorTest, ScalarPathReturnsNull) {
    std::string desc = R"DELIM({
        "originDescription": "Correlate(invocation=[jsontest(JSON_QUERY($cor0.f0, '$.name'))])",
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
                        "value": "$.name"
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

    int nrow = 1;
    // $.name -> "Alice" which is a scalar string, not object/array -> null
    std::string jsonInput = R"({"name":"Alice","tags":["a"]})";

    auto* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(&jsonInput, nrow));

    json parsedJson = json::parse(desc);
    auto* output = new OutputTestVectorBatch();
    StreamCorrelateOperator op(parsedJson, output);
    op.open();

    auto* record = new StreamRecord(vb);
    op.processBatch(record);

    auto& results = output->getAll();
    // Scalar result -> null -> UDTF gets empty -> no output (InnerJoin)
    EXPECT_EQ(results.size(), 0);

    op.close();
    delete output;
}

/**
 * Test LEFT JOIN with json_query path producing no output: null row should be padded.
 */
TEST(StreamCorrelateOperatorTest, LeftJoinJsonQueryNoOutput) {
    std::string desc = R"DELIM({
        "originDescription": "Correlate(invocation=[jsontest(JSON_QUERY($cor0.f0, '$.missing'))])",
        "joinType": "LeftOuterJoin",
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
                        "value": "$.missing"
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

    int nrow = 2;
    std::vector<std::string> col0 = {
        R"({"rooms":["a","b"]})",
        R"({"data":"value"})"
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

    // Both rows: $.missing doesn't exist -> null -> no UDTF output
    // LeftOuterJoin pads both rows with null UDTF columns
    EXPECT_EQ(result->GetRowCount(), 2);
    EXPECT_EQ(result->GetVectorCount(), 2);

    // Verify UDTF output column (column 1) has nulls
    auto* udtfCol = result->Get(1);
    EXPECT_TRUE(udtfCol->IsNull(0));
    EXPECT_TRUE(udtfCol->IsNull(1));

    op.close();
    delete output;
}

/**
 * Unit test for the parseJsonPath static helper directly.
 */
TEST(StreamCorrelateOperatorTest, ParseJsonPathUnit) {
    // Simple dot path
    auto keys = StreamCorrelateOperator::parseJsonPath("$.rooms");
    ASSERT_EQ(keys.size(), 1);
    EXPECT_EQ(keys[0], "rooms");

    // Multi-level dot path
    keys = StreamCorrelateOperator::parseJsonPath("$.a.b.c");
    ASSERT_EQ(keys.size(), 3);
    EXPECT_EQ(keys[0], "a");
    EXPECT_EQ(keys[1], "b");
    EXPECT_EQ(keys[2], "c");

    // Array subscript
    keys = StreamCorrelateOperator::parseJsonPath("$.roomInfos[0].attrs");
    ASSERT_EQ(keys.size(), 3);
    EXPECT_EQ(keys[0], "roomInfos");
    EXPECT_EQ(keys[1], "0");
    EXPECT_EQ(keys[2], "attrs");

    // Multiple subscripts
    keys = StreamCorrelateOperator::parseJsonPath("$.data[1][2]");
    ASSERT_EQ(keys.size(), 3);
    EXPECT_EQ(keys[0], "data");
    EXPECT_EQ(keys[1], "1");
    EXPECT_EQ(keys[2], "2");

    // Quoted bracket notation
    keys = StreamCorrelateOperator::parseJsonPath("$['special.key']");
    ASSERT_EQ(keys.size(), 1);
    EXPECT_EQ(keys[0], "special.key");

    // Invalid path (no $)
    keys = StreamCorrelateOperator::parseJsonPath("rooms");
    EXPECT_TRUE(keys.empty());

    // Empty path
    keys = StreamCorrelateOperator::parseJsonPath("");
    EXPECT_TRUE(keys.empty());
}

/**
 * Unit test for evalJsonQuery static helper directly.
 */
TEST(StreamCorrelateOperatorTest, EvalJsonQueryUnit) {
    bool isNull = false;

    // Extract object
    auto result = StreamCorrelateOperator::evalJsonQuery(
        R"({"a":{"b":1},"c":[1,2]})", "$.a", isNull);
    EXPECT_FALSE(isNull);
    EXPECT_EQ(result, R"({"b":1})");

    // Extract array
    result = StreamCorrelateOperator::evalJsonQuery(
        R"({"items":[1,2,3]})", "$.items", isNull);
    EXPECT_FALSE(isNull);
    EXPECT_EQ(result, "[1,2,3]");

    // Scalar -> null
    result = StreamCorrelateOperator::evalJsonQuery(
        R"({"name":"Alice"})", "$.name", isNull);
    EXPECT_TRUE(isNull);

    // Array subscript access to object
    result = StreamCorrelateOperator::evalJsonQuery(
        R"({"arr":[{"x":1},{"x":2}]})", "$.arr[0]", isNull);
    EXPECT_FALSE(isNull);
    EXPECT_EQ(result, R"({"x":1})");

    // Nested array subscript
    result = StreamCorrelateOperator::evalJsonQuery(
        R"({"roomInfos":[{"id":1,"attrs":{"status":"open"}}]})",
        "$.roomInfos[0].attrs", isNull);
    EXPECT_FALSE(isNull);
    EXPECT_EQ(result, R"({"status":"open"})");

    // Missing path -> null
    result = StreamCorrelateOperator::evalJsonQuery(
        R"({"a":1})", "$.missing", isNull);
    EXPECT_TRUE(isNull);

    // Invalid JSON -> null
    result = StreamCorrelateOperator::evalJsonQuery(
        "not json {{{", "$.a", isNull);
    EXPECT_TRUE(isNull);

    // Null/empty input -> null
    result = StreamCorrelateOperator::evalJsonQuery("", "$.a", isNull);
    EXPECT_TRUE(isNull);

    result = StreamCorrelateOperator::evalJsonQuery(
        std::string_view(nullptr, 0), "$.a", isNull);
    EXPECT_TRUE(isNull);

    // Invalid path (no $) -> null
    result = StreamCorrelateOperator::evalJsonQuery(
        R"({"a":[1]})", "a", isNull);
    EXPECT_TRUE(isNull);

    // Array index out of bounds -> null
    result = StreamCorrelateOperator::evalJsonQuery(
        R"({"arr":[1,2]})", "$.arr[5]", isNull);
    EXPECT_TRUE(isNull);
}
