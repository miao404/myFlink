/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Per-operator fuzz data structures for OmniStream DT fuzz testing.
 *              Each operator defines its own FuzzData struct tailored to its specific
 *              data type requirements and processing semantics.
 */

#ifndef OMNISTREAM_DT_FUZZ_DATA_H
#define OMNISTREAM_DT_FUZZ_DATA_H

#include <cstdint>
#include <string>

enum class FuzzRowKind : uint8_t {
    INSERT = 0,
    UPDATE_BEFORE = 1,
    UPDATE_AFTER = 2,
    DELETE_KIND = 3
};

enum class FuzzStateBackend : uint8_t {
    HASHMAP = 0,
    ROCKSDB = 1
};

// GroupAggFunction: BIGINT keys + BIGINT agg values, RowKind semantics
struct AggregateFuzzData {
    int64_t keyValue;
    int64_t keyValue2;
    int64_t aggValue;
    int64_t aggValue2;
    int64_t aggValue3;
    uint8_t rowKindFlag;
    uint8_t stateBackendFlag;
};

// RowTimeDeduplicateFunction: BIGINT keys + timestamps + VARCHAR
struct DeduplicateFuzzData {
    int64_t keyCol1;
    int64_t keyCol2;
    int64_t timeCol;
    int64_t timestamp;
    uint8_t keepLastFlag;
    uint8_t generateUpdateBeforeFlag;
};

// WindowJoinOperator: INT/BIGINT keys + window end time + INT values
struct JoinFuzzData {
    int32_t leftKey;
    int32_t rightKey;
    int64_t windowEndTime;
    int32_t leftValue;
    int32_t rightValue;
    uint8_t joinTypeFlag;
};

// AppendOnlyTopNFunction / FastTop1Function: BIGINT partition + sort keys
struct RankFuzzData {
    int64_t partitionKey;
    int64_t partitionKey2;
    int64_t sortValue;
    int64_t sortValue2;
    int64_t dataValue;
    uint8_t rankFuncFlag;
    uint8_t sortOrderFlag;
};

// SinkOperator: BIGINT values + VARCHAR, VectorBatch-based
struct SinkFuzzData {
    int64_t longCol;
    int64_t longCol2;
    int32_t intCol;
    uint8_t sinkTypeFlag;
};

// Source operators: CSV fields, BIGINT + STRING types
struct SourceFuzzData {
    int64_t longField;
    int64_t longField2;
    int64_t longField3;
    uint8_t formatFlag;
};

// AggregateWindowOperator: BIGINT bidder + TIMESTAMP, window semantics
struct WindowFuzzData {
    int64_t keyValue;
    int64_t timestamp;
    int64_t timestamp2;
    int64_t windowSize;
    uint8_t windowTypeFlag;
};

// WatermarkAssignerOperator: timestamps + out-of-order tolerance
struct WatermarkAssignerFuzzData {
    int64_t eventTime;
    int64_t eventTime2;
    int64_t eventTime3;
    int64_t outOfOrderness;
    int64_t emissionInterval;
};

// ProcessOperator (LookupJoinRunner): BIGINT input columns
struct ProcessFuzzData {
    int64_t col0;
    int64_t col1;
    int64_t col2;
    int64_t col3;
    int64_t col4;
};

// StreamCalcBatch: BIGINT columns for projection/filter/expression
struct CalcFuzzData {
    int64_t col0;
    int64_t col1;
    int64_t col2;
    uint8_t exprTypeFlag;
    uint8_t conditionFlag;
};

// StreamExpand: BIGINT columns for multi-projection expansion
struct ExpandFuzzData {
    int64_t col0;
    int64_t col1;
    int64_t col2;
    uint8_t projectCountFlag;
};

// StreamFilter: generic Object-based, UDF .so loading
struct FilterFuzzData {
    int64_t longValue;
    int32_t intValue;
    bool boolValue;
    uint8_t filterModeFlag;
};

// StreamFlatMap: generic Object-based, UDF .so loading
struct FlatMapFuzzData {
    int64_t longValue;
    int32_t intValue;
    bool boolValue;
    uint8_t flatMapModeFlag;
};

// StreamGroupedReduceOperator: Object key + Object value, stateful
struct GroupReduceFuzzData {
    int64_t keyLong;
    int64_t valueLong;
    int64_t valueLong2;
    uint8_t stateBackendFlag;
};

// StreamMap: generic Object-based, UDF .so loading
struct MapFuzzData {
    int64_t longValue;
    int32_t intValue;
    bool boolValue;
    uint8_t mapModeFlag;
};

// StreamSource: source function configuration
struct SourceOperatorFuzzData {
    int64_t longField;
    int64_t longField2;
    int32_t intField;
    uint8_t sourceTypeFlag;
};

// KeyedProcessOperator (used with GroupAgg): same as aggregate
struct KeyedProcessFuzzData {
    int64_t keyValue;
    int64_t aggValue;
    int64_t aggValue2;
    uint8_t rowKindFlag;
    uint8_t aggFuncFlag;
};

// KeyedCoProcessOperator: dual-input with keys
struct CoProcessFuzzData {
    int64_t leftKey;
    int64_t leftValue;
    int64_t rightKey;
    int64_t rightValue;
    uint8_t modeFlag;
};

// Legacy compatibility: TableFuzzData / StreamingFuzzData wrappers
struct TableFuzzData {
    int32_t intValue;
    int64_t longValue;
    int64_t longValue2;
    int64_t longValue3;
    int64_t longValue4;
    bool boolValue;
    int64_t timestampMillis;
    std::string strValue;
    uint8_t rowKindFlag;
};

struct StreamingFuzzData {
    int32_t intValue;
    int64_t longValue;
    int64_t longValue2;
    bool boolValue;
    std::string strValue;
    int64_t timestampMillis;
    uint8_t rowKindFlag;
};

#endif // OMNISTREAM_DT_FUZZ_DATA_H
