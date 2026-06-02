#ifndef OMNISTREAM_FUZZ_WRAPPER_H
#define OMNISTREAM_FUZZ_WRAPPER_H

#include <string>
#include <vector>
#include <cstdint>

// ==================== Table Operator FuzzData Structs ====================

struct GroupAggFuzzData {
    int64_t keyValue;
    int64_t aggValue1;
    int64_t aggValue2;
    double aggValue3;
    bool boolValue;
    int32_t loopCount;
    int32_t aggFunctionType;  // 0=SUM, 1=COUNT, 2=AVG, 3=MAX, 4=MIN
    int32_t rowKind;          // 0=INSERT, 1=UPDATE_AFTER, 2=UPDATE_BEFORE, 3=DELETE
    std::string filterExpr;
};

struct DeduplicateFuzzData {
    int64_t keyValue;
    int64_t keyValue2;
    int64_t rowtimeValue;
    int64_t timestampValue;
    int32_t loopCount;
    bool keepLastRow;
    bool generateUpdateBefore;
    int32_t rowKind;
    std::string filterExpr;
};

struct JoinFuzzData {
    int32_t leftKeyValue;
    int32_t rightKeyValue;
    int64_t windowEndTime;
    int32_t leftValue;
    int32_t rightValue;
    int32_t loopCount;
    int32_t joinType;    // 0=InnerJoin, 1=LeftOuterJoin
    int32_t rowKind;
    std::string filterExpr;
};

struct RankFuzzData {
    int64_t keyValue;
    int64_t sortValue;
    int64_t dataValue;
    int32_t loopCount;
    int32_t topN;
    bool outputRankNumber;
    bool generateUpdateBefore;
    int32_t rowKind;
    std::string filterExpr;
};

struct SinkFuzzData {
    int32_t intValue;
    int64_t longValue;
    int32_t loopCount;
    int32_t rowKind;
    std::string outputFile;
    std::string filterExpr;
};

struct SourceFuzzData {
    int64_t value1;
    int64_t value2;
    int32_t loopCount;
    int32_t fieldCount;
    std::string filePath;
    std::string filterExpr;
};

struct WindowFuzzData {
    int64_t bidderValue;
    int64_t timestampValue;
    int64_t windowSize;
    int64_t timestamp2;
    int64_t stateValue;
    int32_t loopCount;
    int32_t rowKind;
    std::string filterExpr;
};

struct WatermarkAssignerFuzzData {
    int64_t timestampValue;
    int64_t dataValue;
    int64_t outOfOrderTime;
    int64_t emissionInterval;
    int32_t loopCount;
    int32_t timeRowIndex;
    std::string filterExpr;
};

// ==================== Streaming Operator FuzzData Structs ====================

struct KeyedProcessFuzzData {
    int64_t keyValue;
    int64_t value1;
    int64_t value2;
    int32_t loopCount;
    int32_t functionType;  // 0=GroupAgg, 1=FastTop1, 2=AppendOnlyTopN
    int32_t rowKind;
    std::string filterExpr;
};

struct CoProcessFuzzData {
    int64_t keyValue;
    int64_t value1;
    int64_t value2;
    int32_t loopCount;
    int32_t rowKind;
    std::string filterExpr;
};

struct ProcessFuzzData {
    int64_t value1;
    int64_t value2;
    int64_t value3;
    int64_t value4;
    int64_t lookupKey;
    int32_t loopCount;
    std::string filterExpr;
};

struct CalcFuzzData {
    int64_t value1;
    int64_t value2;
    int64_t value3;
    int32_t loopCount;
    int32_t exprType;  // 0=projection, 1=filter, 2=expression(ADD), 3=modulus
    std::string filterExpr;
};

struct ExpandFuzzData {
    int64_t value1;
    int64_t value2;
    int64_t value3;
    int32_t loopCount;
    int32_t projectCount;
    std::string filterExpr;
};

struct FilterFuzzData {
    int64_t value1;
    int64_t value2;
    int64_t value3;
    int32_t loopCount;
    int32_t rowKind;
    std::string filterExpr;
};

struct FlatMapFuzzData {
    int64_t value1;
    int64_t value2;
    int32_t loopCount;
    int32_t outputCount;
    std::string filterExpr;
};

struct GroupReduceFuzzData {
    int64_t longValue;
    int32_t loopCount;
    int32_t rowKind;
    std::string keyValue;
    std::string filterExpr;
};

struct MapFuzzData {
    int64_t value1;
    int64_t value2;
    int32_t loopCount;
    std::string filterExpr;
};

struct SourceOperatorFuzzData {
    int64_t value1;
    int64_t value2;
    int32_t loopCount;
    int32_t fieldCount;
    std::string filePath;
    std::string filterExpr;
};

struct TransformFuzzData {
    int64_t value1;
    int64_t value2;
    int64_t value3;
    int32_t loopCount;
    int32_t transformType;  // 0=filter, 1=map, 2=flatmap
    int32_t rowKind;
    std::string filterExpr;
};

// ==================== Global Fuzz Entry Points ====================

// Table operators
int GlobalGroupAggFuzz(struct GroupAggFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalDeduplicateFuzz(struct DeduplicateFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalJoinFuzz(struct JoinFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalRankFuzz(struct RankFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalSinkFuzz(struct SinkFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalSourceFuzz(struct SourceFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalWindowFuzz(struct WindowFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalWatermarkAssignerFuzz(struct WatermarkAssignerFuzzData fzd, std::string filterExpr, int32_t chooseFunc);

// Streaming operators
int GlobalKeyedProcessFuzz(struct KeyedProcessFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalCoProcessFuzz(struct CoProcessFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalProcessFuzz(struct ProcessFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalCalcFuzz(struct CalcFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalExpandFuzz(struct ExpandFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalFilterFuzz(struct FilterFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalFlatMapFuzz(struct FlatMapFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalGroupReduceFuzz(struct GroupReduceFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalMapFuzz(struct MapFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalSourceOperatorFuzz(struct SourceOperatorFuzzData fzd, std::string filterExpr, int32_t chooseFunc);
int GlobalTransformFuzz(struct TransformFuzzData fzd, std::string filterExpr, int32_t chooseFunc);

#endif // OMNISTREAM_FUZZ_WRAPPER_H
