/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz data structures for OmniStream DT fuzz testing
 */

#ifndef OMNISTREAM_DT_FUZZ_DATA_H
#define OMNISTREAM_DT_FUZZ_DATA_H

#include <cstdint>
#include <string>
#include <vector>

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

enum class FuzzDataType : uint8_t {
    BIGINT = 0,
    INTEGER = 1,
    VARCHAR = 2,
    BOOLEAN = 3,
    TIMESTAMP_0 = 4,
    TIMESTAMP_3 = 5,
    STRING = 6,
    DECIMAL64 = 7,
    DECIMAL128 = 8
};

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

#endif // OMNISTREAM_DT_FUZZ_DATA_H
