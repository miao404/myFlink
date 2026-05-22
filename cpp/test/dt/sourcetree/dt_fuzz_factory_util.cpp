/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Factory utilities implementation for OmniStream DT fuzz testing
 */

#include "dt_fuzz_factory_util.h"

namespace DtFuzzFactoryUtil {

json CreateGroupAggConfig(const std::string &aggFunction, const std::string &aggName,
                          const std::vector<std::string> &inputTypes,
                          const std::vector<std::string> &outputTypes,
                          const std::vector<int> &grouping,
                          const std::vector<int> &argIndexes,
                          const std::vector<std::string> &accTypes,
                          const std::vector<std::string> &aggValueTypes,
                          int indexOfCountStar)
{
    json config;
    config["originDescription"] = nullptr;
    config["inputTypes"] = inputTypes;
    config["outputTypes"] = outputTypes;
    config["grouping"] = grouping;
    config["distinctInfos"] = json::array();

    json aggCall;
    aggCall["name"] = aggName;
    aggCall["aggregationFunction"] = aggFunction;
    aggCall["argIndexes"] = argIndexes;
    aggCall["consumeRetraction"] = "false";
    aggCall["filterArg"] = -1;

    json aggInfoList;
    aggInfoList["aggregateCalls"] = json::array({aggCall});
    aggInfoList["accTypes"] = accTypes;
    aggInfoList["aggValueTypes"] = aggValueTypes;
    aggInfoList["indexOfCountStar"] = indexOfCountStar;

    config["aggInfoList"] = aggInfoList;
    return config;
}

json CreateDeduplicateConfig(const std::vector<std::string> &inputTypes,
                             const std::vector<int32_t> &grouping,
                             int rowtimeIndex,
                             bool keepLastRow,
                             bool generateUpdateBefore,
                             bool generateInsert)
{
    json config;
    config["inputTypes"] = inputTypes;
    config["grouping"] = grouping;
    config["rowtimeIndex"] = rowtimeIndex;
    config["keepLastRow"] = keepLastRow;
    config["generateUpdateBefore"] = generateUpdateBefore;
    config["generateInsert"] = generateInsert;
    return config;
}

json CreateJoinConfig(const std::string &joinType,
                      const std::vector<std::string> &leftInputTypes,
                      const std::vector<std::string> &rightInputTypes,
                      const std::vector<int> &leftKeys,
                      const std::vector<int> &rightKeys,
                      const std::vector<bool> &filterNulls)
{
    json config;
    config["joinType"] = joinType;
    config["leftInputTypes"] = leftInputTypes;
    config["rightInputTypes"] = rightInputTypes;
    config["leftKeys"] = leftKeys;
    config["rightKeys"] = rightKeys;
    config["filterNulls"] = filterNulls;
    config["leftIsOuter"] = (joinType == "LeftOuterJoin");
    config["rightIsOuter"] = false;

    json leftCondition;
    leftCondition["minRetentionTime"] = 0;
    leftCondition["maxRetentionTime"] = 0;
    config["leftCondition"] = leftCondition;

    json rightCondition;
    rightCondition["minRetentionTime"] = 0;
    rightCondition["maxRetentionTime"] = 0;
    config["rightCondition"] = rightCondition;

    return config;
}

json CreateRankConfig(const std::string &processFunction,
                      const std::vector<std::string> &inputTypes,
                      const std::vector<std::string> &outputTypes,
                      const std::vector<int> &partitionKey,
                      const std::vector<int> &sortFieldIndices,
                      const std::vector<bool> &sortAscendingOrders,
                      const std::vector<bool> &sortNullsIsLast,
                      bool outputRankNumber,
                      const std::string &rankRange,
                      bool generateUpdateBefore)
{
    json config;
    config["originDescription"] = nullptr;
    config["inputTypes"] = inputTypes;
    config["outputTypes"] = outputTypes;
    config["partitionKey"] = partitionKey;
    config["sortFieldIndices"] = sortFieldIndices;
    config["sortAscendingOrders"] = sortAscendingOrders;
    config["sortNullsIsLast"] = sortNullsIsLast;
    config["outputRankNumber"] = outputRankNumber;
    config["rankRange"] = rankRange;
    config["generateUpdateBefore"] = generateUpdateBefore;
    config["processFunction"] = processFunction;
    return config;
}

BinaryRowData *CreateBinaryRowDataFromLongs(const std::vector<int64_t> &values)
{
    size_t numCols = values.size();
    BinaryRowData *rowData = BinaryRowData::createBinaryRowDataWithMem(numCols);
    for (size_t i = 0; i < numCols; ++i) {
        rowData->setLong(i, values[i]);
    }
    return rowData;
}

std::vector<omnistream::RowField> CreateRowFields(const std::vector<std::string> &types)
{
    std::vector<omnistream::RowField> fields;
    for (size_t i = 0; i < types.size(); ++i) {
        std::string colName = "col" + std::to_string(i);
        if (types[i] == "BIGINT") {
            fields.emplace_back(colName, BasicLogicalType::BIGINT);
        } else if (types[i] == "INTEGER") {
            fields.emplace_back(colName, BasicLogicalType::INTEGER);
        } else if (types[i].find("VARCHAR") != std::string::npos || types[i] == "STRING") {
            fields.emplace_back(colName, BasicLogicalType::VARCHAR);
        } else if (types[i] == "BOOLEAN") {
            fields.emplace_back(colName, BasicLogicalType::BOOLEAN);
        } else if (types[i].find("TIMESTAMP") != std::string::npos) {
            fields.emplace_back(colName, BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE);
        } else {
            fields.emplace_back(colName, BasicLogicalType::BIGINT);
        }
    }
    return fields;
}

} // namespace DtFuzzFactoryUtil
