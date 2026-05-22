/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Factory utilities for creating OmniStream operators in fuzz testing
 */

#ifndef OMNISTREAM_DT_FUZZ_FACTORY_UTIL_H
#define OMNISTREAM_DT_FUZZ_FACTORY_UTIL_H

#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "table/runtime/operators/aggregate/GroupAggFunction.h"
#include "table/runtime/operators/deduplicate/RowTimeDeduplicateFunction.h"
#include "table/runtime/operators/rank/AppendOnlyTopNFunction.h"
#include "table/runtime/operators/rank/FastTop1Function.h"
#include "table/runtime/operators/join/StreamingJoinOperator.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/RowDataSerializer.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "core/operators/OutputTest.h"

using json = nlohmann::json;

namespace DtFuzzFactoryUtil {

json CreateGroupAggConfig(const std::string &aggFunction, const std::string &aggName,
                          const std::vector<std::string> &inputTypes,
                          const std::vector<std::string> &outputTypes,
                          const std::vector<int> &grouping,
                          const std::vector<int> &argIndexes,
                          const std::vector<std::string> &accTypes,
                          const std::vector<std::string> &aggValueTypes,
                          int indexOfCountStar);

json CreateDeduplicateConfig(const std::vector<std::string> &inputTypes,
                             const std::vector<int32_t> &grouping,
                             int rowtimeIndex,
                             bool keepLastRow,
                             bool generateUpdateBefore,
                             bool generateInsert);

json CreateJoinConfig(const std::string &joinType,
                      const std::vector<std::string> &leftInputTypes,
                      const std::vector<std::string> &rightInputTypes,
                      const std::vector<int> &leftKeys,
                      const std::vector<int> &rightKeys,
                      const std::vector<bool> &filterNulls);

json CreateRankConfig(const std::string &processFunction,
                      const std::vector<std::string> &inputTypes,
                      const std::vector<std::string> &outputTypes,
                      const std::vector<int> &partitionKey,
                      const std::vector<int> &sortFieldIndices,
                      const std::vector<bool> &sortAscendingOrders,
                      const std::vector<bool> &sortNullsIsLast,
                      bool outputRankNumber,
                      const std::string &rankRange,
                      bool generateUpdateBefore);

BinaryRowData *CreateBinaryRowDataFromLongs(const std::vector<int64_t> &values);

std::vector<omnistream::RowField> CreateRowFields(const std::vector<std::string> &types);

} // namespace DtFuzzFactoryUtil

#endif // OMNISTREAM_DT_FUZZ_FACTORY_UTIL_H
