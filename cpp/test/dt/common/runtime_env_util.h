/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Runtime environment initialization utilities for DT fuzz testing
 */

#ifndef OMNISTREAM_DT_RUNTIME_ENV_UTIL_H
#define OMNISTREAM_DT_RUNTIME_ENV_UTIL_H

#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/TaskStateManager.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/typeutils/RowDataSerializer.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include <string>
#include <vector>
#include <memory>

namespace DtRuntimeEnvUtil {

struct RuntimeEnvContext {
    omnistream::RuntimeEnvironmentV2 *env = nullptr;
    TaskInformationPOD *taskInfo = nullptr;
    StreamTaskStateInitializerImpl *initializer = nullptr;
    TypeSerializer *serializer = nullptr;

    ~RuntimeEnvContext()
    {
        delete env;
        delete taskInfo;
        delete initializer;
        delete serializer;
    }
};

RuntimeEnvContext *CreateRuntimeEnv(const std::string &stateBackend,
                                   const std::vector<omnistream::RowField> &rowFields);

RuntimeEnvContext *CreateRuntimeEnvWithOperatorId(const std::string &stateBackend,
                                                  const std::vector<omnistream::RowField> &rowFields,
                                                  const std::string &operatorId);

void InitializeOperatorState(KeyedProcessOperator<RowData *, RowData *, RowData *> &op,
                             RuntimeEnvContext *ctx);

} // namespace DtRuntimeEnvUtil

#endif // OMNISTREAM_DT_RUNTIME_ENV_UTIL_H
