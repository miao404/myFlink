/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Runtime environment initialization utilities implementation
 */

#include "runtime_env_util.h"

namespace DtRuntimeEnvUtil {

RuntimeEnvContext *CreateRuntimeEnv(const std::string &stateBackend,
                                   const std::vector<omnistream::RowField> &rowFields)
{
    return CreateRuntimeEnvWithOperatorId(stateBackend, rowFields, "deadbeefdeadbeefdeadbeefdeadbeef");
}

RuntimeEnvContext *CreateRuntimeEnvWithOperatorId(const std::string &stateBackend,
                                                  const std::vector<omnistream::RowField> &rowFields,
                                                  const std::string &operatorId)
{
    auto *ctx = new RuntimeEnvContext();

    ctx->env = new omnistream::RuntimeEnvironmentV2();
    ctx->taskInfo = new TaskInformationPOD();
    ctx->taskInfo->setStateBackend(stateBackend);

    {
        auto configPOD = ctx->taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId(operatorId);
        configPOD.setOperatorDescription(operatorDesc);
        ctx->taskInfo->setStreamConfigPOD(configPOD);
    }

    ctx->env->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    ctx->env->setTaskConfiguration(*(ctx->taskInfo));

    ctx->initializer = new StreamTaskStateInitializerImpl(ctx->env);

    auto *typeInfo = new std::vector<omnistream::RowField>(rowFields);
    ctx->serializer = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    return ctx;
}

void InitializeOperatorState(KeyedProcessOperator<RowData *, RowData *, RowData *> &op,
                             RuntimeEnvContext *ctx)
{
    op.initializeState(ctx->initializer, ctx->serializer);
}

} // namespace DtRuntimeEnvUtil
