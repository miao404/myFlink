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

#include "OmniSourceStreamTask.h"

#include "common.h"

namespace omnistream {

    StopMode FinishingReasonToStopMode(FinishingReason reason)
    {
        switch (reason) {
            case FinishingReason::END_OF_DATA:
                return StopMode::DRAIN;
            case FinishingReason::STOP_WITH_SAVEPOINT_DRAIN:
                return StopMode::DRAIN;
            case FinishingReason::STOP_WITH_SAVEPOINT_NO_DRAIN:
                return StopMode::NO_DRAIN;
            default:
                return StopMode::DRAIN;
        }
    }

    // Optional: For easier debugging or logging
    std::string FinishingReasonToString(FinishingReason reason)
    {
        switch (reason) {
            case FinishingReason::END_OF_DATA:
                return "END_OF_DATA";
            case FinishingReason::STOP_WITH_SAVEPOINT_DRAIN:
                return "STOP_WITH_SAVEPOINT_DRAIN";
            case FinishingReason::STOP_WITH_SAVEPOINT_NO_DRAIN:
                return "STOP_WITH_SAVEPOINT_NO_DRAIN";
            default:
                return "UNKNOWN_FINISHING_REASON";
        }
    }

OmniSourceStreamTask::OmniSourceStreamTask(std::shared_ptr<RuntimeEnvironmentV2> &env, int taskType)
    : OmniSourceStreamTask(env, taskType, new Object()) {}

OmniSourceStreamTask::OmniSourceStreamTask(std::shared_ptr<RuntimeEnvironmentV2> &env, int taskType, Object* lock)
    : OmniStreamTask(env, synchronizedExecutor(&lock->mutex), taskType),
      lock_(lock) {}

OmniSourceStreamTask::~OmniSourceStreamTask()
{
    if (sourceThread_ && sourceThread_->joinable()) {
        sourceThread_->join();
    }
    delete lock_;
}

void OmniSourceStreamTask::init()
{
    OmniStreamTask::init();
}

void OmniSourceStreamTask::processInput(MailboxDefaultAction::Controller *controller)
{
    LOG("OmniSourceStreamTask::processInput")

    auto* source = dynamic_cast<StreamSource<omnistream::VectorBatch> *>(mainOperator_);
    if (!source) {
        throw std::runtime_error("mainOperator_ is not of type StreamSource<omnistream::VectorBatch>");
    }

    controller->suspendDefaultAction();

    sourceThread_ = std::make_unique<std::thread>([this, source]() {
        try {
            source->run(lock_);

            auto completionMail = std::make_shared<VoidFunctionRunnable>([this]() {
                CompleteProcessing();
                mailboxProcessor_->suspend();
                LOG_INFO_IMP("Task : " << taskName_ << " suspended");
            });
            mainMailboxExecutor_->execute(completionMail, "source completion");
        } catch (const std::exception& e) {
            LOG("Source thread exception: " << e.what())
            mailboxProcessor_->reportThrowable(std::current_exception());
        }
    });
}

void OmniSourceStreamTask::CompleteProcessing()
{
    // so we need to call it here
    auto stopMode = FinishingReasonToStopMode(finishingReason);
    if (stopMode == StopMode::DRAIN) {
       // reserved for future bound input
    }
    EndData(stopMode);
}

void OmniSourceStreamTask::AdvanceToEndOfEventTime()
{
    operatorChain->GetMainOperatorOutput()->emitWatermark(new Watermark(LONG_MAX));
}

const std::string OmniSourceStreamTask::getName() const
{
    return std::string("OmniSourceStreamTask");
}

void OmniSourceStreamTask::cancel()
{
    OmniStreamTask::cancel();
    if (sourceThread_ && sourceThread_->joinable()) {
        sourceThread_->join();
    }
    // avoid back pressure
    recordWriter_->cancel();
}
}
