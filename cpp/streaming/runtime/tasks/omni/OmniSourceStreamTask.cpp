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
#include "../../../../core/include/common.h"

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

void OmniSourceStreamTask::init()
{
    OmniStreamTask::init();
}

void OmniSourceStreamTask::processInput(MailboxDefaultAction::Controller *controller)
{
    INFO_RELEASE("OmniSourceStreamTask::processInput - starting source thread, task=" << taskName_);
    LOG("OmniSourceStreamTask::processInput")
    controller->suspendDefaultAction();  // 暂停默认action

    sourceRunning_ = true;
    sourceThread_ = std::make_unique<std::thread>([this]() {
        INFO_RELEASE("OmniSourceStreamTask::sourceThread - started, task=" << taskName_);
        runSourceInThread();
        INFO_RELEASE("OmniSourceStreamTask::sourceThread - finished, task=" << taskName_);
    });
    INFO_RELEASE("OmniSourceStreamTask::processInput - source thread started, returning to mailbox loop");
}

void OmniSourceStreamTask::runSourceInThread()
{
    try {
        INFO_RELEASE("OmniSourceStreamTask::runSourceInThread - calling StreamSource::run");
        if (!dynamic_cast<StreamSource<omnistream::VectorBatch> *>(mainOperator_)) {
            throw std::runtime_error("mainOperator_ is not of type StreamSource<omnistream::VectorBatch>");
        }

        // 调用带lock参数的run方法
        dynamic_cast<StreamSource<omnistream::VectorBatch> *>(mainOperator_)->run(lockObject_);

        CompleteProcessing();

        INFO_RELEASE("OmniSourceStreamTask::runSourceInThread - sending completion mail");
        // 完成后通过mailbox通知主线程
        auto completionMail = std::make_shared<VoidFunctionRunnable>([this]() {
            sourceRunning_ = false;
            mailboxProcessor_->suspend();
            LOG_INFO_IMP("Task : " << taskName_ << " suspended");
        });
        mainMailboxExecutor_->execute(completionMail, "Source completion");
    } catch (const std::exception& e) {
        auto errorMail = std::make_shared<VoidFunctionRunnable>([this, e]() {
            sourceRunning_ = false;
            mailboxProcessor_->reportThrowable(std::make_exception_ptr(e));
        });
        mainMailboxExecutor_->execute(errorMail, "Source error");
    }
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
    INFO_RELEASE("OmniSourceStreamTask::cancel() called")
    sourceRunning_ = false;
    if (mainOperator_) {
        auto source = dynamic_cast<StreamSource<omnistream::VectorBatch> *>(mainOperator_);
        if (source) {
            INFO_RELEASE("OmniSourceStreamTask::cancel() called: source->cancel()")
            source->cancel();
        }
    }

    if (sourceThread_ && sourceThread_->joinable()) {
        INFO_RELEASE("OmniSourceStreamTask::cancel() called: sourceThread_->join()")
        sourceThread_->join();
    }
    OmniStreamTask::cancel();
    // avoid back pressure
    recordWriter_->cancel();
}

OmniSourceStreamTask::~OmniSourceStreamTask()
{
    INFO_RELEASE("~OmniSourceStreamTask called")
    if (sourceThread_ && sourceThread_->joinable()) {
        INFO_RELEASE("~OmniSourceStreamTask called sourceThread_->join()")
        sourceThread_->join();
        INFO_RELEASE("~OmniSourceStreamTask called sourceThread_->join() finish")
    }
    if (lockObject_) {
        INFO_RELEASE("~OmniSourceStreamTask called delete lockObject_")
        delete lockObject_;
        INFO_RELEASE("~OmniSourceStreamTask called delete lockObject_ finish")
    }
}

}
