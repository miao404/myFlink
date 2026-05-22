/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz wrapper interface for OmniStream streaming (DataStream) operators
 */

#ifndef OMNISTREAM_STREAMING_FUZZ_WRAPPER_H
#define OMNISTREAM_STREAMING_FUZZ_WRAPPER_H

#include <iostream>
#include "dt_fuzz_data.h"

int KeyedProcessFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int CoProcessFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int TransformFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseTransform);

int StreamingGlobalFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseFunc);

#endif // OMNISTREAM_STREAMING_FUZZ_WRAPPER_H
