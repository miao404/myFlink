/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz wrapper interface for OmniStream streaming (DataStream) operators (11 total)
 */

#ifndef OMNISTREAM_STREAMING_FUZZ_WRAPPER_H
#define OMNISTREAM_STREAMING_FUZZ_WRAPPER_H

#include <iostream>
#include "dt_fuzz_data.h"

// Original streaming operators
int KeyedProcessFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int CoProcessFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int TransformFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseTransform);

// New streaming operators
int ProcessFuzz(struct ProcessFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int CalcFuzz(struct CalcFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int ExpandFuzz(struct ExpandFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int FilterFuzz(struct FilterFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int FlatMapFuzz(struct FlatMapFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int GroupReduceFuzz(struct GroupReduceFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int MapFuzz(struct MapFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int SourceOperatorFuzz(struct SourceOperatorFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);

int StreamingGlobalFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseFunc);

#endif // OMNISTREAM_STREAMING_FUZZ_WRAPPER_H
