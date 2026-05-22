/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Unified fuzz wrapper entry point for DTFrame registration.
 *              Combines table operator and streaming operator fuzz interfaces.
 */

#ifndef OMNISTREAM_FUZZ_WRAPPER_H
#define OMNISTREAM_FUZZ_WRAPPER_H

#include "dt_fuzz_data.h"

// Table operator fuzz functions
int AggregateFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseAgg);
int DeduplicateFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseDedupMode);
int JoinFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseJoinType);
int RankFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseRankFunc);
int TableGlobalFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseFunc);

// Streaming operator fuzz functions
int KeyedProcessFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int CoProcessFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int TransformFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseTransform);
int StreamingGlobalFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseFunc);

#endif // OMNISTREAM_FUZZ_WRAPPER_H
