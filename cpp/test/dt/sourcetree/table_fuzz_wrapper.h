/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz wrapper interface for OmniStream table operators
 */

#ifndef OMNISTREAM_TABLE_FUZZ_WRAPPER_H
#define OMNISTREAM_TABLE_FUZZ_WRAPPER_H

#include <iostream>
#include "dt_fuzz_data.h"

int AggregateFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseAgg);
int DeduplicateFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseDedupMode);
int JoinFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseJoinType);
int RankFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseRankFunc);

int TableGlobalFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseFunc);

#endif // OMNISTREAM_TABLE_FUZZ_WRAPPER_H
