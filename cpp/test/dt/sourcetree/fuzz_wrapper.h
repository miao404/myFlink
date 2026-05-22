/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Unified fuzz wrapper entry point for DTFrame registration.
 *              Covers all 19 table and streaming operator fuzz interfaces.
 */

#ifndef OMNISTREAM_FUZZ_WRAPPER_H
#define OMNISTREAM_FUZZ_WRAPPER_H

#include "dt_fuzz_data.h"

// Table operator fuzz functions (original 4)
int AggregateFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseAgg);
int DeduplicateFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseDedupMode);
int JoinFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseJoinType);
int RankFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseRankFunc);
int TableGlobalFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseFunc);

// Table operator fuzz functions (new 4)
int SinkFuzz(struct SinkFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int SourceFuzz(struct SourceFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int WindowFuzz(struct WindowFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int WatermarkAssignerFuzz(struct WatermarkAssignerFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);

// Streaming operator fuzz functions (original 3)
int KeyedProcessFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int CoProcessFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int TransformFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseTransform);
int StreamingGlobalFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseFunc);

// Streaming operator fuzz functions (new 8)
int ProcessFuzz(struct ProcessFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int CalcFuzz(struct CalcFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int ExpandFuzz(struct ExpandFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int FilterFuzz(struct FilterFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int FlatMapFuzz(struct FlatMapFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int GroupReduceFuzz(struct GroupReduceFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int MapFuzz(struct MapFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);
int SourceOperatorFuzz(struct SourceOperatorFuzzData fzd, uint16_t loopCount, uint16_t chooseMode);

#endif // OMNISTREAM_FUZZ_WRAPPER_H
