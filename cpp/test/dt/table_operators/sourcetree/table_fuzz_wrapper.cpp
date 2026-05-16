/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz wrapper implementation dispatching to individual table operator fuzz tests
 */

#include "table_fuzz_wrapper.h"
#include <iostream>

int TableGlobalFuzz(struct TableFuzzData fzd, uint16_t loopCount, uint16_t chooseFunc)
{
    uint16_t funcChoice = chooseFunc % 4;
    int result = 0;

    switch (funcChoice) {
        case 0:
            result = AggregateFuzz(fzd, loopCount, fzd.intValue % 5);
            break;
        case 1:
            result = DeduplicateFuzz(fzd, loopCount, fzd.intValue % 2);
            break;
        case 2:
            result = JoinFuzz(fzd, loopCount, fzd.intValue % 2);
            break;
        case 3:
            result = RankFuzz(fzd, loopCount, fzd.intValue % 2);
            break;
        default:
            break;
    }

    return result;
}
