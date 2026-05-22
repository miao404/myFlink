/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Description: Fuzz wrapper implementation dispatching to individual streaming operator fuzz tests
 */

#include "streaming_fuzz_wrapper.h"
#include <iostream>

int StreamingGlobalFuzz(struct StreamingFuzzData fzd, uint16_t loopCount, uint16_t chooseFunc)
{
    uint16_t funcChoice = chooseFunc % 3;
    int result = 0;

    switch (funcChoice) {
        case 0:
            result = KeyedProcessFuzz(fzd, loopCount, fzd.intValue % 3);
            break;
        case 1:
            result = CoProcessFuzz(fzd, loopCount, fzd.intValue % 2);
            break;
        case 2:
            result = TransformFuzz(fzd, loopCount, fzd.intValue % 3);
            break;
        default:
            break;
    }

    return result;
}
