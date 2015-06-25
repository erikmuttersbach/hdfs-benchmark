//
// Created by Erik Muttersbach on 20/05/15.
//

#include "Compare.h"

bool Compare::operator()(Block block1, Block block2) {
    return block1.idx > block2.idx;
}
