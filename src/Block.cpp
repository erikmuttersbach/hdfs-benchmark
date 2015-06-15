//
// Created by Erik Muttersbach on 20/05/15.
//

#include "Block.h"

Block::Block(uint32_t idx, string host, void *data, size_t len) : idx(idx), host(host), data(data), len(len) {
}

Block::Block(uint32_t idx, set<string> hosts) : idx(idx), hosts(hosts) {
}