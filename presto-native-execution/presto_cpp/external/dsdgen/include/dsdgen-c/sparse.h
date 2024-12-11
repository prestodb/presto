/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under external/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#include "dist.h"

int initSparseKeys(
    int nTable,
    DSDGenContext& dsdGenContext); /* populate the set of valid keys */
ds_key_t randomSparseKey(
    int nTable,
    int nColumn,
    DSDGenContext& dsdGenContext); /* select a random sparse key */
