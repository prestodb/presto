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

#ifndef DBGEN_VERSION_H
#define DBGEN_VERSION_H

#include "dist.h"

int mk_dbgen_version(
    void* pDest,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);
int pr_dbgen_version(void* pSrc);
int ld_dbgen_version(void* pSrc);
#endif
