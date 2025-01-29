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

#ifndef W_CATALOG_RETURNS_H
#define W_CATALOG_RETURNS_H

#include "dist.h"
#include "pricing.h"

int mk_w_catalog_returns(
    void* row,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);

#endif
