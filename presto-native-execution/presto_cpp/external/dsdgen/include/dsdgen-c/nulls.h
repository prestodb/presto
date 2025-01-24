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

#ifndef R_NULLCHECK_H
#define R_NULLCHECK_H

#include "dist.h"

int nullCheck(int nColumn, DSDGenContext& dsdGenContext);
void nullSet(ds_key_t* pDest, int nStream, DSDGenContext& dsdGenContext);

#endif
