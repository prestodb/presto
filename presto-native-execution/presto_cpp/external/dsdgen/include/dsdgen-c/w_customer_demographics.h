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

#ifndef W_CUSTOMER_DEMOGRAPHICS_H
#define W_CUSTOMER_DEMOGRAPHICS_H

#include "dist.h"
#include "porting.h"

/***
*** CD_xxx Customer Demographcis Defines
***/
#define CD_MAX_CHILDREN 7
#define CD_MAX_EMPLOYED 7
#define CD_MAX_COLLEGE 7

int mk_w_customer_demographics(
    void* info_arr,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);

#endif
