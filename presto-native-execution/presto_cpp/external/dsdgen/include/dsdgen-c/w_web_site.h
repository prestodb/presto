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

#ifndef W_WEB_SITE_H
#define W_WEB_SITE_H

#include "address.h"
#include "constants.h"
#include "decimal.h"

#define WEB_MIN_TAX_PERCENTAGE "0.00"
#define WEB_MAX_TAX_PERCENTAGE "0.12"

int mk_w_web_site(
    void* info_arr,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);
#endif
