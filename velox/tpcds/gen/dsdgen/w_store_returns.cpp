/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under tpcds/gen/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#include "velox/tpcds/gen/dsdgen/include/w_store_returns.h"
#include <stdio.h>
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/parallel.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/pricing.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"
#include "velox/tpcds/gen/dsdgen/include/w_store_sales.h"

/*
 * Routine: mk_store_returns()
 * Purpose: populate a return fact *sync'd with a sales fact*
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int mk_w_store_returns(
    void* row,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  int res = 0, nTemp;
  struct W_STORE_RETURNS_TBL* r;
  struct W_STORE_SALES_TBL* sale = &dsdGenContext.g_w_store_sales;
  tdef* pT = getSimpleTdefsByNumber(STORE_RETURNS, dsdGenContext);

  decimal_t dMin, dMax;
  /* begin locals declarations */
  if (row == NULL)
    r = &dsdGenContext.g_w_store_returns;
  else
    r = static_cast<W_STORE_RETURNS_TBL*>(row);

  strtodec(&dMin, "1.00");
  strtodec(&dMax, "100000.00");

  nullSet(&pT->kNullBitMap, SR_NULLS, dsdGenContext);
  /*
   * Some of the information in the return is taken from the original sale
   * which has been regenerated
   */
  r->sr_ticket_number = sale->ss_ticket_number;
  r->sr_item_sk = sale->ss_sold_item_sk;
  memcpy(
      static_cast<void*>(&r->sr_pricing),
      static_cast<void*>(&sale->ss_pricing),
      sizeof(ds_pricing_t));

  /*
   * some of the fields are conditionally taken from the sale
   */
  r->sr_customer_sk = mk_join(SR_CUSTOMER_SK, CUSTOMER, 1, dsdGenContext);
  if (genrand_integer(
          NULL, DIST_UNIFORM, 1, 100, 0, SR_TICKET_NUMBER, dsdGenContext) <
      SR_SAME_CUSTOMER)
    r->sr_customer_sk = sale->ss_sold_customer_sk;

  /*
   * the rest of the columns are generated for this specific return
   */
  /* the items cannot be returned until they are sold; offset is handled in
   * mk_join, based on sales date */
  r->sr_returned_date_sk =
      mk_join(SR_RETURNED_DATE_SK, DATET, sale->ss_sold_date_sk, dsdGenContext);
  genrand_integer(
      &nTemp,
      DIST_UNIFORM,
      (8 * 3600) - 1,
      (17 * 3600) - 1,
      0,
      SR_RETURNED_TIME_SK,
      dsdGenContext);
  r->sr_returned_time_sk = nTemp;
  r->sr_cdemo_sk =
      mk_join(SR_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1, dsdGenContext);
  r->sr_hdemo_sk =
      mk_join(SR_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1, dsdGenContext);
  r->sr_addr_sk = mk_join(SR_ADDR_SK, CUSTOMER_ADDRESS, 1, dsdGenContext);
  r->sr_store_sk = mk_join(SR_STORE_SK, STORE, 1, dsdGenContext);
  r->sr_reason_sk = mk_join(SR_REASON_SK, REASON, 1, dsdGenContext);
  genrand_integer(
      &r->sr_pricing.quantity,
      DIST_UNIFORM,
      1,
      sale->ss_pricing.quantity,
      0,
      SR_PRICING,
      dsdGenContext);
  set_pricing(SR_PRICING, &r->sr_pricing, dsdGenContext);

  return (res);
}
