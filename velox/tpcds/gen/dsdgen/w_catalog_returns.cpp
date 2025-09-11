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

#include "velox/tpcds/gen/dsdgen/include/w_catalog_returns.h"
#include <stdio.h>
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/constants.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/parallel.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"
#include "velox/tpcds/gen/dsdgen/include/w_catalog_sales.h"

/*
 * Routine: mk_catalog_returns()
 * Purpose: populate a return fact *sync'd with a sales fact*
 * Algorithm: Since the returns need to be in line with a prior sale, they need
 *	to use the output of the mk_catalog_sales() routine, and then add
 *return-related information Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 * 20020902 jms Need to link call center to date/time of return
 * 20031023 jms removed ability for stand alone generation
 */
int mk_w_catalog_returns(
    void* row,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  int res = 0;

  decimal_t dHundred;
  int nTemp;
  struct W_CATALOG_RETURNS_TBL* r;
  struct W_CATALOG_SALES_TBL* sale = &dsdGenContext.g_w_catalog_sales;
  static int bStandAlone = 0;
  tdef* pTdef = getSimpleTdefsByNumber(CATALOG_RETURNS, dsdGenContext);

  if (row == NULL)
    r = &dsdGenContext.g_w_catalog_returns;
  else
    r = static_cast<W_CATALOG_RETURNS_TBL*>(row);
  strtodec(&dHundred, "100.00");

  /* if we were not called from the parent table's mk_xxx routine, then
   * move to a parent row that needs to be returned, and generate it
   */
  nullSet(&pTdef->kNullBitMap, CR_NULLS, dsdGenContext);
  if (bStandAlone) {
    genrand_integer(
        &nTemp, DIST_UNIFORM, 0, 99, 0, CR_IS_RETURNED, dsdGenContext);
    if (nTemp >= CR_RETURN_PCT) {
      row_skip(CATALOG_SALES, 1, dsdGenContext);
      return (1);
    }
    mk_w_catalog_sales(&dsdGenContext.g_w_catalog_sales, index, dsdGenContext);
  }

  /*
   * Some of the information in the return is taken from the original sale
   * which has been regenerated
   */
  r->cr_item_sk = sale->cs_sold_item_sk;
  r->cr_catalog_page_sk = sale->cs_catalog_page_sk;
  r->cr_order_number = sale->cs_order_number;
  memcpy(&r->cr_pricing, &sale->cs_pricing, sizeof(ds_pricing_t));
  r->cr_refunded_customer_sk = sale->cs_bill_customer_sk;
  r->cr_refunded_cdemo_sk = sale->cs_bill_cdemo_sk;
  r->cr_refunded_hdemo_sk = sale->cs_bill_hdemo_sk;
  r->cr_refunded_addr_sk = sale->cs_bill_addr_sk;
  r->cr_call_center_sk = sale->cs_call_center_sk;

  /*
   * some of the fields are conditionally taken from the sale
   */
  r->cr_returning_customer_sk =
      mk_join(CR_RETURNING_CUSTOMER_SK, CUSTOMER, 2, dsdGenContext);
  r->cr_returning_cdemo_sk =
      mk_join(CR_RETURNING_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 2, dsdGenContext);
  r->cr_returning_hdemo_sk =
      mk_join(CR_RETURNING_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 2, dsdGenContext);
  r->cr_returning_addr_sk =
      mk_join(CR_RETURNING_ADDR_SK, CUSTOMER_ADDRESS, 2, dsdGenContext);
  if (genrand_integer(
          NULL,
          DIST_UNIFORM,
          0,
          99,
          0,
          CR_RETURNING_CUSTOMER_SK,
          dsdGenContext) < CS_GIFT_PCT) {
    r->cr_returning_customer_sk = sale->cs_ship_customer_sk;
    r->cr_returning_cdemo_sk = sale->cs_ship_cdemo_sk;
    /* cr_returning_hdemo_sk removed, since it doesn't exist on the sales
     * record */
    r->cr_returning_addr_sk = sale->cs_ship_addr_sk;
  }

  /**
   * the rest of the columns are generated for this specific return
   */
  /* the items cannot be returned until they are shipped; offset is handled in
   * mk_join, based on sales date */
  r->cr_returned_date_sk =
      mk_join(CR_RETURNED_DATE_SK, DATET, sale->cs_ship_date_sk, dsdGenContext);

  /* the call center determines the time of the return */
  r->cr_returned_time_sk =
      mk_join(CR_RETURNED_TIME_SK, TIMET, 1, dsdGenContext);

  r->cr_ship_mode_sk = mk_join(CR_SHIP_MODE_SK, SHIP_MODE, 1, dsdGenContext);
  r->cr_warehouse_sk = mk_join(CR_WAREHOUSE_SK, WAREHOUSE, 1, dsdGenContext);
  r->cr_reason_sk = mk_join(CR_REASON_SK, REASON, 1, dsdGenContext);
  if (sale->cs_pricing.quantity != -1)
    genrand_integer(
        &r->cr_pricing.quantity,
        DIST_UNIFORM,
        1,
        sale->cs_pricing.quantity,
        0,
        CR_PRICING,
        dsdGenContext);
  else
    r->cr_pricing.quantity = -1;
  set_pricing(CR_PRICING, &r->cr_pricing, dsdGenContext);

  return (res);
}
