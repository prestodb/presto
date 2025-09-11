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

#include "velox/tpcds/gen/dsdgen/include/w_catalog_page.h"

#include "velox/tpcds/gen/dsdgen/include/append_info.h"
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/constants.h"
#include "velox/tpcds/gen/dsdgen/include/date.h"
#include "velox/tpcds/gen/dsdgen/include/decimal.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/misc.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/scaling.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

#include <stdio.h>

/*
 * Routine: mk_catalog_page()
 * Purpose: populate the catalog_page table
 * Algorithm:
 *	catalogs are issued either monthly, quarterly or bi-annually (cp_type)
 *	there is 1 of each type circulating at all times
 * Data tdefsures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 * 20020903 jms cp_department needs to be randomized
 * 20020903 jms cp_description needs to be randomized
 */
int mk_w_catalog_page(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  date_t dStartDate;
  int nCatalogPageMax;
  int nDuration, nOffset, nType;
  struct CATALOG_PAGE_TBL* r;
  int nCatalogInterval;
  tdef* pTdef = getSimpleTdefsByNumber(CATALOG_PAGE, dsdGenContext);

  r = &dsdGenContext.g_w_catalog_page;

  nCatalogPageMax =
      (static_cast<int>(
          get_rowcount(CATALOG_PAGE, dsdGenContext) / CP_CATALOGS_PER_YEAR)) /
      (YEAR_MAXIMUM - YEAR_MINIMUM + 2);
  dStartDate = strtodate(DATA_START_DATE);
  strcpy(r->cp_department, "DEPARTMENT");

  nullSet(&pTdef->kNullBitMap, CP_NULLS, dsdGenContext);
  r->cp_catalog_page_sk = index;
  mk_bkey(&r->cp_catalog_page_id[0], index, CP_CATALOG_PAGE_ID);
  r->cp_catalog_number = static_cast<long>((index - 1) / nCatalogPageMax + 1);
  r->cp_catalog_page_number =
      static_cast<long>((index - 1) % nCatalogPageMax + 1);
  switch (nCatalogInterval =
              ((r->cp_catalog_number - 1) % CP_CATALOGS_PER_YEAR)) {
    case 0: /* bi-annual */
    case 1:
      nType = 1;
      nDuration = 182;
      nOffset = nCatalogInterval * nDuration;
      break;
    case 2:
    case 3: /* Q2 */
    case 4: /* Q3 */
    case 5: /* Q4 */
      nDuration = 91;
      nOffset = (nCatalogInterval - 2) * nDuration;
      nType = 2;
      break;
    default:
      nDuration = 30;
      nOffset = (nCatalogInterval - 6) * nDuration;
      nType = 3; /* monthly */
  }
  r->cp_start_date_id = dStartDate.julian + nOffset;
  r->cp_start_date_id +=
      ((r->cp_catalog_number - 1) / CP_CATALOGS_PER_YEAR) * 365;
  r->cp_end_date_id = r->cp_start_date_id + nDuration - 1;
  dist_member(&r->cp_type, "catalog_page_type", nType, 1, dsdGenContext);
  gen_text(
      &r->cp_description[0],
      RS_CP_DESCRIPTION / 2,
      RS_CP_DESCRIPTION - 1,
      CP_DESCRIPTION,
      dsdGenContext);

  void* info = append_info_get(info_arr, CATALOG_PAGE);

  append_row_start(info);

  append_key(CP_CATALOG_PAGE_SK, info, r->cp_catalog_page_sk);
  append_varchar(CP_CATALOG_PAGE_ID, info, r->cp_catalog_page_id);
  append_integer(CP_START_DATE_ID, info, r->cp_start_date_id);
  append_integer(CP_END_DATE_ID, info, r->cp_end_date_id);
  append_varchar(CP_DEPARTMENT, info, &r->cp_department[0]);
  append_integer(CP_CATALOG_NUMBER, info, r->cp_catalog_number);
  append_integer(CP_CATALOG_PAGE_NUMBER, info, r->cp_catalog_page_number);
  append_varchar(CP_DESCRIPTION, info, &r->cp_description[0]);
  append_varchar(CP_TYPE, info, r->cp_type);

  append_row_end(info);

  return 0;
}
