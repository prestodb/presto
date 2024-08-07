/*
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
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors:
 * Gradient Systems
 */
#include "w_warehouse.h"

#include "address.h"
#include "append_info.h"
#include "build_support.h"
#include "config.h"
#include "constants.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "misc.h"
#include "nulls.h"
#include "porting.h"
#include "tables.h"
#include "tdefs.h"

#include <stdio.h>

/*
 * mk_warehouse
 */
int mk_w_warehouse(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  /* begin locals declarations */
  struct W_WAREHOUSE_TBL* r;
  tdef* pT = getSimpleTdefsByNumber(WAREHOUSE, dsdGenContext);

  r = &dsdGenContext.g_w_warehouse;

  nullSet(&pT->kNullBitMap, W_NULLS, dsdGenContext);
  r->w_warehouse_sk = index;
  mk_bkey(&r->w_warehouse_id[0], index, W_WAREHOUSE_ID);
  gen_text(
      &r->w_warehouse_name[0],
      W_NAME_MIN,
      RS_W_WAREHOUSE_NAME,
      W_WAREHOUSE_NAME,
      dsdGenContext);
  r->w_warehouse_sq_ft = genrand_integer(
      NULL,
      DIST_UNIFORM,
      W_SQFT_MIN,
      W_SQFT_MAX,
      0,
      W_WAREHOUSE_SQ_FT,
      dsdGenContext);

  mk_address(&r->w_address, W_WAREHOUSE_ADDRESS, dsdGenContext);

  char szTemp[128];

  void* info = append_info_get(info_arr, WAREHOUSE);
  append_row_start(info);

  append_key(W_WAREHOUSE_SK, info, r->w_warehouse_sk);
  append_varchar(W_WAREHOUSE_ID, info, r->w_warehouse_id);
  append_varchar(W_WAREHOUSE_NAME, info, &r->w_warehouse_name[0]);
  append_integer(W_WAREHOUSE_SQ_FT, info, r->w_warehouse_sq_ft);
  append_varchar(
      W_ADDRESS_STREET_NUM, info, std::to_string(r->w_address.street_num));
  if (r->w_address.street_name2 != NULL) {
    sprintf(
        szTemp, "%s %s", r->w_address.street_name1, r->w_address.street_name2);
    append_varchar(W_ADDRESS_STREET_NAME1, info, szTemp);
  } else
    append_varchar(W_ADDRESS_STREET_NAME1, info, r->w_address.street_name1);
  append_varchar(W_ADDRESS_STREET_TYPE, info, r->w_address.street_type);
  append_varchar(W_ADDRESS_SUITE_NUM, info, r->w_address.suite_num);
  append_varchar(W_ADDRESS_CITY, info, r->w_address.city);
  append_varchar(W_ADDRESS_COUNTY, info, r->w_address.county);
  append_varchar(W_ADDRESS_STATE, info, r->w_address.state);
  sprintf(szTemp, "%05d", r->w_address.zip);
  append_varchar(W_ADDRESS_ZIP, info, szTemp);
  append_varchar(W_ADDRESS_COUNTRY, info, r->w_address.country);
  append_integer_decimal(W_ADDRESS_GMT_OFFSET, info, r->w_address.gmt_offset);

  append_row_end(info);

  return 0;
}
