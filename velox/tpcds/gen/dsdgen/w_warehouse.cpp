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

#include "velox/tpcds/gen/dsdgen/include/w_warehouse.h"

#include "velox/tpcds/gen/dsdgen/include/address.h"
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
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

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

  std::vector<char> szTemp(128);

  void* info = append_info_get(info_arr, WAREHOUSE);
  append_row_start(info);

  append_key(W_WAREHOUSE_SK, info, r->w_warehouse_sk);
  append_varchar(W_WAREHOUSE_ID, info, r->w_warehouse_id);
  append_varchar(W_WAREHOUSE_NAME, info, &r->w_warehouse_name[0]);
  append_integer(W_WAREHOUSE_SQ_FT, info, r->w_warehouse_sq_ft);
  append_varchar(
      W_ADDRESS_STREET_NUM, info, std::to_string(r->w_address.street_num));
  if (r->w_address.street_name2 != NULL) {
    snprintf(
        szTemp.data(),
        szTemp.size(),
        "%s %s",
        r->w_address.street_name1,
        r->w_address.street_name2);
    append_varchar(W_ADDRESS_STREET_NAME1, info, szTemp.data());
  } else
    append_varchar(W_ADDRESS_STREET_NAME1, info, r->w_address.street_name1);
  append_varchar(W_ADDRESS_STREET_TYPE, info, r->w_address.street_type);
  append_varchar(W_ADDRESS_SUITE_NUM, info, r->w_address.suite_num);
  append_varchar(W_ADDRESS_CITY, info, r->w_address.city);
  append_varchar(W_ADDRESS_COUNTY, info, r->w_address.county);
  append_varchar(W_ADDRESS_STATE, info, r->w_address.state);
  snprintf(szTemp.data(), szTemp.size(), "%05d", r->w_address.zip);
  append_varchar(W_ADDRESS_ZIP, info, szTemp.data());
  append_varchar(W_ADDRESS_COUNTRY, info, r->w_address.country);
  append_integer_decimal(W_ADDRESS_GMT_OFFSET, info, r->w_address.gmt_offset);

  append_row_end(info);

  return 0;
}
