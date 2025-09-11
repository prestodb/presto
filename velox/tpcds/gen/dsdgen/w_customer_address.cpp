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

#include "velox/tpcds/gen/dsdgen/include/w_customer_address.h"

#include "velox/tpcds/gen/dsdgen/include/append_info.h"
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/date.h"
#include "velox/tpcds/gen/dsdgen/include/decimal.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

#include <stdio.h>

/*
 * mk_customer_address
 */
int mk_w_customer_address(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  /* begin locals declarations */
  struct W_CUSTOMER_ADDRESS_TBL* r;
  tdef* pTdef = getSimpleTdefsByNumber(CUSTOMER_ADDRESS, dsdGenContext);

  r = &dsdGenContext.g_w_customer_address;

  nullSet(&pTdef->kNullBitMap, CA_NULLS, dsdGenContext);
  r->ca_addr_sk = index;
  mk_bkey(&r->ca_addr_id[0], index, CA_ADDRESS_ID);
  pick_distribution(
      &r->ca_location_type,
      "location_type",
      1,
      1,
      CA_LOCATION_TYPE,
      dsdGenContext);
  mk_address(&r->ca_address, CA_ADDRESS, dsdGenContext);

  void* info = append_info_get(info_arr, CUSTOMER_ADDRESS);
  append_row_start(info);

  std::vector<char> szTemp(128);

  append_key(CA_ADDRESS_SK, info, r->ca_addr_sk);
  append_varchar(CA_ADDRESS_ID, info, r->ca_addr_id);
  append_varchar(
      CA_ADDRESS_STREET_NUM, info, std::to_string(r->ca_address.street_num));
  if (r->ca_address.street_name2) {
    snprintf(
        szTemp.data(),
        szTemp.size(),
        "%s %s",
        r->ca_address.street_name1,
        r->ca_address.street_name2);
    append_varchar(CA_ADDRESS_STREET_NAME1, info, szTemp.data());
  } else
    append_varchar(CA_ADDRESS_STREET_NAME1, info, r->ca_address.street_name1);
  append_varchar(CA_ADDRESS_STREET_TYPE, info, r->ca_address.street_type);
  append_varchar(CA_ADDRESS_SUITE_NUM, info, &r->ca_address.suite_num[0]);
  append_varchar(CA_ADDRESS_CITY, info, r->ca_address.city);
  append_varchar(CA_ADDRESS_COUNTY, info, r->ca_address.county);
  append_varchar(CA_ADDRESS_STATE, info, r->ca_address.state);
  snprintf(szTemp.data(), szTemp.size(), "%05d", r->ca_address.zip);
  append_varchar(CA_ADDRESS_ZIP, info, szTemp.data());
  append_varchar(CA_ADDRESS_COUNTRY, info, &r->ca_address.country[0]);
  append_integer_decimal(CA_ADDRESS_GMT_OFFSET, info, r->ca_address.gmt_offset);
  append_varchar(CA_LOCATION_TYPE, info, r->ca_location_type);

  append_row_end(info);

  return 0;
}
