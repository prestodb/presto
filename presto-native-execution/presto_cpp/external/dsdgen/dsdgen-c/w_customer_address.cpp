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

#include "w_customer_address.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "nulls.h"
#include "porting.h"
#include "tables.h"
#include "tdefs.h"

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

  char szTemp[128];

  append_key(CA_ADDRESS_SK, info, r->ca_addr_sk);
  append_varchar(CA_ADDRESS_ID, info, r->ca_addr_id);
  append_varchar(
      CA_ADDRESS_STREET_NUM, info, std::to_string(r->ca_address.street_num));
  if (r->ca_address.street_name2) {
    snprintf(
        szTemp,
        sizeof(szTemp),
        "%s %s",
        r->ca_address.street_name1,
        r->ca_address.street_name2);
    append_varchar(CA_ADDRESS_STREET_NAME1, info, szTemp);
  } else
    append_varchar(CA_ADDRESS_STREET_NAME1, info, r->ca_address.street_name1);
  append_varchar(CA_ADDRESS_STREET_TYPE, info, r->ca_address.street_type);
  append_varchar(CA_ADDRESS_SUITE_NUM, info, &r->ca_address.suite_num[0]);
  append_varchar(CA_ADDRESS_CITY, info, r->ca_address.city);
  append_varchar(CA_ADDRESS_COUNTY, info, r->ca_address.county);
  append_varchar(CA_ADDRESS_STATE, info, r->ca_address.state);
  snprintf(szTemp, sizeof(szTemp), "%05d", r->ca_address.zip);
  append_varchar(CA_ADDRESS_ZIP, info, szTemp);
  append_varchar(CA_ADDRESS_COUNTRY, info, &r->ca_address.country[0]);
  append_integer_decimal(CA_ADDRESS_GMT_OFFSET, info, r->ca_address.gmt_offset);
  append_varchar(CA_LOCATION_TYPE, info, r->ca_location_type);

  append_row_end(info);

  return 0;
}
