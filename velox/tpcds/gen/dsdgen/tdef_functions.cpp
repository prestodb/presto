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

#include "velox/tpcds/gen/dsdgen/include/tdef_functions.h"
#include "velox/tpcds/gen/dsdgen/include/dbgen_version.h"
#include "velox/tpcds/gen/dsdgen/include/w_call_center.h"
#include "velox/tpcds/gen/dsdgen/include/w_catalog_page.h"
#include "velox/tpcds/gen/dsdgen/include/w_catalog_returns.h"
#include "velox/tpcds/gen/dsdgen/include/w_catalog_sales.h"
#include "velox/tpcds/gen/dsdgen/include/w_customer.h"
#include "velox/tpcds/gen/dsdgen/include/w_customer_address.h"
#include "velox/tpcds/gen/dsdgen/include/w_customer_demographics.h"
#include "velox/tpcds/gen/dsdgen/include/w_datetbl.h"
#include "velox/tpcds/gen/dsdgen/include/w_household_demographics.h"
#include "velox/tpcds/gen/dsdgen/include/w_income_band.h"
#include "velox/tpcds/gen/dsdgen/include/w_inventory.h"
#include "velox/tpcds/gen/dsdgen/include/w_item.h"
#include "velox/tpcds/gen/dsdgen/include/w_promotion.h"
#include "velox/tpcds/gen/dsdgen/include/w_reason.h"
#include "velox/tpcds/gen/dsdgen/include/w_ship_mode.h"
#include "velox/tpcds/gen/dsdgen/include/w_store.h"
#include "velox/tpcds/gen/dsdgen/include/w_store_returns.h"
#include "velox/tpcds/gen/dsdgen/include/w_store_sales.h"
#include "velox/tpcds/gen/dsdgen/include/w_timetbl.h"
#include "velox/tpcds/gen/dsdgen/include/w_warehouse.h"
#include "velox/tpcds/gen/dsdgen/include/w_web_page.h"
#include "velox/tpcds/gen/dsdgen/include/w_web_returns.h"
#include "velox/tpcds/gen/dsdgen/include/w_web_sales.h"
#include "velox/tpcds/gen/dsdgen/include/w_web_site.h"

table_func_t w_tdef_funcs[] = {
    {"call_center", mk_w_call_center, {NULL, NULL}, NULL},
    {"catalog_page", mk_w_catalog_page, {NULL, NULL}, NULL},
    {"catalog_returns", NULL, {NULL, NULL}, NULL},
    {"catalog_sales", mk_w_catalog_sales, {NULL, NULL}, NULL},
    {"customer", mk_w_customer, {NULL, NULL}, NULL},
    {"customer_address", mk_w_customer_address, {NULL, NULL}, NULL},
    {"customer_demographics", mk_w_customer_demographics, {NULL, NULL}, NULL},
    {"date", mk_w_date, {NULL, NULL}, NULL},
    {"household_demographics", mk_w_household_demographics, {NULL, NULL}, NULL},
    {"income_band", mk_w_income_band, {NULL, NULL}, NULL},
    {"inventory", mk_w_inventory, {NULL, NULL}, NULL},
    {"item", mk_w_item, {NULL, NULL}, NULL},
    {"promotion", mk_w_promotion, {NULL, NULL}, NULL},
    {"reason", mk_w_reason, {NULL, NULL}, NULL},
    {"ship_mode", mk_w_ship_mode, {NULL, NULL}, NULL},
    {"store", mk_w_store, {NULL, NULL}, NULL},
    {"store_returns", mk_w_store_returns, {NULL, NULL}, NULL},
    {"store_sales", mk_w_store_sales, {NULL, NULL}, NULL},
    {"time", mk_w_time, {NULL, NULL}, NULL},
    {"warehouse", mk_w_warehouse, {NULL, NULL}, NULL},
    {"web_page", mk_w_web_page, {NULL, NULL}, NULL},
    {"web_returns", mk_w_web_returns, {NULL, NULL}, NULL},
    {"web_sales", mk_w_web_sales, {NULL, NULL}, NULL},
    {"web_site", mk_w_web_site, {NULL, NULL}, NULL},
    {"dbgen_version", mk_dbgen_version, {NULL, NULL}, NULL},
    {NULL, NULL, {NULL, NULL}, NULL}};

table_func_t* getTdefFunctionsByNumber(int nTable) {
  return (&w_tdef_funcs[nTable]);
}
