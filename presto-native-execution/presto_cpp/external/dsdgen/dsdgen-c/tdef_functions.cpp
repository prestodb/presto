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

#include "tdef_functions.h"
#include "dbgen_version.h"
#include "w_call_center.h"
#include "w_catalog_page.h"
#include "w_catalog_returns.h"
#include "w_catalog_sales.h"
#include "w_customer.h"
#include "w_customer_address.h"
#include "w_customer_demographics.h"
#include "w_datetbl.h"
#include "w_household_demographics.h"
#include "w_income_band.h"
#include "w_inventory.h"
#include "w_item.h"
#include "w_promotion.h"
#include "w_reason.h"
#include "w_ship_mode.h"
#include "w_store.h"
#include "w_store_returns.h"
#include "w_store_sales.h"
#include "w_timetbl.h"
#include "w_warehouse.h"
#include "w_web_page.h"
#include "w_web_returns.h"
#include "w_web_sales.h"
#include "w_web_site.h"

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
    {NULL}};

table_func_t* getTdefFunctionsByNumber(int nTable) {
  return (&w_tdef_funcs[nTable]);
}
