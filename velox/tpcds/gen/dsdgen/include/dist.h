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

#pragma once
#include <folly/Singleton.h>
#include <folly/Synchronized.h>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"

#ifndef STREAMS_H
#define STREAMS_H
#endif
#include "velox/tpcds/gen/dsdgen/include/constants.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"

#ifndef R_DIST_H
#define R_DIST_H
#endif

/***
 *** RELEASE INFORMATION
 ***/
#define VERSION 2
#define RELEASE 10
#define MODIFICATION 0
#define PATCH ""
#define COPYRIGHT "Transaction Processing Performance Council (TPC)"
#define C_DATES "2001 - 2018"

#define D_NAME_LEN 20
#define MAX_STREAM 805

#define RS_W_STORE_NAME 50
#define RS_W_STORE_MGR 40
#define RS_W_MARKET_MGR 40
#define RS_W_MARKET_DESC 100
#define STORE_MIN_TAX_PERCENTAGE "0.00"
#define STORE_MAX_TAX_PERCENTAGE "0.11"

/***
 *** STORE_xxx Store Defines
 ***/
#define STORE_MIN_DAYS_OPEN 5
#define STORE_MAX_DAYS_OPEN 500
#define STORE_CLOSED_PCT 30
#define STORE_MIN_REV_GROWTH "-0.05"
#define STORE_MAX_REV_GROWTH "0.50"
#define STORE_DESC_MIN 15

#define RS_VERSION_LENGTH 100
#define RS_CMDLINARGS_LENGTH 200

#define OPT_NONE 0x00
#define OPT_FLG 0x01 /* option is a flag; no parameter */
#define OPT_INT 0x02 /* argument is an integer */
#define OPT_STR 0x04 /* argument is a string */
#define OPT_NOP 0x08 /* flags non-operational options */
#define OPT_SUB 0x10 /* sub-option defined */
#define OPT_ADV 0x20 /* advanced option */
#define OPT_SET \
  0x40 /* not changeable -- used for default/file/command precedence */
#define OPT_DFLT 0x80 /* param set to non-zero default */
#define OPT_MULTI 0x100 /* param may be set repeatedly */
#define OPT_HIDE 0x200 /* hidden option -- not listed in usage */
#define TYPE_MASK 0x07

/*
 * Flag field definitions used in tdefs[]
 */
#define FL_NONE 0x0000 /* this table is not defined */
#define FL_NOP 0x0001 /* this table is not defined */
#define FL_DATE_BASED 0x0002 /* this table is produced in date order */
#define FL_CHILD 0x0004 /* this table is the child in a parent/child link */
#define FL_OPEN 0x0008 /* this table has a valid output destination */
#define FL_DUP_NAME 0x0010 /* to keep find_table() from complaining twice */
#define FL_TYPE_2                                                          \
  0x0020 /* this dimension keeps history -- rowcount shows unique entities \
            (not including revisions) */
#define FL_SMALL 0x0040 /* this table has low rowcount; used by address.c */
#define FL_SPARSE 0x0080
/* unused 0x0100 */
#define FL_NO_UPDATE \
  0x0200 /* this table is not altered by the update process */
#define FL_SOURCE_DDL 0x0400 /* table in the souce schema */
#define FL_JOIN_ERROR 0x0800 /* join called without an explicit rule */
#define FL_PARENT 0x1000 /* this table has a child in nParam */
#define FL_FACT_TABLE 0x2000
#define FL_PASSTHRU 0x4000 /* verify routine uses warehouse without change */
#define FL_VPRINT 0x8000 /* verify routine includes print function */

#define JMS 1

typedef HUGE_TYPE ds_key_t;

struct DSDGenContext;

typedef struct DIST_T {
  std::vector<int32_t> type_vector;
  std::vector<std::vector<int32_t>> weight_sets;
  std::vector<int32_t> maximums;
  std::vector<std::vector<int32_t>> value_sets;
  std::vector<char> strings;
  std::vector<char> names;
  int size;
} dist_t;

struct DBGEN_VERSION_TBL {
  char szVersion[RS_VERSION_LENGTH + 1];
  char szDate[26];
  char szTime[26];
  char szCmdLineArgs[RS_CMDLINARGS_LENGTH + 1];
};

typedef struct D_IDX_T {
  char name[D_NAME_LEN + 1];
  int index;
  int nAllocatedLength;
  int nRemainingStrSpace;
  int offset;
  int str_space;
  int name_space;
  int length;
  int w_width;
  int v_width;
  int flags;
  dist_t dist;
} d_idx_t;

typedef struct DISTINDEX_T {
  int nDistCount;
  int nAllocatedCount;
  d_idx_t* pEntries;
} distindex_t;

/*
 * a precise decimal data type, using scaled integer
 * arithmetic.
 */
typedef struct DECIMAL_T {
  int flags;
  int precision;
  int scale;
  ds_key_t number;
} decimal_t;

typedef struct OPTION_T {
  const char* name;
  int flags;
  int index;
  const char* usage;
  int (*action)(
      const char* szPName,
      const char* optarg,
      DSDGenContext& dsdGenContext);
  std::string dflt;
} option_t;

typedef struct DS_PRICING_T {
  decimal_t wholesale_cost;
  decimal_t list_price;
  decimal_t sales_price;
  int quantity;
  decimal_t ext_discount_amt;
  decimal_t ext_sales_price;
  decimal_t ext_wholesale_cost;
  decimal_t ext_list_price;
  decimal_t tax_pct;
  decimal_t ext_tax;
  decimal_t coupon_amt;
  decimal_t ship_cost;
  decimal_t ext_ship_cost;
  decimal_t net_paid;
  decimal_t net_paid_inc_tax;
  decimal_t net_paid_inc_ship;
  decimal_t net_paid_inc_ship_tax;
  decimal_t net_profit;
  decimal_t refunded_cash;
  decimal_t reversed_charge;
  decimal_t store_credit;
  decimal_t fee;
  decimal_t net_loss;
} ds_pricing_t;

typedef struct DS_ADDR_T {
  char suite_num[RS_CC_SUITE_NUM + 1];
  int street_num;
  char* street_name1;
  char* street_name2;
  char* street_type;
  char* city;
  char* county;
  char* state;
  char country[RS_CC_COUNTRY + 1];
  int zip;
  int plus4;
  int gmt_offset;
} ds_addr_t;

/*
 * STORE_SALES table structure
 */
struct W_STORE_SALES_TBL {
  ds_key_t ss_sold_date_sk;
  ds_key_t ss_sold_time_sk;
  ds_key_t ss_sold_item_sk;
  ds_key_t ss_sold_customer_sk;
  ds_key_t ss_sold_cdemo_sk;
  ds_key_t ss_sold_hdemo_sk;
  ds_key_t ss_sold_addr_sk;
  ds_key_t ss_sold_store_sk;
  ds_key_t ss_sold_promo_sk;
  ds_key_t ss_ticket_number;
  ds_pricing_t ss_pricing;
};

/*
 * STORE_RETURNS table structure
 */
struct W_STORE_RETURNS_TBL {
  ds_key_t sr_returned_date_sk;
  ds_key_t sr_returned_time_sk;
  ds_key_t sr_item_sk;
  ds_key_t sr_customer_sk;
  ds_key_t sr_cdemo_sk;
  ds_key_t sr_hdemo_sk;
  ds_key_t sr_addr_sk;
  ds_key_t sr_store_sk;
  ds_key_t sr_reason_sk;
  ds_key_t sr_ticket_number;
  ds_pricing_t sr_pricing;
};

typedef struct RNG_T {
  int nUsed;
  int nUsedPerRow;
  long nSeed;
  long nInitialSeed; /* used to allow skip_row() to back up */
  int nColumn; /* column where this stream is used */
  int nTable; /* table where this stream is used */
  int nDuplicateOf; /* duplicate streams allow independent tables to share
                       data streams */
#ifdef JMS
  ds_key_t nTotal;
#endif
} rng_t;

struct W_DATE_TBL {
  ds_key_t d_date_sk;
  char d_date_id[RS_BKEY + 1];
  /* this is generated at output from d_date_sk */
  /* date_t		d_date; */
  int d_month_seq;
  int d_week_seq;
  int d_quarter_seq;
  int d_year;
  int d_dow;
  int d_moy;
  int d_dom;
  int d_qoy;
  int d_fy_year;
  int d_fy_quarter_seq;
  int d_fy_week_seq;
  const char* d_day_name;
  /* char		d_quarter_name[RS_D_QUARTER_NAME + 1]; derived at print
   * time */
  int d_holiday;
  int d_weekend;
  int d_following_holiday;
  int d_first_dom;
  int d_last_dom;
  int d_same_day_ly;
  int d_same_day_lq;
  int d_current_day;
  int d_current_week;
  int d_current_month;
  int d_current_quarter;
  int d_current_year;
};

/*
 * STORE table structure
 */
struct W_STORE_TBL {
  ds_key_t store_sk;
  char store_id[RS_BKEY + 1];
  ds_key_t rec_start_date_id;
  ds_key_t rec_end_date_id;
  ds_key_t closed_date_id;
  char store_name[RS_W_STORE_NAME + 1];
  int employees;
  int floor_space;
  char* hours;
  char store_manager[RS_W_STORE_MGR + 1];
  int market_id;
  decimal_t dTaxPercentage;
  char* geography_class;
  char market_desc[RS_W_MARKET_DESC + 1];
  char market_manager[RS_W_MARKET_MGR + 1];
  ds_key_t division_id;
  char* division_name;
  ds_key_t company_id;
  char* company_name;
  ds_addr_t address;
};

/*
 * CALL_CENTER table structure
 */
struct CALL_CENTER_TBL {
  ds_key_t cc_call_center_sk;
  char cc_call_center_id[RS_BKEY + 1];
  ds_key_t cc_rec_start_date_id;
  ds_key_t cc_rec_end_date_id;
  ds_key_t cc_closed_date_id;
  ds_key_t cc_open_date_id;
  char cc_name[RS_CC_NAME + 1];
  char* cc_class;
  int cc_employees;
  int cc_sq_ft;
  char* cc_hours;
  char cc_manager[RS_CC_MANAGER + 1];
  int cc_market_id;
  char cc_market_class[RS_CC_MARKET_CLASS + 1];
  char cc_market_desc[RS_CC_MARKET_DESC + 1];
  char cc_market_manager[RS_CC_MARKET_MANAGER + 1];
  int cc_division_id;
  char cc_division_name[RS_CC_DIVISION_NAME + 1];
  int cc_company;
  char cc_company_name[RS_CC_COMPANY_NAME + 1];
  ds_addr_t cc_address;
  decimal_t cc_tax_percentage;
};

struct SCALING_T {
  ds_key_t kBaseRowcount;
  ds_key_t kNextInsertValue;
  int nUpdatePercentage;
  ds_key_t kDayRowcount[6];
};

/*
 * CATALOG_PAGE table structure
 */
struct CATALOG_PAGE_TBL {
  ds_key_t cp_catalog_page_sk;
  char cp_catalog_page_id[RS_BKEY + 1];
  ds_key_t cp_start_date_id;
  ds_key_t cp_end_date_id;
  char cp_department[RS_CP_DEPARTMENT + 1];
  int cp_catalog_number;
  int cp_catalog_page_number;
  char cp_description[RS_CP_DESCRIPTION + 1];
  char* cp_type;
};

/*
 * CATALOG_RETURNS table structure
 */
struct W_CATALOG_RETURNS_TBL {
  ds_key_t cr_returned_date_sk;
  ds_key_t cr_returned_time_sk;
  ds_key_t cr_item_sk;
  ds_key_t cr_refunded_customer_sk;
  ds_key_t cr_refunded_cdemo_sk;
  ds_key_t cr_refunded_hdemo_sk;
  ds_key_t cr_refunded_addr_sk;
  ds_key_t cr_returning_customer_sk;
  ds_key_t cr_returning_cdemo_sk;
  ds_key_t cr_returning_hdemo_sk;
  ds_key_t cr_returning_addr_sk;
  ds_key_t cr_call_center_sk;
  ds_key_t cr_catalog_page_sk;
  ds_key_t cr_ship_mode_sk;
  ds_key_t cr_warehouse_sk;
  ds_key_t cr_reason_sk;
  ds_key_t cr_order_number;
  ds_pricing_t cr_pricing;
  decimal_t cr_fee;
  decimal_t cr_refunded_cash;
  decimal_t cr_reversed_charge;
  decimal_t cr_store_credit;
  decimal_t cr_net_loss;
};

/*
 * CATALOG_SALES table structure
 */
struct W_CATALOG_SALES_TBL {
  ds_key_t cs_sold_date_sk;
  ds_key_t cs_sold_time_sk;
  ds_key_t cs_ship_date_sk;
  ds_key_t cs_bill_customer_sk;
  ds_key_t cs_bill_cdemo_sk;
  ds_key_t cs_bill_hdemo_sk;
  ds_key_t cs_bill_addr_sk;
  ds_key_t cs_ship_customer_sk;
  ds_key_t cs_ship_cdemo_sk;
  ds_key_t cs_ship_hdemo_sk;
  ds_key_t cs_ship_addr_sk;
  ds_key_t cs_call_center_sk;
  ds_key_t cs_catalog_page_sk;
  ds_key_t cs_ship_mode_sk;
  ds_key_t cs_warehouse_sk;
  ds_key_t cs_sold_item_sk;
  ds_key_t cs_promo_sk;
  ds_key_t cs_order_number;
  ds_pricing_t cs_pricing;
};

/*
 * CUSTOMER table structure
 */
struct W_CUSTOMER_TBL {
  ds_key_t c_customer_sk;
  char c_customer_id[RS_BKEY + 1];
  ds_key_t c_current_cdemo_sk;
  ds_key_t c_current_hdemo_sk;
  ds_key_t c_current_addr_sk;
  int c_first_shipto_date_id;
  int c_first_sales_date_id;
  char* c_salutation;
  char* c_first_name;
  char* c_last_name;
  int c_preferred_cust_flag;
  int c_birth_day;
  int c_birth_month;
  int c_birth_year;
  char* c_birth_country;
  char c_login[RS_C_LOGIN + 1] = {};
  char c_email_address[RS_C_EMAIL + 1];
  int c_last_review_date;
};

/*
 * CUSTOMER_ADDRESS table structure
 */
struct W_CUSTOMER_ADDRESS_TBL {
  ds_key_t ca_addr_sk;
  char ca_addr_id[RS_BKEY + 1];
  ds_addr_t ca_address;
  char* ca_location_type;
};

/*
 * CUSTOMER_DEMOGRAPHICS table structure
 */
struct W_CUSTOMER_DEMOGRAPHICS_TBL {
  ds_key_t cd_demo_sk;
  char* cd_gender;
  char* cd_marital_status;
  char* cd_education_status;
  int cd_purchase_estimate;
  char* cd_credit_rating;
  int cd_dep_count;
  int cd_dep_employed_count;
  int cd_dep_college_count;
};

/*
 * INVENTORY table structure
 */
struct W_INVENTORY_TBL {
  ds_key_t inv_date_sk;
  ds_key_t inv_item_sk;
  ds_key_t inv_warehouse_sk;
  int inv_quantity_on_hand;
};

/*
 * TIME_DIM table structure
 */
struct W_TIME_TBL {
  ds_key_t t_time_sk;
  char t_time_id[RS_BKEY + 1];
  int t_time;
  int t_hour;
  int t_minute;
  int t_second;
  char* t_am_pm;
  char* t_shift;
  char* t_sub_shift;
  char* t_meal_time;
};

/*
 * INCOME_BAND table structure
 */
struct W_INCOME_BAND_TBL {
  int ib_income_band_id;
  int ib_lower_bound;
  int ib_upper_bound;
};

/*
 * HOUSEHOLD_DEMOGRAPHICS table structure
 */
struct W_HOUSEHOLD_DEMOGRAPHICS_TBL {
  ds_key_t hd_demo_sk;
  ds_key_t hd_income_band_id;
  char* hd_buy_potential;
  int hd_dep_count;
  int hd_vehicle_count;
};

/*
 * SHIP_MODE table structure
 */
struct W_SHIP_MODE_TBL {
  ds_key_t sm_ship_mode_sk;
  char sm_ship_mode_id[RS_BKEY + 1];
  char* sm_type;
  char* sm_code;
  char* sm_carrier;
  char sm_contract[RS_SM_CONTRACT + 1];
};

/*
 * PROMOTION table structure
 */
struct W_PROMOTION_TBL {
  ds_key_t p_promo_sk;
  char p_promo_id[RS_BKEY + 1];
  ds_key_t p_start_date_id;
  ds_key_t p_end_date_id;
  ds_key_t p_item_sk;
  decimal_t p_cost;
  int p_response_target;
  char p_promo_name[RS_P_PROMO_NAME + 1];
  int p_channel_dmail;
  int p_channel_email;
  int p_channel_catalog;
  int p_channel_tv;
  int p_channel_radio;
  int p_channel_press;
  int p_channel_event;
  int p_channel_demo;
  char p_channel_details[RS_P_CHANNEL_DETAILS + 1];
  char* p_purpose;
  int p_discount_active;
};

/*
 * REASON table structure
 */
struct W_REASON_TBL {
  ds_key_t r_reason_sk;
  char r_reason_id[RS_BKEY + 1];
  char* r_reason_description;
};

/*
 * WAREHOUSE table structure
 */
struct W_WAREHOUSE_TBL {
  ds_key_t w_warehouse_sk;
  char w_warehouse_id[RS_BKEY + 1];
  char w_warehouse_name[RS_W_WAREHOUSE_NAME + 1];
  int w_warehouse_sq_ft;
  ds_addr_t w_address;
};

/*
 * WEB_SALES table structure
 */
struct W_WEB_SALES_TBL {
  ds_key_t ws_sold_date_sk;
  ds_key_t ws_sold_time_sk;
  ds_key_t ws_ship_date_sk;
  ds_key_t ws_item_sk;
  ds_key_t ws_bill_customer_sk;
  ds_key_t ws_bill_cdemo_sk;
  ds_key_t ws_bill_hdemo_sk;
  ds_key_t ws_bill_addr_sk;
  ds_key_t ws_ship_customer_sk;
  ds_key_t ws_ship_cdemo_sk;
  ds_key_t ws_ship_hdemo_sk;
  ds_key_t ws_ship_addr_sk;
  ds_key_t ws_web_page_sk;
  ds_key_t ws_web_site_sk;
  ds_key_t ws_ship_mode_sk;
  ds_key_t ws_warehouse_sk;
  ds_key_t ws_promo_sk;
  ds_key_t ws_order_number;
  ds_pricing_t ws_pricing;
};

/*
 * WEB_RETURNS table structure
 */
struct W_WEB_RETURNS_TBL {
  ds_key_t wr_returned_date_sk;
  ds_key_t wr_returned_time_sk;
  ds_key_t wr_item_sk;
  ds_key_t wr_refunded_customer_sk;
  ds_key_t wr_refunded_cdemo_sk;
  ds_key_t wr_refunded_hdemo_sk;
  ds_key_t wr_refunded_addr_sk;
  ds_key_t wr_returning_customer_sk;
  ds_key_t wr_returning_cdemo_sk;
  ds_key_t wr_returning_hdemo_sk;
  ds_key_t wr_returning_addr_sk;
  ds_key_t wr_web_page_sk;
  ds_key_t wr_reason_sk;
  ds_key_t wr_order_number;
  ds_pricing_t wr_pricing;
};

/*
 * WEB_PAGE table structure
 */
struct W_WEB_PAGE_TBL {
  ds_key_t wp_page_sk;
  char wp_page_id[RS_BKEY + 1];
  char wp_site_id[RS_BKEY + 1];
  ds_key_t wp_rec_start_date_id;
  ds_key_t wp_rec_end_date_id;
  ds_key_t wp_creation_date_sk;
  ds_key_t wp_access_date_sk;
  int wp_autogen_flag;
  ds_key_t wp_customer_sk;
  char wp_url[RS_WP_URL + 1];
  char* wp_type;
  int wp_char_count;
  int wp_link_count;
  int wp_image_count;
  int wp_max_ad_count;
};

/*
 * ITEM table structure
 */
struct W_ITEM_TBL {
  ds_key_t i_item_sk;
  char i_item_id[RS_BKEY + 1];
  ds_key_t i_rec_start_date_id;
  ds_key_t i_rec_end_date_id;
  char i_item_desc[RS_I_ITEM_DESC + 1];
  decimal_t i_current_price; /* list price */
  decimal_t i_wholesale_cost;
  ds_key_t i_brand_id;
  char i_brand[RS_I_BRAND + 1];
  ds_key_t i_class_id;
  char* i_class;
  ds_key_t i_category_id;
  char* i_category;
  ds_key_t i_manufact_id;
  char i_manufact[RS_I_MANUFACT + 1];
  char* i_size;
  char i_formulation[RS_I_FORMULATION + 1];
  char* i_color;
  char* i_units;
  char* i_container;
  ds_key_t i_manager_id;
  char i_product_name[RS_I_PRODUCT_NAME + 1];
  ds_key_t i_promo_sk;
};

/*
 * WEB_SITE table structure
 */
struct W_WEB_SITE_TBL {
  ds_key_t web_site_sk;
  char web_site_id[RS_BKEY + 1];
  ds_key_t web_rec_start_date_id;
  ds_key_t web_rec_end_date_id;
  char web_name[RS_WEB_NAME + 1];
  ds_key_t web_open_date;
  ds_key_t web_close_date;
  char web_class[RS_WEB_CLASS + 1];
  char web_manager[RS_WEB_MANAGER + 1];
  int web_market_id;
  char web_market_class[RS_WEB_MARKET_CLASS + 1];
  char web_market_desc[RS_WEB_MARKET_DESC + 1];
  char web_market_manager[RS_WEB_MARKET_MANAGER + 1];
  int web_company_id;
  char web_company_name[RS_WEB_COMPANY_NAME + 1];
  ds_addr_t web_address;
  decimal_t web_tax_percentage;
};

/*
 * general table descriptions.
 * NOTE: This table contains the constant elements in the table descriptions; it
 * must be kept in sync with the declararions of assocaited functions, found in
 * tdef_functions.h
 */

typedef struct TDEF_T {
  const char* name = nullptr; /* -- name of the table; */
  const char* abreviation = nullptr; /* -- shorthand name of the table */
  unsigned int flags = 0; /* -- control table options */
  int nFirstColumn = 0; /* -- first column/RNG for this table */
  int nLastColumn = 0; /* -- last column/RNG for this table */
  int nTableIndex = 0; /* used for rowcount calculations */
  int nParam = 0; /* -- additional table param (revision count, child number,
                 etc.) */
  FILE* outfile = nullptr; /* -- output destination */
  int nUpdateSize = 0; /* -- percentage of base rowcount in each update set
                      (basis points) */
  int nNewRowPct = 0;
  int nNullPct = 0; /* percentage of rows with nulls (basis points) */
  ds_key_t kNullBitMap = 0; /* colums that should be NULL in the current row */
  ds_key_t kNotNullBitMap = 0; /* columns that are defined NOT NULL */
  std::vector<ds_key_t> arSparseKeys =
      {}; /* sparse key set for table; used if FL_SPARSE is set */
} tdef;

int read_file(
    const char* param_name,
    const char* arg,
    DSDGenContext& dsdGenContext);
int usage(
    const char* param_name,
    const char* msg,
    DSDGenContext& dsdGenContext);
int SetScaleIndex(
    const char* szName,
    const char* szValue,
    DSDGenContext& dsdGenContext);
int printReleaseInfo(
    const char* param,
    const char* val,
    DSDGenContext& dsdGenContext);
static void print_options(
    struct OPTION_T* o,
    int bShowOptional,
    DSDGenContext& dsdGenContext);

static folly::Synchronized<std::unordered_map<std::string, d_idx_t>> idx_;
static std::once_flag initFlag_;

// DSDGenContext to access global variables.
struct DSDGenContext {
  DSDGenContext() : params(23 + 2) {}

  struct DBGEN_VERSION_TBL g_dbgen_version;

  struct W_STORE_SALES_TBL g_w_store_sales;
  int *pStoreSalesItemPermutation, *pCatalogSalesItemPermutation,
      *pWebSalesItemPermutation;
  int nStoreSalesItemCount, nCatalogSalesItemCount, nWebSalesItemCount;
  int nStoreSalesItemIndex, nCatalogSalesItemIndex, nWebSalesItemIndex;
  ds_key_t jDate, kNewDateIndex;
  int nTicketItemBase = 1;

  struct W_STORE_RETURNS_TBL g_w_store_returns;
  struct DIST_T dist;
  option_t options[26] = {
      {"ABREVIATION", OPT_STR, 0, "build table with abreviation <s>", NULL, ""},
      {"DELIMITER",
       OPT_STR | OPT_ADV,
       1,
       "use <s> as output field separator",
       NULL,
       "|"},
      {"DIR", OPT_STR, 2, "generate tables in directory <s>", NULL, "."},
      {"DISTRIBUTIONS",
       OPT_STR | OPT_ADV,
       3,
       "read distributions from file <s>",
       NULL,
       "NONE"},
      {"FORCE",
       OPT_FLG | OPT_ADV,
       4,
       "over-write data files without prompting",
       NULL,
       "N"},
      {"HELP", OPT_INT, 5, "display this message", usage, "0"},
      {"PARAMS", OPT_STR, 6, "read parameters from file <s>", read_file, ""},
      {"PROG",
       OPT_STR | OPT_HIDE | OPT_SET,
       7,
       "DO NOT MODIFY",
       NULL,
       "dsdgen"},
      {"QUIET", OPT_FLG, 8, "disable all output to stdout/stderr", NULL, "N"},
      {"SCALE",
       OPT_STR,
       9,
       "volume of data to generate in GB",
       SetScaleIndex,
       "1"},
      {"SUFFIX",
       OPT_STR | OPT_ADV,
       10,
       "use <s> as output file suffix",
       NULL,
       ".dat"},
      {"TABLE", OPT_STR, 11, "build only table <s>", NULL, "ALL"},
      {"TERMINATE",
       OPT_FLG | OPT_ADV,
       12,
       "end each record with a field delimiter",
       NULL,
       "Y"},
      {"UPDATE", OPT_INT, 13, "generate update data set <n>", NULL, ""},
      {"VERBOSE", OPT_FLG, 14, "enable verbose output", NULL, "N"},
      {"_SCALE_INDEX",
       OPT_INT | OPT_HIDE,
       15,
       "Scale band; used for dist lookups",
       NULL,
       "1"},
      {"PARALLEL", OPT_INT, 16, "build data in <n> separate chunks", NULL, ""},
      {"CHILD",
       OPT_INT,
       17,
       "generate <n>th chunk of the parallelized data",
       NULL,
       "1"},
      {"CHKSEEDS",
       OPT_FLG | OPT_HIDE,
       18,
       "validate RNG usage for parallelism",
       NULL,
       "N"},
      {"RELEASE",
       OPT_FLG,
       19,
       "display the release information",
       printReleaseInfo,
       "N"},
      {"_FILTER", OPT_FLG, 20, "output data to stdout", NULL, "N"},
      {"VALIDATE", OPT_FLG, 21, "produce rows for data validation", NULL, "N"},
      {"VCOUNT",
       OPT_INT | OPT_ADV,
       22,
       "set number of validation rows to be produced",
       NULL,
       "50"},
      {"VSUFFIX",
       OPT_STR | OPT_ADV,
       23,
       "set file suffix for data validation",
       NULL,
       ".vld"},
      {"RNGSEED", OPT_INT | OPT_ADV, 24, "set RNG seed", NULL, "19620718"}};

  std::vector<std::vector<char>> params;

  struct W_DATE_TBL g_w_date;

  struct W_STORE_TBL g_w_store;

  struct CALL_CENTER_TBL g_w_call_center;

  struct W_WEB_PAGE_TBL g_w_web_page;

  struct W_ITEM_TBL g_w_item;

  struct W_WEB_SITE_TBL g_w_web_site;

  struct CATALOG_PAGE_TBL g_w_catalog_page;

  struct W_CATALOG_RETURNS_TBL g_w_catalog_returns;

  struct W_CATALOG_SALES_TBL g_w_catalog_sales;

  struct W_CUSTOMER_TBL g_w_customer;

  struct W_CUSTOMER_ADDRESS_TBL g_w_customer_address;

  struct W_CUSTOMER_DEMOGRAPHICS_TBL g_w_customer_demographics;

  struct W_INVENTORY_TBL g_w_inventory;

  struct W_TIME_TBL g_w_time;

  struct W_INCOME_BAND_TBL g_w_income_band;

  struct W_HOUSEHOLD_DEMOGRAPHICS_TBL g_w_household_demographics;

  struct W_SHIP_MODE_TBL g_w_ship_mode;

  struct W_PROMOTION_TBL g_w_promotion;

  struct W_REASON_TBL g_w_reason;

  struct W_WAREHOUSE_TBL g_w_warehouse;

  struct W_WEB_SALES_TBL g_w_web_sales;

  struct W_WEB_RETURNS_TBL g_w_web_returns;

  SCALING_T arRowcount[MAX_TABLE + 1];

  int arUpdateDates[6];
  int arInventoryUpdateDates[6];

  int arScaleVolume[9] = {1, 10, 100, 300, 1000, 3000, 10000, 30000, 100000};

  int init_rand_init;
  int mk_address_init;
  int setUpdateDateRange_init;
  int mk_dbgen_version_init;
  int getCatalogNumberFromPage_init;
  int checkSeeds_init;
  int dateScaling_init;
  int mk_w_call_center_init;
  int mk_w_catalog_page_init;
  int mk_master_catalog_sales_init;
  int dectostr_init;
  int date_join_init;
  int setSCDKeys_init;
  int scd_join_init;
  int matchSCDSK_init;
  int skipDays_init;
  int mk_w_catalog_returns_init;
  int mk_detail_catalog_sales_init;
  int mk_w_customer_init;
  int mk_w_date_init;
  int mk_w_inventory_init;
  int mk_w_item_init;
  int mk_w_promotion_init;
  int mk_w_reason_init;
  int mk_w_ship_mode_init;
  int mk_w_store_returns_init;
  int mk_master_store_sales_init;
  int mk_master_web_sales_init;
  int mk_detail_web_sales_init;
  int mk_w_store_init;
  int mk_w_web_page_init;
  int mk_w_web_returns_init;
  int mk_master_init;
  int mk_detail_init;
  int mk_w_web_site_init;
  int mk_cust_init;
  int mk_order_init;
  int mk_part_init;
  int mk_supp_init;
  int dbg_text_init;
  int find_dist_init;
  int cp_join_init;
  int web_join_init;
  int set_pricing_init;
  int init_params_init;
  int get_rowcount_init;

  tdef s_tdefs[38] = {
      {"s_brand",
       "s_br",
       FL_NOP | FL_SOURCE_DDL,
       S_BRAND_START,
       S_BRAND_END,
       S_BRAND,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_customer_address",
       "s_ca",
       FL_SOURCE_DDL | FL_PASSTHRU,
       S_CUSTOMER_ADDRESS_START,
       S_CUSTOMER_ADDRESS_END,
       S_CUSTOMER_ADDRESS,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x01,
       {}},
      {"s_call_center",
       "s_cc",
       FL_SOURCE_DDL,
       S_CALL_CENTER_START,
       S_CALL_CENTER_END,
       S_CALL_CENTER,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x02,
       {}},
      {"s_catalog",
       "s_ct",
       FL_SOURCE_DDL | FL_NOP,
       S_CATALOG_START,
       S_CATALOG_END,
       S_CATALOG,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_catalog_order",
       "s_cord",
       FL_SOURCE_DDL | FL_PARENT | FL_DATE_BASED,
       S_CATALOG_ORDER_START,
       S_CATALOG_ORDER_END,
       S_CATALOG_ORDER,
       S_CATALOG_ORDER_LINEITEM,
       NULL,
       0,
       0,
       0,
       0x0,
       0x01,
       {}},
      {"s_catalog_order_lineitem",
       "s_cl",
       FL_SOURCE_DDL | FL_CHILD | FL_PARENT,
       S_CATALOG_ORDER_LINEITEM_START,
       S_CATALOG_ORDER_LINEITEM_END,
       S_CATALOG_ORDER_LINEITEM,
       S_CATALOG_RETURNS,
       NULL,
       0,
       0,
       0,
       0x0,
       0x07,
       {}},
      {"s_catalog_page",
       "s_cp",
       FL_SOURCE_DDL | FL_PASSTHRU,
       S_CATALOG_PAGE_START,
       S_CATALOG_PAGE_END,
       S_CATALOG_PAGE,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x033,
       {}},
      {"s_catalog_promotional_item",
       "s_ci",
       FL_NOP | FL_SOURCE_DDL,
       S_CATALOG_PROMOTIONAL_ITEM_START,
       S_CATALOG_PROMOTIONAL_ITEM_END,
       S_CATALOG_PROMOTIONAL_ITEM,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_catalog_returns",
       "s_cr",
       FL_SOURCE_DDL | FL_CHILD,
       S_CATALOG_RETURNS_START,
       S_CATALOG_RETURNS_END,
       S_CATALOG_RETURNS,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0E,
       {}},
      {"s_category",
       "s_cg",
       FL_NOP | FL_SOURCE_DDL,
       S_CATEGORY_START,
       S_CATEGORY_END,
       S_CATEGORY,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_class",
       "s_cl",
       FL_NOP | FL_SOURCE_DDL,
       S_CLASS_START,
       S_CLASS_END,
       S_CLASS,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_company",
       "s_co",
       FL_NOP | FL_SOURCE_DDL,
       S_COMPANY_START,
       S_COMPANY_END,
       S_COMPANY,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_customer",
       "s_cu",
       FL_SOURCE_DDL,
       S_CUSTOMER_START,
       S_CUSTOMER_END,
       S_CUSTOMER,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x01,
       {}},
      {"s_division",
       "s_di",
       FL_NOP | FL_SOURCE_DDL,
       S_DIVISION_START,
       S_DIVISION_END,
       S_DIVISION,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_inventory",
       "s_in",
       FL_SOURCE_DDL | FL_DATE_BASED,
       S_INVENTORY_START,
       S_INVENTORY_END,
       S_INVENTORY,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x07,
       {}},
      {"s_item",
       "s_it",
       FL_SOURCE_DDL,
       S_ITEM_START,
       S_ITEM_END,
       S_ITEM,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x01,
       {}},
      {"s_manager",
       "s_mg",
       FL_NOP | FL_SOURCE_DDL,
       S_MANAGER_START,
       S_MANAGER_END,
       S_MANAGER,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_manufacturer",
       "s_mn",
       FL_NOP | FL_SOURCE_DDL,
       S_MANUFACTURER_START,
       S_MANUFACTURER_END,
       S_MANUFACTURER,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_market",
       "s_mk",
       FL_NOP | FL_SOURCE_DDL,
       S_MARKET_START,
       S_MARKET_END,
       S_MARKET,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_product",
       "s_pr",
       FL_NOP | FL_SOURCE_DDL,
       S_PRODUCT_START,
       S_PRODUCT_END,
       S_PRODUCT,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_promotion",
       "s_pm",
       FL_SOURCE_DDL | FL_PASSTHRU,
       S_PROMOTION_START,
       S_PROMOTION_END,
       S_PROMOTION,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x01,
       {}},
      {"s_purchase",
       "s_pu",
       FL_SOURCE_DDL | FL_PARENT | FL_DATE_BASED,
       S_PURCHASE_START,
       S_PURCHASE_END,
       S_PURCHASE,
       S_PURCHASE_LINEITEM,
       NULL,
       0,
       0,
       0,
       0x0,
       0x01,
       {}},
      {"s_purchase_lineitem",
       "s_pl",
       FL_SOURCE_DDL | FL_CHILD | FL_PARENT,
       S_PURCHASE_LINEITEM_START,
       S_PURCHASE_LINEITEM_END,
       S_PURCHASE_LINEITEM,
       S_STORE_RETURNS,
       NULL,
       0,
       0,
       0,
       0x0,
       0x07,
       {}},
      {"s_reason",
       "s_re",
       FL_NOP | FL_SOURCE_DDL,
       S_REASON_START,
       S_REASON_END,
       S_REASON,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_store",
       "s_st",
       FL_SOURCE_DDL,
       S_STORE_START,
       S_STORE_END,
       S_STORE,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x01,
       {}},
      {"s_store_promotional_item",
       "s_sp",
       FL_NOP | FL_SOURCE_DDL,
       S_STORE_PROMOTIONAL_ITEM_START,
       S_STORE_PROMOTIONAL_ITEM_END,
       S_STORE_PROMOTIONAL_ITEM,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_store_returns",
       "s_sr",
       FL_SOURCE_DDL | FL_CHILD,
       S_STORE_RETURNS_START,
       S_STORE_RETURNS_END,
       S_STORE_RETURNS,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0E,
       {}},
      {"s_subcategory",
       "s_ct",
       FL_NOP | FL_SOURCE_DDL,
       S_SUBCATEGORY_START,
       S_SUBCATEGORY_END,
       S_SUBCATEGORY,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_subclass",
       "s_sc",
       FL_NOP | FL_SOURCE_DDL,
       S_SUBCLASS_START,
       S_SUBCLASS_END,
       S_SUBCLASS,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_warehouse",
       "s_.h",
       FL_SOURCE_DDL,
       S_WAREHOUSE_START,
       S_WAREHOUSE_END,
       S_WAREHOUSE,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x01,
       {}},
      {"s_web_order",
       "s_wo",
       FL_SOURCE_DDL | FL_PARENT | FL_DATE_BASED,
       S_WEB_ORDER_START,
       S_WEB_ORDER_END,
       S_WEB_ORDER,
       S_WEB_ORDER_LINEITEM,
       NULL,
       0,
       0,
       0,
       0x0,
       0x01,
       {}},
      {"s_web_order_lineitem",
       "s_wl",
       FL_SOURCE_DDL | FL_CHILD | FL_PARENT,
       S_WEB_ORDER_LINEITEM_START,
       S_WEB_ORDER_LINEITEM_END,
       S_WEB_ORDER_LINEITEM,
       S_WEB_RETURNS,
       NULL,
       0,
       0,
       0,
       0x0,
       0x07,
       {}},
      {"s_web_page",
       "s_wp",
       FL_SOURCE_DDL | FL_PASSTHRU,
       S_WEB_PAGE_START,
       S_WEB_PAGE_END,
       S_WEB_PAGE,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x01,
       {}},
      {"s_web_promotional_item",
       "s_wi",
       FL_NOP | FL_SOURCE_DDL,
       S_WEB_PROMOTIONAL_ITEM_START,
       S_WEB_PROMOTIONAL_ITEM_END,
       S_WEB_PROMOTIONAL_ITEM,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0,
       {}},
      {"s_web_returns",
       "s_wr",
       FL_SOURCE_DDL | FL_CHILD,
       S_WEB_RETURNS_START,
       S_WEB_RETURNS_END,
       S_WEB_RETURNS,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x0E,
       {}},
      {"s_web_site",
       "s_ws",
       FL_SOURCE_DDL,
       S_WEB_SITE_START,
       S_WEB_SITE_END,
       S_WEB_SITE,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x01,
       {}},
      {"s_zip_to_gmt",
       "s_zi",
       FL_SOURCE_DDL | FL_VPRINT,
       S_ZIPG_START,
       S_ZIPG_END,
       S_ZIPG,
       -1,
       NULL,
       0,
       0,
       0,
       0x0,
       0x03,
       {}},
      {NULL}};

  tdef w_tdefs[26] = {
      {"call_center",
       "cc",
       FL_TYPE_2 | FL_SMALL,
       CALL_CENTER_START,
       CALL_CENTER_END,
       CALL_CENTER,
       -1,
       NULL,
       0,
       0,
       100,
       0,
       0x0B,
       {}},
      {"catalog_page",
       "cp",
       0,
       CATALOG_PAGE_START,
       CATALOG_PAGE_END,
       CATALOG_PAGE,
       -1,
       NULL,
       0,
       0,
       200,
       0,
       0x03,
       {}},
      {"catalog_returns",
       "cr",
       FL_CHILD,
       CATALOG_RETURNS_START,
       CATALOG_RETURNS_END,
       CATALOG_RETURNS,
       -1,
       NULL,
       0,
       0,
       400,
       0,
       0x10007,
       {}},
      {"catalog_sales",
       "cs",
       FL_PARENT | FL_DATE_BASED | FL_VPRINT,
       CATALOG_SALES_START,
       CATALOG_SALES_END,
       CATALOG_SALES,
       CATALOG_RETURNS,
       NULL,
       0,
       0,
       100,
       0,
       0x28000,
       {}},
      {"customer",
       "cu",
       0,
       CUSTOMER_START,
       CUSTOMER_END,
       CUSTOMER,
       -1,
       NULL,
       0,
       0,
       700,
       0,
       0x13,
       {}},
      {"customer_address",
       "ca",
       0,
       CUSTOMER_ADDRESS_START,
       CUSTOMER_ADDRESS_END,
       CUSTOMER_ADDRESS,
       -1,
       NULL,
       0,
       0,
       600,
       0,
       0x03,
       {}},
      {"customer_demographics",
       "cd",
       0,
       CUSTOMER_DEMOGRAPHICS_START,
       CUSTOMER_DEMOGRAPHICS_END,
       CUSTOMER_DEMOGRAPHICS,
       823200,
       NULL,
       0,
       0,
       0,
       0,
       0x1,
       {}},
      {"date_dim",
       "da",
       0,
       DATE_START,
       DATE_END,
       DATET,
       -1,
       NULL,
       0,
       0,
       0,
       0,
       0x03,
       {}},
      {"household_demographics",
       "hd",
       0,
       HOUSEHOLD_DEMOGRAPHICS_START,
       HOUSEHOLD_DEMOGRAPHICS_END,
       HOUSEHOLD_DEMOGRAPHICS,
       7200,
       NULL,
       0,
       0,
       0,
       0,
       0x01,
       {}},
      {"income_band",
       "ib",
       0,
       INCOME_BAND_START,
       INCOME_BAND_END,
       INCOME_BAND,
       -1,
       NULL,
       0,
       0,
       0,
       0,
       0x1,
       {}},
      {"inventory",
       "inv",
       FL_DATE_BASED,
       INVENTORY_START,
       INVENTORY_END,
       INVENTORY,
       -1,
       NULL,
       0,
       0,
       1000,
       0,
       0x07,
       {}},
      {"item",
       "it",
       FL_TYPE_2,
       ITEM_START,
       ITEM_END,
       ITEM,
       -1,
       NULL,
       0,
       0,
       50,
       0,
       0x0B,
       {}},
      {"promotion",
       "pr",
       0,
       PROMOTION_START,
       PROMOTION_END,
       PROMOTION,
       -1,
       NULL,
       0,
       0,
       200,
       0,
       0x03,
       {}},
      {"reason",
       "re",
       0,
       REASON_START,
       REASON_END,
       REASON,
       -1,
       NULL,
       0,
       0,
       0,
       0,
       0x03,
       {}},
      {"ship_mode",
       "sm",
       0,
       SHIP_MODE_START,
       SHIP_MODE_END,
       SHIP_MODE,
       -1,
       NULL,
       0,
       0,
       0,
       0,
       0x03,
       {}},
      {"store",
       "st",
       FL_TYPE_2 | FL_SMALL,
       STORE_START,
       STORE_END,
       STORE,
       -1,
       NULL,
       0,
       0,
       100,
       0,
       0xB,
       {}},
      {"store_returns",
       "sr",
       FL_CHILD,
       STORE_RETURNS_START,
       STORE_RETURNS_END,
       STORE_RETURNS,
       -1,
       NULL,
       0,
       0,
       700,
       0,
       0x204,
       {}},
      {"store_sales",
       "ss",
       FL_PARENT | FL_DATE_BASED | FL_VPRINT,
       STORE_SALES_START,
       STORE_SALES_END,
       STORE_SALES,
       STORE_RETURNS,
       NULL,
       0,
       0,
       900,
       0,
       0x204,
       {}},
      {"time_dim",
       "ti",
       0,
       TIME_START,
       TIME_END,
       TIMET,
       -1,
       NULL,
       0,
       0,
       0,
       0,
       0x03,
       {}},
      {"warehouse",
       "wa",
       FL_SMALL,
       WAREHOUSE_START,
       WAREHOUSE_END,
       WAREHOUSE,
       -1,
       NULL,
       0,
       0,
       200,
       0,
       0x03,
       {}},
      {"web_page",
       "wp",
       FL_TYPE_2,
       WEB_PAGE_START,
       WEB_PAGE_END,
       WEB_PAGE,
       -1,
       NULL,
       0,
       0,
       250,
       0,
       0x0B,
       {}},
      {"web_returns",
       "wr",
       FL_CHILD,
       WEB_RETURNS_START,
       WEB_RETURNS_END,
       WEB_RETURNS,
       -1,
       NULL,
       0,
       0,
       900,
       0,
       0x2004,
       {}},
      {"web_sales",
       "ws",
       FL_VPRINT | FL_PARENT | FL_DATE_BASED,
       WEB_SALES_START,
       WEB_SALES_END,
       WEB_SALES,
       WEB_RETURNS,
       NULL,
       0,
       0,
       5,
       1100,
       0x20008,
       {}},
      {"web_site",
       "web",
       FL_TYPE_2 | FL_SMALL,
       WEB_SITE_START,
       WEB_SITE_END,
       WEB_SITE,
       -1,
       NULL,
       0,
       0,
       100,
       0,
       0x0B,
       {}},
      {"dbgen_version",
       "dv",
       0,
       DBGEN_VERSION_START,
       DBGEN_VERSION_END,
       DBGEN_VERSION,
       -1,
       NULL,
       0,
       0,
       0,
       0,
       0,
       {}},
      {NULL}};

  rng_t Streams[MAX_STREAM + 1] = {
      {0, 0, 0, 0, 0, 0, 0, 0},
      {0, 0, 0, 0, CC_CALL_CENTER_SK, CALL_CENTER, CC_CALL_CENTER_SK, 0},
      {0, 15, 0, 0, CC_CALL_CENTER_ID, CALL_CENTER, CC_CALL_CENTER_ID, 0},
      {0, 10, 0, 0, CC_REC_START_DATE_ID, CALL_CENTER, CC_REC_START_DATE_ID, 0},
      {0, 1, 0, 0, CC_REC_END_DATE_ID, CALL_CENTER, CC_REC_END_DATE_ID, 0},
      {0, 4, 0, 0, CC_CLOSED_DATE_ID, CALL_CENTER, CC_CLOSED_DATE_ID, 0},
      {0, 10, 0, 0, CC_OPEN_DATE_ID, CALL_CENTER, CC_OPEN_DATE_ID, 0},
      {0, 0, 0, 0, CC_NAME, CALL_CENTER, CC_NAME, 0},
      {0, 2, 0, 0, CC_CLASS, CALL_CENTER, CC_CLASS, 0},
      {0, 1, 0, 0, CC_EMPLOYEES, CALL_CENTER, CC_EMPLOYEES, 0},
      {0, 1, 0, 0, CC_SQ_FT, CALL_CENTER, CC_SQ_FT, 0},
      {0, 1, 0, 0, CC_HOURS, CALL_CENTER, CC_HOURS, 0},
      {0, 2, 0, 0, CC_MANAGER, CALL_CENTER, CC_MANAGER, 0},
      {0, 1, 0, 0, CC_MARKET_ID, CALL_CENTER, CC_MARKET_ID, 0},
      {0, 50, 0, 0, CC_MARKET_CLASS, CALL_CENTER, CC_MARKET_CLASS, 0},
      {0, 50, 0, 0, CC_MARKET_DESC, CALL_CENTER, CC_MARKET_DESC, 0},
      {0, 2, 0, 0, CC_MARKET_MANAGER, CALL_CENTER, CC_MARKET_MANAGER, 0},
      {0, 2, 0, 0, CC_DIVISION, CALL_CENTER, CC_DIVISION, 0},
      {0, 2, 0, 0, CC_DIVISION_NAME, CALL_CENTER, CC_DIVISION_NAME, 0},
      {0, 2, 0, 0, CC_COMPANY, CALL_CENTER, CC_COMPANY, 0},
      {0, 2, 0, 0, CC_COMPANY_NAME, CALL_CENTER, CC_COMPANY_NAME, 0},
      {0, 0, 0, 0, CC_STREET_NUMBER, CALL_CENTER, CC_STREET_NUMBER, 0},
      {0, 0, 0, 0, CC_STREET_NAME, CALL_CENTER, CC_STREET_NAME, 0},
      {0, 0, 0, 0, CC_STREET_TYPE, CALL_CENTER, CC_STREET_TYPE, 0},
      {0, 0, 0, 0, CC_SUITE_NUMBER, CALL_CENTER, CC_SUITE_NUMBER, 0},
      {0, 0, 0, 0, CC_CITY, CALL_CENTER, CC_CITY, 0},
      {0, 0, 0, 0, CC_COUNTY, CALL_CENTER, CC_COUNTY, 0},
      {0, 0, 0, 0, CC_STATE, CALL_CENTER, CC_STATE, 0},
      {0, 0, 0, 0, CC_ZIP, CALL_CENTER, CC_ZIP, 0},
      {0, 0, 0, 0, CC_COUNTRY, CALL_CENTER, CC_COUNTRY, 0},
      {0, 0, 0, 0, CC_GMT_OFFSET, CALL_CENTER, CC_GMT_OFFSET, 0},
      {0, 15, 0, 0, CC_ADDRESS, CALL_CENTER, CC_ADDRESS, 0},
      {0, 1, 0, 0, CC_TAX_PERCENTAGE, CALL_CENTER, CC_TAX_PERCENTAGE, 0},
      {0, 1, 0, 0, CC_SCD, CALL_CENTER, CC_SCD, 0},
      {0, 2, 0, 0, CC_NULLS, CALL_CENTER, CC_NULLS, 0},
      {0, 1, 0, 0, CP_CATALOG_PAGE_SK, CATALOG_PAGE, CP_CATALOG_PAGE_SK, 0},
      {0, 1, 0, 0, CP_CATALOG_PAGE_ID, CATALOG_PAGE, CP_CATALOG_PAGE_ID, 0},
      {0, 1, 0, 0, CP_START_DATE_ID, CATALOG_PAGE, CP_START_DATE_ID, 0},
      {0, 1, 0, 0, CP_END_DATE_ID, CATALOG_PAGE, CP_END_DATE_ID, 0},
      {0, 1, 0, 0, CP_PROMO_ID, CATALOG_PAGE, CP_PROMO_ID, 0},
      {0, 1, 0, 0, CP_DEPARTMENT, CATALOG_PAGE, CP_DEPARTMENT, 0},
      {0, 1, 0, 0, CP_CATALOG_NUMBER, CATALOG_PAGE, CP_CATALOG_NUMBER, 0},
      {0,
       1,
       0,
       0,
       CP_CATALOG_PAGE_NUMBER,
       CATALOG_PAGE,
       CP_CATALOG_PAGE_NUMBER,
       0},
      {0, 100, 0, 0, CP_DESCRIPTION, CATALOG_PAGE, S_CP_DESCRIPTION, 0},
      {0, 1, 0, 0, CP_TYPE, CATALOG_PAGE, CP_TYPE, 0},
      {0, 2, 0, 0, CP_NULLS, CATALOG_PAGE, CP_NULLS, 0},
      {0,
       28,
       0,
       0,
       CR_RETURNED_DATE_SK,
       CATALOG_RETURNS,
       CR_RETURNED_DATE_SK,
       0},
      {0,
       28,
       0,
       0,
       CR_RETURNED_TIME_SK,
       CATALOG_RETURNS,
       CR_RETURNED_TIME_SK,
       0},
      {0, 14, 0, 0, CR_ITEM_SK, CATALOG_RETURNS, CR_ITEM_SK, 0},
      {0,
       14,
       0,
       0,
       CR_REFUNDED_CUSTOMER_SK,
       CATALOG_RETURNS,
       CR_REFUNDED_CUSTOMER_SK,
       0},
      {0,
       14,
       0,
       0,
       CR_REFUNDED_CDEMO_SK,
       CATALOG_RETURNS,
       CR_REFUNDED_CDEMO_SK,
       0},
      {0,
       14,
       0,
       0,
       CR_REFUNDED_HDEMO_SK,
       CATALOG_RETURNS,
       CR_REFUNDED_HDEMO_SK,
       0},
      {0,
       14,
       0,
       0,
       CR_REFUNDED_ADDR_SK,
       CATALOG_RETURNS,
       CR_REFUNDED_ADDR_SK,
       0},
      {0,
       28,
       0,
       0,
       CR_RETURNING_CUSTOMER_SK,
       CATALOG_RETURNS,
       CR_RETURNING_CUSTOMER_SK,
       0},
      {0,
       14,
       0,
       0,
       CR_RETURNING_CDEMO_SK,
       CATALOG_RETURNS,
       CR_RETURNING_CDEMO_SK,
       0},
      {0,
       14,
       0,
       0,
       CR_RETURNING_HDEMO_SK,
       CATALOG_RETURNS,
       CR_RETURNING_HDEMO_SK,
       0},
      {0,
       14,
       0,
       0,
       CR_RETURNING_ADDR_SK,
       CATALOG_RETURNS,
       CR_RETURNING_ADDR_SK,
       0},
      {0, 0, 0, 0, CR_CALL_CENTER_SK, CATALOG_RETURNS, CR_CALL_CENTER_SK, 0},
      {0, 14, 0, 0, CR_CATALOG_PAGE_SK, CATALOG_RETURNS, CR_CATALOG_PAGE_SK, 0},
      {0, 14, 0, 0, CR_SHIP_MODE_SK, CATALOG_RETURNS, CR_SHIP_MODE_SK, 0},
      {0, 14, 0, 0, CR_WAREHOUSE_SK, CATALOG_RETURNS, CR_WAREHOUSE_SK, 0},
      {0, 14, 0, 0, CR_REASON_SK, CATALOG_RETURNS, CR_REASON_SK, 0},
      {0, 0, 0, 0, CR_ORDER_NUMBER, CATALOG_RETURNS, CR_ORDER_NUMBER, 0},
      {0,
       0,
       0,
       0,
       CR_PRICING_QUANTITY,
       CATALOG_RETURNS,
       CR_PRICING_QUANTITY,
       0},
      {0,
       0,
       0,
       0,
       CR_PRICING_NET_PAID,
       CATALOG_RETURNS,
       CR_PRICING_NET_PAID,
       0},
      {0, 0, 0, 0, CR_PRICING_EXT_TAX, CATALOG_RETURNS, CR_PRICING_EXT_TAX, 0},
      {0,
       0,
       0,
       0,
       CR_PRICING_NET_PAID_INC_TAX,
       CATALOG_RETURNS,
       CR_PRICING_NET_PAID_INC_TAX,
       0},
      {0, 0, 0, 0, CR_PRICING_FEE, CATALOG_RETURNS, CR_PRICING_FEE, 0},
      {0,
       0,
       0,
       0,
       CR_PRICING_EXT_SHIP_COST,
       CATALOG_RETURNS,
       CR_PRICING_EXT_SHIP_COST,
       0},
      {0,
       0,
       0,
       0,
       CR_PRICING_REFUNDED_CASH,
       CATALOG_RETURNS,
       CR_PRICING_REFUNDED_CASH,
       0},
      {0,
       0,
       0,
       0,
       CR_PRICING_REVERSED_CHARGE,
       CATALOG_RETURNS,
       CR_PRICING_REVERSED_CHARGE,
       0},
      {0,
       0,
       0,
       0,
       CR_PRICING_STORE_CREDIT,
       CATALOG_RETURNS,
       CR_PRICING_STORE_CREDIT,
       0},
      {0,
       0,
       0,
       0,
       CR_PRICING_NET_LOSS,
       CATALOG_RETURNS,
       CR_PRICING_NET_LOSS,
       0},
      {0, 28, 0, 0, CR_NULLS, CATALOG_RETURNS, CR_NULLS, 0},
      {0, 70, 0, 0, CR_PRICING, CATALOG_RETURNS, CR_PRICING, 0},
      {0, 1, 0, 0, CS_SOLD_DATE_SK, CATALOG_SALES, CS_SOLD_DATE_SK, 0},
      {0, 2, 0, 0, CS_SOLD_TIME_SK, CATALOG_SALES, CS_SOLD_TIME_SK, 0},
      {0, 14, 0, 0, CS_SHIP_DATE_SK, CATALOG_SALES, CS_SHIP_DATE_SK, 0},
      {0, 1, 0, 0, CS_BILL_CUSTOMER_SK, CATALOG_SALES, CS_BILL_CUSTOMER_SK, 0},
      {0, 1, 0, 0, CS_BILL_CDEMO_SK, CATALOG_SALES, CS_BILL_CDEMO_SK, 0},
      {0, 1, 0, 0, CS_BILL_HDEMO_SK, CATALOG_SALES, CS_BILL_HDEMO_SK, 0},
      {0, 1, 0, 0, CS_BILL_ADDR_SK, CATALOG_SALES, CS_BILL_ADDR_SK, 0},
      {0, 2, 0, 0, CS_SHIP_CUSTOMER_SK, CATALOG_SALES, CS_SHIP_CUSTOMER_SK, 0},
      {0, 1, 0, 0, CS_SHIP_CDEMO_SK, CATALOG_SALES, CS_SHIP_CDEMO_SK, 0},
      {0, 1, 0, 0, CS_SHIP_HDEMO_SK, CATALOG_SALES, CS_SHIP_HDEMO_SK, 0},
      {0, 1, 0, 0, CS_SHIP_ADDR_SK, CATALOG_SALES, CS_SHIP_ADDR_SK, 0},
      {0, 1, 0, 0, CS_CALL_CENTER_SK, CATALOG_SALES, CS_CALL_CENTER_SK, 0},
      {0, 42, 0, 0, CS_CATALOG_PAGE_SK, CATALOG_SALES, CS_CATALOG_PAGE_SK, 0},
      {0, 14, 0, 0, CS_SHIP_MODE_SK, CATALOG_SALES, CS_SHIP_MODE_SK, 0},
      {0, 14, 0, 0, CS_WAREHOUSE_SK, CATALOG_SALES, CS_WAREHOUSE_SK, 0},
      {0, 1, 0, 0, CS_SOLD_ITEM_SK, CATALOG_SALES, CS_SOLD_ITEM_SK, 0},
      {0, 14, 0, 0, CS_PROMO_SK, CATALOG_SALES, CS_PROMO_SK, 0},
      {0, 1, 0, 0, CS_ORDER_NUMBER, CATALOG_SALES, CS_ORDER_NUMBER, 0},
      {0, 0, 0, 0, CS_PRICING_QUANTITY, CATALOG_SALES, CS_PRICING_QUANTITY, 0},
      {0,
       0,
       0,
       0,
       CS_PRICING_WHOLESALE_COST,
       CATALOG_SALES,
       CS_PRICING_WHOLESALE_COST,
       0},
      {0,
       0,
       0,
       0,
       CS_PRICING_LIST_PRICE,
       CATALOG_SALES,
       CS_PRICING_LIST_PRICE,
       0},
      {0,
       0,
       0,
       0,
       CS_PRICING_SALES_PRICE,
       CATALOG_SALES,
       CS_PRICING_SALES_PRICE,
       0},
      {0,
       0,
       0,
       0,
       CS_PRICING_COUPON_AMT,
       CATALOG_SALES,
       CS_PRICING_COUPON_AMT,
       0},
      {0,
       0,
       0,
       0,
       CS_PRICING_EXT_SALES_PRICE,
       CATALOG_SALES,
       CS_PRICING_EXT_SALES_PRICE,
       0},
      {0,
       0,
       0,
       0,
       CS_PRICING_EXT_DISCOUNT_AMOUNT,
       CATALOG_SALES,
       CS_PRICING_EXT_DISCOUNT_AMOUNT,
       0},
      {0,
       0,
       0,
       0,
       CS_PRICING_EXT_WHOLESALE_COST,
       CATALOG_SALES,
       CS_PRICING_EXT_WHOLESALE_COST,
       0},
      {0,
       0,
       0,
       0,
       CS_PRICING_EXT_LIST_PRICE,
       CATALOG_SALES,
       CS_PRICING_EXT_LIST_PRICE,
       0},
      {0, 0, 0, 0, CS_PRICING_EXT_TAX, CATALOG_SALES, CS_PRICING_EXT_TAX, 0},
      {0,
       0,
       0,
       0,
       CS_PRICING_EXT_SHIP_COST,
       CATALOG_SALES,
       CS_PRICING_EXT_SHIP_COST,
       0},
      {0, 0, 0, 0, CS_PRICING_NET_PAID, CATALOG_SALES, CS_PRICING_NET_PAID, 0},
      {0,
       0,
       0,
       0,
       CS_PRICING_NET_PAID_INC_TAX,
       CATALOG_SALES,
       CS_PRICING_NET_PAID_INC_TAX,
       0},
      {0,
       0,
       0,
       0,
       CS_PRICING_NET_PAID_INC_SHIP,
       CATALOG_SALES,
       CS_PRICING_NET_PAID_INC_SHIP,
       0},
      {0,
       0,
       0,
       0,
       CS_PRICING_NET_PAID_INC_SHIP_TAX,
       CATALOG_SALES,
       CS_PRICING_NET_PAID_INC_SHIP_TAX,
       0},
      {0,
       0,
       0,
       0,
       CS_PRICING_NET_PROFIT,
       CATALOG_SALES,
       CS_PRICING_NET_PROFIT,
       0},
      {0, 112, 0, 0, CS_PRICING, CATALOG_SALES, CS_PRICING, 0},
      {0, 0, 0, 0, CS_PERMUTE, CATALOG_SALES, CS_PERMUTE, 0},
      {0, 28, 0, 0, CS_NULLS, CATALOG_SALES, CS_NULLS, 0},
      {0, 14, 0, 0, CR_IS_RETURNED, CATALOG_SALES, CR_IS_RETURNED, 0},
      {0, 0, 0, 0, CS_PERMUTATION, CATALOG_SALES, CS_PERMUTATION, 0},
      {0, 1, 0, 0, C_CUSTOMER_SK, CUSTOMER, C_CUSTOMER_SK, 0},
      {0, 1, 0, 0, C_CUSTOMER_ID, CUSTOMER, C_CUSTOMER_ID, 0},
      {0, 1, 0, 0, C_CURRENT_CDEMO_SK, CUSTOMER, C_CURRENT_CDEMO_SK, 0},
      {0, 1, 0, 0, C_CURRENT_HDEMO_SK, CUSTOMER, C_CURRENT_HDEMO_SK, 0},
      {0, 1, 0, 0, C_CURRENT_ADDR_SK, CUSTOMER, C_CURRENT_ADDR_SK, 0},
      {0, 0, 0, 0, C_FIRST_SHIPTO_DATE_ID, CUSTOMER, C_FIRST_SHIPTO_DATE_ID, 0},
      {0, 1, 0, 0, C_FIRST_SALES_DATE_ID, CUSTOMER, C_FIRST_SALES_DATE_ID, 0},
      {0, 1, 0, 0, C_SALUTATION, CUSTOMER, C_SALUTATION, 0},
      {0, 1, 0, 0, C_FIRST_NAME, CUSTOMER, C_FIRST_NAME, 0},
      {0, 1, 0, 0, C_LAST_NAME, CUSTOMER, C_LAST_NAME, 0},
      {0, 2, 0, 0, C_PREFERRED_CUST_FLAG, CUSTOMER, C_PREFERRED_CUST_FLAG, 0},
      {0, 1, 0, 0, C_BIRTH_DAY, CUSTOMER, C_BIRTH_DAY, 0},
      {0, 0, 0, 0, C_BIRTH_MONTH, CUSTOMER, C_BIRTH_MONTH, 0},
      {0, 0, 0, 0, C_BIRTH_YEAR, CUSTOMER, C_BIRTH_YEAR, 0},
      {0, 1, 0, 0, C_BIRTH_COUNTRY, CUSTOMER, C_BIRTH_COUNTRY, 0},
      {0, 1, 0, 0, C_LOGIN, CUSTOMER, C_LOGIN, 0},
      {0, 23, 0, 0, C_EMAIL_ADDRESS, CUSTOMER, C_EMAIL_ADDRESS, 0},
      {0, 1, 0, 0, C_LAST_REVIEW_DATE, CUSTOMER, C_LAST_REVIEW_DATE, 0},
      {0, 2, 0, 0, C_NULLS, CUSTOMER, C_NULLS, 0},
      {0, 1, 0, 0, CA_ADDRESS_SK, CUSTOMER_ADDRESS, CA_ADDRESS_SK, 0},
      {0, 1, 0, 0, CA_ADDRESS_ID, CUSTOMER_ADDRESS, CA_ADDRESS_ID, 0},
      {0,
       1,
       0,
       0,
       CA_ADDRESS_STREET_NUM,
       CUSTOMER_ADDRESS,
       CA_ADDRESS_STREET_NUM,
       0},
      {0,
       1,
       0,
       0,
       CA_ADDRESS_STREET_NAME1,
       CUSTOMER_ADDRESS,
       CA_ADDRESS_STREET_NAME1,
       0},
      {0,
       1,
       0,
       0,
       CA_ADDRESS_STREET_TYPE,
       CUSTOMER_ADDRESS,
       CA_ADDRESS_STREET_TYPE,
       0},
      {0,
       1,
       0,
       0,
       CA_ADDRESS_SUITE_NUM,
       CUSTOMER_ADDRESS,
       CA_ADDRESS_SUITE_NUM,
       0},
      {0, 1, 0, 0, CA_ADDRESS_CITY, CUSTOMER_ADDRESS, CA_ADDRESS_CITY, 0},
      {0, 1, 0, 0, CA_ADDRESS_COUNTY, CUSTOMER_ADDRESS, CA_ADDRESS_COUNTY, 0},
      {0, 1, 0, 0, CA_ADDRESS_STATE, CUSTOMER_ADDRESS, CA_ADDRESS_STATE, 0},
      {0, 1, 0, 0, CA_ADDRESS_ZIP, CUSTOMER_ADDRESS, CA_ADDRESS_ZIP, 0},
      {0, 1, 0, 0, CA_ADDRESS_COUNTRY, CUSTOMER_ADDRESS, CA_ADDRESS_COUNTRY, 0},
      {0,
       1,
       0,
       0,
       CA_ADDRESS_GMT_OFFSET,
       CUSTOMER_ADDRESS,
       CA_ADDRESS_GMT_OFFSET,
       0},
      {0, 1, 0, 0, CA_LOCATION_TYPE, CUSTOMER_ADDRESS, CA_LOCATION_TYPE, 0},
      {0, 2, 0, 0, CA_NULLS, CUSTOMER_ADDRESS, CA_NULLS, 0},
      {0, 7, 0, 0, CA_ADDRESS, CUSTOMER_ADDRESS, CA_ADDRESS, 0},
      {0,
       1,
       0,
       0,
       CA_ADDRESS_STREET_NAME2,
       CUSTOMER_ADDRESS,
       CA_ADDRESS_STREET_NAME2,
       0},
      {0, 1, 0, 0, CD_DEMO_SK, CUSTOMER_DEMOGRAPHICS, CD_DEMO_SK, 0},
      {0, 1, 0, 0, CD_GENDER, CUSTOMER_DEMOGRAPHICS, CD_GENDER, 0},
      {0,
       1,
       0,
       0,
       CD_MARITAL_STATUS,
       CUSTOMER_DEMOGRAPHICS,
       CD_MARITAL_STATUS,
       0},
      {0,
       1,
       0,
       0,
       CD_EDUCATION_STATUS,
       CUSTOMER_DEMOGRAPHICS,
       CD_EDUCATION_STATUS,
       0},
      {0,
       1,
       0,
       0,
       CD_PURCHASE_ESTIMATE,
       CUSTOMER_DEMOGRAPHICS,
       CD_PURCHASE_ESTIMATE,
       0},
      {0,
       1,
       0,
       0,
       CD_CREDIT_RATING,
       CUSTOMER_DEMOGRAPHICS,
       CD_CREDIT_RATING,
       0},
      {0, 1, 0, 0, CD_DEP_COUNT, CUSTOMER_DEMOGRAPHICS, CD_DEP_COUNT, 0},
      {0,
       1,
       0,
       0,
       CD_DEP_EMPLOYED_COUNT,
       CUSTOMER_DEMOGRAPHICS,
       CD_DEP_EMPLOYED_COUNT,
       0},
      {0,
       1,
       0,
       0,
       CD_DEP_COLLEGE_COUNT,
       CUSTOMER_DEMOGRAPHICS,
       CD_DEP_COLLEGE_COUNT,
       0},
      {0, 2, 0, 0, CD_NULLS, CUSTOMER_DEMOGRAPHICS, CD_NULLS, 0},
      {0, 0, 0, 0, D_DATE_SK, DATET, D_DATE_SK, 0},
      {0, 0, 0, 0, D_DATE_ID, DATET, D_DATE_ID, 0},
      {0, 0, 0, 0, D_DATE, DATET, D_DATE, 0},
      {0, 0, 0, 0, D_MONTH_SEQ, DATET, D_MONTH_SEQ, 0},
      {0, 0, 0, 0, D_WEEK_SEQ, DATET, D_WEEK_SEQ, 0},
      {0, 0, 0, 0, D_QUARTER_SEQ, DATET, D_QUARTER_SEQ, 0},
      {0, 0, 0, 0, D_YEAR, DATET, D_YEAR, 0},
      {0, 0, 0, 0, D_DOW, DATET, D_DOW, 0},
      {0, 0, 0, 0, D_MOY, DATET, D_MOY, 0},
      {0, 0, 0, 0, D_DOM, DATET, D_DOM, 0},
      {0, 0, 0, 0, D_QOY, DATET, D_QOY, 0},
      {0, 0, 0, 0, D_FY_YEAR, DATET, D_FY_YEAR, 0},
      {0, 0, 0, 0, D_FY_QUARTER_SEQ, DATET, D_FY_QUARTER_SEQ, 0},
      {0, 0, 0, 0, D_FY_WEEK_SEQ, DATET, D_FY_WEEK_SEQ, 0},
      {0, 0, 0, 0, D_DAY_NAME, DATET, D_DAY_NAME, 0},
      {0, 0, 0, 0, D_QUARTER_NAME, DATET, D_QUARTER_NAME, 0},
      {0, 0, 0, 0, D_HOLIDAY, DATET, D_HOLIDAY, 0},
      {0, 0, 0, 0, D_WEEKEND, DATET, D_WEEKEND, 0},
      {0, 0, 0, 0, D_FOLLOWING_HOLIDAY, DATET, D_FOLLOWING_HOLIDAY, 0},
      {0, 0, 0, 0, D_FIRST_DOM, DATET, D_FIRST_DOM, 0},
      {0, 0, 0, 0, D_LAST_DOM, DATET, D_LAST_DOM, 0},
      {0, 0, 0, 0, D_SAME_DAY_LY, DATET, D_SAME_DAY_LY, 0},
      {0, 0, 0, 0, D_SAME_DAY_LQ, DATET, D_SAME_DAY_LQ, 0},
      {0, 0, 0, 0, D_CURRENT_DAY, DATET, D_CURRENT_DAY, 0},
      {0, 0, 0, 0, D_CURRENT_WEEK, DATET, D_CURRENT_WEEK, 0},
      {0, 0, 0, 0, D_CURRENT_MONTH, DATET, D_CURRENT_MONTH, 0},
      {0, 0, 0, 0, D_CURRENT_QUARTER, DATET, D_CURRENT_QUARTER, 0},
      {0, 0, 0, 0, D_CURRENT_YEAR, DATET, D_CURRENT_YEAR, 0},
      {0, 2, 0, 0, D_NULLS, DATET, D_NULLS, 0},
      {0, 1, 0, 0, HD_DEMO_SK, HOUSEHOLD_DEMOGRAPHICS, HD_DEMO_SK, 0},
      {0,
       1,
       0,
       0,
       HD_INCOME_BAND_ID,
       HOUSEHOLD_DEMOGRAPHICS,
       HD_INCOME_BAND_ID,
       0},
      {0,
       1,
       0,
       0,
       HD_BUY_POTENTIAL,
       HOUSEHOLD_DEMOGRAPHICS,
       HD_BUY_POTENTIAL,
       0},
      {0, 1, 0, 0, HD_DEP_COUNT, HOUSEHOLD_DEMOGRAPHICS, HD_DEP_COUNT, 0},
      {0,
       1,
       0,
       0,
       HD_VEHICLE_COUNT,
       HOUSEHOLD_DEMOGRAPHICS,
       HD_VEHICLE_COUNT,
       0},
      {0, 2, 0, 0, HD_NULLS, HOUSEHOLD_DEMOGRAPHICS, HD_NULLS, 0},
      {0, 1, 0, 0, IB_INCOME_BAND_ID, INCOME_BAND, IB_INCOME_BAND_ID, 0},
      {0, 1, 0, 0, IB_LOWER_BOUND, INCOME_BAND, IB_LOWER_BOUND, 0},
      {0, 1, 0, 0, IB_UPPER_BOUND, INCOME_BAND, IB_UPPER_BOUND, 0},
      {0, 2, 0, 0, IB_NULLS, INCOME_BAND, IB_NULLS, 0},
      {0, 1, 0, 0, INV_DATE_SK, INVENTORY, INV_DATE_SK, 0},
      {0, 1, 0, 0, INV_ITEM_SK, INVENTORY, INV_ITEM_SK, 0},
      {0, 1, 0, 0, INV_WAREHOUSE_SK, INVENTORY, INV_WAREHOUSE_SK, 0},
      {0, 1, 0, 0, INV_QUANTITY_ON_HAND, INVENTORY, INV_QUANTITY_ON_HAND, 0},
      {0, 2, 0, 0, INV_NULLS, INVENTORY, INV_NULLS, 0},
      {0, 1, 0, 0, I_ITEM_SK, ITEM, I_ITEM_SK, 0},
      {0, 1, 0, 0, I_ITEM_ID, ITEM, I_ITEM_ID, 0},
      {0, 1, 0, 0, I_REC_START_DATE_ID, ITEM, I_REC_START_DATE_ID, 0},
      {0, 2, 0, 0, I_REC_END_DATE_ID, ITEM, I_REC_END_DATE_ID, 0},
      {0, 200, 0, 0, I_ITEM_DESC, ITEM, S_ITEM_DESC, 0},
      {0, 2, 0, 0, I_CURRENT_PRICE, ITEM, I_CURRENT_PRICE, 0},
      {0, 1, 0, 0, I_WHOLESALE_COST, ITEM, I_WHOLESALE_COST, 0},
      {0, 1, 0, 0, I_BRAND_ID, ITEM, I_BRAND_ID, 0},
      {0, 1, 0, 0, I_BRAND, ITEM, I_BRAND, 0},
      {0, 1, 0, 0, I_CLASS_ID, ITEM, I_CLASS_ID, 0},
      {0, 1, 0, 0, I_CLASS, ITEM, I_CLASS, 0},
      {0, 1, 0, 0, I_CATEGORY_ID, ITEM, I_CATEGORY_ID, 0},
      {0, 1, 0, 0, I_CATEGORY, ITEM, I_CATEGORY, 0},
      {0, 2, 0, 0, I_MANUFACT_ID, ITEM, I_MANUFACT_ID, 0},
      {0, 1, 0, 0, I_MANUFACT, ITEM, I_MANUFACT, 0},
      {0, 1, 0, 0, I_SIZE, ITEM, I_SIZE, 0},
      {0, 50, 0, 0, I_FORMULATION, ITEM, I_FORMULATION, 0},
      {0, 1, 0, 0, I_COLOR, ITEM, I_COLOR, 0},
      {0, 1, 0, 0, I_UNITS, ITEM, I_UNITS, 0},
      {0, 1, 0, 0, I_CONTAINER, ITEM, I_CONTAINER, 0},
      {0, 2, 0, 0, I_MANAGER_ID, ITEM, I_MANAGER_ID, 0},
      {0, 1, 0, 0, I_PRODUCT_NAME, ITEM, I_PRODUCT_NAME, 0},
      {0, 2, 0, 0, I_NULLS, ITEM, I_NULLS, 0},
      {0, 1, 0, 0, I_SCD, ITEM, I_SCD, 0},
      {0, 2, 0, 0, I_PROMO_SK, ITEM, I_PROMO_SK, 0},
      {0, 1, 0, 0, P_PROMO_SK, PROMOTION, P_PROMO_SK, 0},
      {0, 1, 0, 0, P_PROMO_ID, PROMOTION, P_PROMO_ID, 0},
      {0, 1, 0, 0, P_START_DATE_ID, PROMOTION, P_START_DATE_ID, 0},
      {0, 1, 0, 0, P_END_DATE_ID, PROMOTION, P_END_DATE_ID, 0},
      {0, 1, 0, 0, P_ITEM_SK, PROMOTION, P_ITEM_SK, 0},
      {0, 1, 0, 0, P_COST, PROMOTION, P_COST, 0},
      {0, 1, 0, 0, P_RESPONSE_TARGET, PROMOTION, P_RESPONSE_TARGET, 0},
      {0, 1, 0, 0, P_PROMO_NAME, PROMOTION, P_PROMO_NAME, 0},
      {0, 1, 0, 0, P_CHANNEL_DMAIL, PROMOTION, P_CHANNEL_DMAIL, 0},
      {0, 1, 0, 0, P_CHANNEL_EMAIL, PROMOTION, P_CHANNEL_EMAIL, 0},
      {0, 1, 0, 0, P_CHANNEL_CATALOG, PROMOTION, P_CHANNEL_CATALOG, 0},
      {0, 1, 0, 0, P_CHANNEL_TV, PROMOTION, P_CHANNEL_TV, 0},
      {0, 1, 0, 0, P_CHANNEL_RADIO, PROMOTION, P_CHANNEL_RADIO, 0},
      {0, 1, 0, 0, P_CHANNEL_PRESS, PROMOTION, P_CHANNEL_PRESS, 0},
      {0, 1, 0, 0, P_CHANNEL_EVENT, PROMOTION, P_CHANNEL_EVENT, 0},
      {0, 1, 0, 0, P_CHANNEL_DEMO, PROMOTION, P_CHANNEL_DEMO, 0},
      {0, 100, 0, 0, P_CHANNEL_DETAILS, PROMOTION, P_CHANNEL_DETAILS, 0},
      {0, 1, 0, 0, P_PURPOSE, PROMOTION, P_PURPOSE, 0},
      {0, 1, 0, 0, P_DISCOUNT_ACTIVE, PROMOTION, P_DISCOUNT_ACTIVE, 0},
      {0, 2, 0, 0, P_NULLS, PROMOTION, P_NULLS, 0},
      {0, 1, 0, 0, R_REASON_SK, REASON, R_REASON_SK, 0},
      {0, 1, 0, 0, R_REASON_ID, REASON, R_REASON_ID, 0},
      {0, 1, 0, 0, R_REASON_DESCRIPTION, REASON, R_REASON_DESCRIPTION, 0},
      {0, 2, 0, 0, R_NULLS, REASON, R_NULLS, 0},
      {0, 1, 0, 0, SM_SHIP_MODE_SK, SHIP_MODE, SM_SHIP_MODE_SK, 0},
      {0, 1, 0, 0, SM_SHIP_MODE_ID, SHIP_MODE, SM_SHIP_MODE_ID, 0},
      {0, 1, 0, 0, SM_TYPE, SHIP_MODE, SM_TYPE, 0},
      {0, 1, 0, 0, SM_CODE, SHIP_MODE, SM_CODE, 0},
      {0, 21, 0, 0, SM_CONTRACT, SHIP_MODE, SM_CONTRACT, 0},
      {0, 1, 0, 0, SM_CARRIER, SHIP_MODE, SM_CARRIER, 0},
      {0, 2, 0, 0, SM_NULLS, SHIP_MODE, SM_NULLS, 0},
      {0, 1, 0, 0, W_STORE_SK, STORE, W_STORE_SK, 0},
      {0, 1, 0, 0, W_STORE_ID, STORE, W_STORE_ID, 0},
      {0,
       1,
       0,
       0,
       W_STORE_REC_START_DATE_ID,
       STORE,
       W_STORE_REC_START_DATE_ID,
       0},
      {0, 2, 0, 0, W_STORE_REC_END_DATE_ID, STORE, W_STORE_REC_END_DATE_ID, 0},
      {0, 2, 0, 0, W_STORE_CLOSED_DATE_ID, STORE, W_STORE_CLOSED_DATE_ID, 0},
      {0, 0, 0, 0, W_STORE_NAME, STORE, W_STORE_NAME, 0},
      {0, 1, 0, 0, W_STORE_EMPLOYEES, STORE, W_STORE_EMPLOYEES, 0},
      {0, 1, 0, 0, W_STORE_FLOOR_SPACE, STORE, W_STORE_FLOOR_SPACE, 0},
      {0, 1, 0, 0, W_STORE_HOURS, STORE, W_STORE_HOURS, 0},
      {0, 2, 0, 0, W_STORE_MANAGER, STORE, W_STORE_MANAGER, 0},
      {0, 1, 0, 0, W_STORE_MARKET_ID, STORE, W_STORE_MARKET_ID, 0},
      {0, 1, 0, 0, W_STORE_TAX_PERCENTAGE, STORE, W_STORE_TAX_PERCENTAGE, 0},
      {0, 1, 0, 0, W_STORE_GEOGRAPHY_CLASS, STORE, W_STORE_GEOGRAPHY_CLASS, 0},
      {0, 100, 0, 0, W_STORE_MARKET_DESC, STORE, W_STORE_MARKET_DESC, 0},
      {0, 2, 0, 0, W_STORE_MARKET_MANAGER, STORE, W_STORE_MARKET_MANAGER, 0},
      {0, 1, 0, 0, W_STORE_DIVISION_ID, STORE, W_STORE_DIVISION_ID, 0},
      {0, 1, 0, 0, W_STORE_DIVISION_NAME, STORE, W_STORE_DIVISION_NAME, 0},
      {0, 1, 0, 0, W_STORE_COMPANY_ID, STORE, W_STORE_COMPANY_ID, 0},
      {0, 1, 0, 0, W_STORE_COMPANY_NAME, STORE, W_STORE_COMPANY_NAME, 0},
      {0,
       1,
       0,
       0,
       W_STORE_ADDRESS_STREET_NUM,
       STORE,
       W_STORE_ADDRESS_STREET_NUM,
       0},
      {0,
       1,
       0,
       0,
       W_STORE_ADDRESS_STREET_NAME1,
       STORE,
       W_STORE_ADDRESS_STREET_NAME1,
       0},
      {0,
       1,
       0,
       0,
       W_STORE_ADDRESS_STREET_TYPE,
       STORE,
       W_STORE_ADDRESS_STREET_TYPE,
       0},
      {0,
       1,
       0,
       0,
       W_STORE_ADDRESS_SUITE_NUM,
       STORE,
       W_STORE_ADDRESS_SUITE_NUM,
       0},
      {0, 1, 0, 0, W_STORE_ADDRESS_CITY, STORE, W_STORE_ADDRESS_CITY, 0},
      {0, 1, 0, 0, W_STORE_ADDRESS_COUNTY, STORE, W_STORE_ADDRESS_COUNTY, 0},
      {0, 1, 0, 0, W_STORE_ADDRESS_STATE, STORE, W_STORE_ADDRESS_STATE, 0},
      {0, 1, 0, 0, W_STORE_ADDRESS_ZIP, STORE, W_STORE_ADDRESS_ZIP, 0},
      {0, 1, 0, 0, W_STORE_ADDRESS_COUNTRY, STORE, W_STORE_ADDRESS_COUNTRY, 0},
      {0,
       1,
       0,
       0,
       W_STORE_ADDRESS_GMT_OFFSET,
       STORE,
       W_STORE_ADDRESS_GMT_OFFSET,
       0},
      {0, 2, 0, 0, W_STORE_NULLS, STORE, W_STORE_NULLS, 0},
      {0, 1, 0, 0, W_STORE_TYPE, STORE, W_STORE_TYPE, 0},
      {0, 1, 0, 0, W_STORE_SCD, STORE, W_STORE_SCD, 0},
      {0, 7, 0, 0, W_STORE_ADDRESS, STORE, W_STORE_ADDRESS, 0},
      {0, 32, 0, 0, SR_RETURNED_DATE_SK, STORE_RETURNS, SR_RETURNED_DATE_SK, 0},
      {0, 32, 0, 0, SR_RETURNED_TIME_SK, STORE_RETURNS, SR_RETURNED_TIME_SK, 0},
      {0, 16, 0, 0, SR_ITEM_SK, STORE_RETURNS, SR_ITEM_SK, 0},
      {0, 16, 0, 0, SR_CUSTOMER_SK, STORE_RETURNS, SR_CUSTOMER_SK, 0},
      {0, 16, 0, 0, SR_CDEMO_SK, STORE_RETURNS, SR_CDEMO_SK, 0},
      {0, 16, 0, 0, SR_HDEMO_SK, STORE_RETURNS, SR_HDEMO_SK, 0},
      {0, 16, 0, 0, SR_ADDR_SK, STORE_RETURNS, SR_ADDR_SK, 0},
      {0, 16, 0, 0, SR_STORE_SK, STORE_RETURNS, SR_STORE_SK, 0},
      {0, 16, 0, 0, SR_REASON_SK, STORE_RETURNS, SR_REASON_SK, 0},
      {0, 16, 0, 0, SR_TICKET_NUMBER, STORE_RETURNS, SR_TICKET_NUMBER, 0},
      {0, 0, 0, 0, SR_PRICING_QUANTITY, STORE_RETURNS, SR_PRICING_QUANTITY, 0},
      {0, 0, 0, 0, SR_PRICING_NET_PAID, STORE_RETURNS, SR_PRICING_NET_PAID, 0},
      {0, 0, 0, 0, SR_PRICING_EXT_TAX, STORE_RETURNS, SR_PRICING_EXT_TAX, 0},
      {0,
       0,
       0,
       0,
       SR_PRICING_NET_PAID_INC_TAX,
       STORE_RETURNS,
       SR_PRICING_NET_PAID_INC_TAX,
       0},
      {0, 0, 0, 0, SR_PRICING_FEE, STORE_RETURNS, SR_PRICING_FEE, 0},
      {0,
       0,
       0,
       0,
       SR_PRICING_EXT_SHIP_COST,
       STORE_RETURNS,
       SR_PRICING_EXT_SHIP_COST,
       0},
      {0,
       0,
       0,
       0,
       SR_PRICING_REFUNDED_CASH,
       STORE_RETURNS,
       SR_PRICING_REFUNDED_CASH,
       0},
      {0,
       0,
       0,
       0,
       SR_PRICING_REVERSED_CHARGE,
       STORE_RETURNS,
       SR_PRICING_REVERSED_CHARGE,
       0},
      {0,
       0,
       0,
       0,
       SR_PRICING_STORE_CREDIT,
       STORE_RETURNS,
       SR_PRICING_STORE_CREDIT,
       0},
      {0, 0, 0, 0, SR_PRICING_NET_LOSS, STORE_RETURNS, SR_PRICING_NET_LOSS, 0},
      {0, 80, 0, 0, SR_PRICING, STORE_RETURNS, SR_PRICING, 0},
      {0, 32, 0, 0, SR_NULLS, STORE_RETURNS, SR_NULLS, 0},
      {0, 2, 0, 0, SS_SOLD_DATE_SK, STORE_SALES, SS_SOLD_DATE_SK, 0},
      {0, 2, 0, 0, SS_SOLD_TIME_SK, STORE_SALES, SS_SOLD_TIME_SK, 0},
      {0, 1, 0, 0, SS_SOLD_ITEM_SK, STORE_SALES, SS_SOLD_ITEM_SK, 0},
      {0, 1, 0, 0, SS_SOLD_CUSTOMER_SK, STORE_SALES, SS_SOLD_CUSTOMER_SK, 0},
      {0, 1, 0, 0, SS_SOLD_CDEMO_SK, STORE_SALES, SS_SOLD_CDEMO_SK, 0},
      {0, 1, 0, 0, SS_SOLD_HDEMO_SK, STORE_SALES, SS_SOLD_HDEMO_SK, 0},
      {0, 1, 0, 0, SS_SOLD_ADDR_SK, STORE_SALES, SS_SOLD_ADDR_SK, 0},
      {0, 1, 0, 0, SS_SOLD_STORE_SK, STORE_SALES, SS_SOLD_STORE_SK, 0},
      {0, 16, 0, 0, SS_SOLD_PROMO_SK, STORE_SALES, SS_SOLD_PROMO_SK, 0},
      {0, 1, 0, 0, SS_TICKET_NUMBER, STORE_SALES, SS_TICKET_NUMBER, 0},
      {0, 1, 0, 0, SS_PRICING_QUANTITY, STORE_SALES, SS_PRICING_QUANTITY, 0},
      {0,
       0,
       0,
       0,
       SS_PRICING_WHOLESALE_COST,
       STORE_SALES,
       SS_PRICING_WHOLESALE_COST,
       0},
      {0,
       0,
       0,
       0,
       SS_PRICING_LIST_PRICE,
       STORE_SALES,
       SS_PRICING_LIST_PRICE,
       0},
      {0,
       0,
       0,
       0,
       SS_PRICING_SALES_PRICE,
       STORE_SALES,
       SS_PRICING_SALES_PRICE,
       0},
      {0,
       0,
       0,
       0,
       SS_PRICING_COUPON_AMT,
       STORE_SALES,
       SS_PRICING_COUPON_AMT,
       0},
      {0,
       0,
       0,
       0,
       SS_PRICING_EXT_SALES_PRICE,
       STORE_SALES,
       SS_PRICING_EXT_SALES_PRICE,
       0},
      {0,
       0,
       0,
       0,
       SS_PRICING_EXT_WHOLESALE_COST,
       STORE_SALES,
       SS_PRICING_EXT_WHOLESALE_COST,
       0},
      {0,
       0,
       0,
       0,
       SS_PRICING_EXT_LIST_PRICE,
       STORE_SALES,
       SS_PRICING_EXT_LIST_PRICE,
       0},
      {0, 0, 0, 0, SS_PRICING_EXT_TAX, STORE_SALES, SS_PRICING_EXT_TAX, 0},
      {0, 0, 0, 0, SS_PRICING_NET_PAID, STORE_SALES, SS_PRICING_NET_PAID, 0},
      {0,
       0,
       0,
       0,
       SS_PRICING_NET_PAID_INC_TAX,
       STORE_SALES,
       SS_PRICING_NET_PAID_INC_TAX,
       0},
      {0,
       0,
       0,
       0,
       SS_PRICING_NET_PROFIT,
       STORE_SALES,
       SS_PRICING_NET_PROFIT,
       0},
      {0, 16, 0, 0, SR_IS_RETURNED, STORE_SALES, SR_IS_RETURNED, 0},
      {0, 128, 0, 0, SS_PRICING, STORE_SALES, SS_PRICING, 0},
      {0, 32, 0, 0, SS_NULLS, STORE_SALES, SS_NULLS, 0},
      {0, 0, 0, 0, SS_PERMUTATION, STORE_SALES, SS_PERMUTATION, 0},
      {0, 1, 0, 0, T_TIME_SK, TIMET, T_TIME_SK, 0},
      {0, 1, 0, 0, T_TIME_ID, TIMET, T_TIME_ID, 0},
      {0, 1, 0, 0, T_TIME, TIMET, T_TIME, 0},
      {0, 1, 0, 0, T_HOUR, TIMET, T_HOUR, 0},
      {0, 1, 0, 0, T_MINUTE, TIMET, T_MINUTE, 0},
      {0, 1, 0, 0, T_SECOND, TIMET, T_SECOND, 0},
      {0, 1, 0, 0, T_AM_PM, TIMET, T_AM_PM, 0},
      {0, 1, 0, 0, T_SHIFT, TIMET, T_SHIFT, 0},
      {0, 1, 0, 0, T_SUB_SHIFT, TIMET, T_SUB_SHIFT, 0},
      {0, 1, 0, 0, T_MEAL_TIME, TIMET, T_MEAL_TIME, 0},
      {0, 2, 0, 0, T_NULLS, TIMET, T_NULLS, 0},
      {0, 1, 0, 0, W_WAREHOUSE_SK, WAREHOUSE, W_WAREHOUSE_SK, 0},
      {0, 1, 0, 0, W_WAREHOUSE_ID, WAREHOUSE, W_WAREHOUSE_ID, 0},
      {0, 80, 0, 0, W_WAREHOUSE_NAME, WAREHOUSE, W_WAREHOUSE_NAME, 0},
      {0, 1, 0, 0, W_WAREHOUSE_SQ_FT, WAREHOUSE, W_WAREHOUSE_SQ_FT, 0},
      {0, 1, 0, 0, W_ADDRESS_STREET_NUM, WAREHOUSE, W_ADDRESS_STREET_NUM, 0},
      {0,
       1,
       0,
       0,
       W_ADDRESS_STREET_NAME1,
       WAREHOUSE,
       W_ADDRESS_STREET_NAME1,
       0},
      {0, 1, 0, 0, W_ADDRESS_STREET_TYPE, WAREHOUSE, W_ADDRESS_STREET_TYPE, 0},
      {0, 1, 0, 0, W_ADDRESS_SUITE_NUM, WAREHOUSE, W_ADDRESS_SUITE_NUM, 0},
      {0, 1, 0, 0, W_ADDRESS_CITY, WAREHOUSE, W_ADDRESS_CITY, 0},
      {0, 1, 0, 0, W_ADDRESS_COUNTY, WAREHOUSE, W_ADDRESS_COUNTY, 0},
      {0, 1, 0, 0, W_ADDRESS_STATE, WAREHOUSE, W_ADDRESS_STATE, 0},
      {0, 1, 0, 0, W_ADDRESS_ZIP, WAREHOUSE, W_ADDRESS_ZIP, 0},
      {0, 1, 0, 0, W_ADDRESS_COUNTRY, WAREHOUSE, W_ADDRESS_COUNTRY, 0},
      {0, 1, 0, 0, W_ADDRESS_GMT_OFFSET, WAREHOUSE, W_ADDRESS_GMT_OFFSET, 0},
      {0, 2, 0, 0, W_NULLS, WAREHOUSE, W_NULLS, 0},
      {0, 7, 0, 0, W_WAREHOUSE_ADDRESS, WAREHOUSE, W_WAREHOUSE_ADDRESS, 0},
      {0, 1, 0, 0, WP_PAGE_SK, WEB_PAGE, WP_PAGE_SK, 0},
      {0, 1, 0, 0, WP_PAGE_ID, WEB_PAGE, WP_PAGE_ID, 0},
      {0, 1, 0, 0, WP_REC_START_DATE_ID, WEB_PAGE, WP_REC_START_DATE_ID, 0},
      {0, 1, 0, 0, WP_REC_END_DATE_ID, WEB_PAGE, WP_REC_END_DATE_ID, 0},
      {0, 2, 0, 0, WP_CREATION_DATE_SK, WEB_PAGE, WP_CREATION_DATE_SK, 0},
      {0, 1, 0, 0, WP_ACCESS_DATE_SK, WEB_PAGE, WP_ACCESS_DATE_SK, 0},
      {0, 1, 0, 0, WP_AUTOGEN_FLAG, WEB_PAGE, WP_AUTOGEN_FLAG, 0},
      {0, 1, 0, 0, WP_CUSTOMER_SK, WEB_PAGE, WP_CUSTOMER_SK, 0},
      {0, 1, 0, 0, WP_URL, WEB_PAGE, WP_URL, 0},
      {0, 1, 0, 0, WP_TYPE, WEB_PAGE, WP_TYPE, 0},
      {0, 1, 0, 0, WP_CHAR_COUNT, WEB_PAGE, WP_CHAR_COUNT, 0},
      {0, 1, 0, 0, WP_LINK_COUNT, WEB_PAGE, WP_LINK_COUNT, 0},
      {0, 1, 0, 0, WP_IMAGE_COUNT, WEB_PAGE, WP_IMAGE_COUNT, 0},
      {0, 1, 0, 0, WP_MAX_AD_COUNT, WEB_PAGE, WP_MAX_AD_COUNT, 0},
      {0, 2, 0, 0, WP_NULLS, WEB_PAGE, WP_NULLS, 0},
      {0, 1, 0, 0, WP_SCD, WEB_PAGE, WP_SCD, 0},
      {0, 32, 0, 0, WR_RETURNED_DATE_SK, WEB_RETURNS, WR_RETURNED_DATE_SK, 0},
      {0, 32, 0, 0, WR_RETURNED_TIME_SK, WEB_RETURNS, WR_RETURNED_TIME_SK, 0},
      {0, 16, 0, 0, WR_ITEM_SK, WEB_RETURNS, WR_ITEM_SK, 0},
      {0,
       16,
       0,
       0,
       WR_REFUNDED_CUSTOMER_SK,
       WEB_RETURNS,
       WR_REFUNDED_CUSTOMER_SK,
       0},
      {0, 16, 0, 0, WR_REFUNDED_CDEMO_SK, WEB_RETURNS, WR_REFUNDED_CDEMO_SK, 0},
      {0, 16, 0, 0, WR_REFUNDED_HDEMO_SK, WEB_RETURNS, WR_REFUNDED_HDEMO_SK, 0},
      {0, 16, 0, 0, WR_REFUNDED_ADDR_SK, WEB_RETURNS, WR_REFUNDED_ADDR_SK, 0},
      {0,
       16,
       0,
       0,
       WR_RETURNING_CUSTOMER_SK,
       WEB_RETURNS,
       WR_RETURNING_CUSTOMER_SK,
       0},
      {0,
       16,
       0,
       0,
       WR_RETURNING_CDEMO_SK,
       WEB_RETURNS,
       WR_RETURNING_CDEMO_SK,
       0},
      {0,
       16,
       0,
       0,
       WR_RETURNING_HDEMO_SK,
       WEB_RETURNS,
       WR_RETURNING_HDEMO_SK,
       0},
      {0, 16, 0, 0, WR_RETURNING_ADDR_SK, WEB_RETURNS, WR_RETURNING_ADDR_SK, 0},
      {0, 16, 0, 0, WR_WEB_PAGE_SK, WEB_RETURNS, WR_WEB_PAGE_SK, 0},
      {0, 16, 0, 0, WR_REASON_SK, WEB_RETURNS, WR_REASON_SK, 0},
      {0, 0, 0, 0, WR_ORDER_NUMBER, WEB_RETURNS, WR_ORDER_NUMBER, 0},
      {0, 0, 0, 0, WR_PRICING_QUANTITY, WEB_RETURNS, WR_PRICING_QUANTITY, 0},
      {0, 0, 0, 0, WR_PRICING_NET_PAID, WEB_RETURNS, WR_PRICING_NET_PAID, 0},
      {0, 0, 0, 0, WR_PRICING_EXT_TAX, WEB_RETURNS, WR_PRICING_EXT_TAX, 0},
      {0,
       0,
       0,
       0,
       WR_PRICING_NET_PAID_INC_TAX,
       WEB_RETURNS,
       WR_PRICING_NET_PAID_INC_TAX,
       0},
      {0, 0, 0, 0, WR_PRICING_FEE, WEB_RETURNS, WR_PRICING_FEE, 0},
      {0,
       0,
       0,
       0,
       WR_PRICING_EXT_SHIP_COST,
       WEB_RETURNS,
       WR_PRICING_EXT_SHIP_COST,
       0},
      {0,
       0,
       0,
       0,
       WR_PRICING_REFUNDED_CASH,
       WEB_RETURNS,
       WR_PRICING_REFUNDED_CASH,
       0},
      {0,
       0,
       0,
       0,
       WR_PRICING_REVERSED_CHARGE,
       WEB_RETURNS,
       WR_PRICING_REVERSED_CHARGE,
       0},
      {0,
       0,
       0,
       0,
       WR_PRICING_STORE_CREDIT,
       WEB_RETURNS,
       WR_PRICING_STORE_CREDIT,
       0},
      {0, 0, 0, 0, WR_PRICING_NET_LOSS, WEB_RETURNS, WR_PRICING_NET_LOSS, 0},
      {0, 80, 0, 0, WR_PRICING, WEB_RETURNS, WR_PRICING, 0},
      {0, 32, 0, 0, WR_NULLS, WEB_RETURNS, WR_NULLS, 0},
      {0, 2, 0, 0, WS_SOLD_DATE_SK, WEB_SALES, WS_SOLD_DATE_SK, 0},
      {0, 2, 0, 0, WS_SOLD_TIME_SK, WEB_SALES, WS_SOLD_TIME_SK, 0},
      {0, 16, 0, 0, WS_SHIP_DATE_SK, WEB_SALES, WS_SHIP_DATE_SK, 0},
      {0, 1, 0, 0, WS_ITEM_SK, WEB_SALES, WS_ITEM_SK, 0},
      {0, 1, 0, 0, WS_BILL_CUSTOMER_SK, WEB_SALES, WS_BILL_CUSTOMER_SK, 0},
      {0, 1, 0, 0, WS_BILL_CDEMO_SK, WEB_SALES, WS_BILL_CDEMO_SK, 0},
      {0, 1, 0, 0, WS_BILL_HDEMO_SK, WEB_SALES, WS_BILL_HDEMO_SK, 0},
      {0, 1, 0, 0, WS_BILL_ADDR_SK, WEB_SALES, WS_BILL_ADDR_SK, 0},
      {0, 2, 0, 0, WS_SHIP_CUSTOMER_SK, WEB_SALES, WS_SHIP_CUSTOMER_SK, 0},
      {0, 2, 0, 0, WS_SHIP_CDEMO_SK, WEB_SALES, WS_SHIP_CDEMO_SK, 0},
      {0, 1, 0, 0, WS_SHIP_HDEMO_SK, WEB_SALES, WS_SHIP_HDEMO_SK, 0},
      {0, 1, 0, 0, WS_SHIP_ADDR_SK, WEB_SALES, WS_SHIP_ADDR_SK, 0},
      {0, 16, 0, 0, WS_WEB_PAGE_SK, WEB_SALES, WS_WEB_PAGE_SK, 0},
      {0, 16, 0, 0, WS_WEB_SITE_SK, WEB_SALES, WS_WEB_SITE_SK, 0},
      {0, 16, 0, 0, WS_SHIP_MODE_SK, WEB_SALES, WS_SHIP_MODE_SK, 0},
      {0, 16, 0, 0, WS_WAREHOUSE_SK, WEB_SALES, WS_WAREHOUSE_SK, 0},
      {0, 16, 0, 0, WS_PROMO_SK, WEB_SALES, WS_PROMO_SK, 0},
      {0, 1, 0, 0, WS_ORDER_NUMBER, WEB_SALES, WS_ORDER_NUMBER, 0},
      {0, 1, 0, 0, WS_PRICING_QUANTITY, WEB_SALES, WS_PRICING_QUANTITY, 0},
      {0,
       1,
       0,
       0,
       WS_PRICING_WHOLESALE_COST,
       WEB_SALES,
       WS_PRICING_WHOLESALE_COST,
       0},
      {0, 0, 0, 0, WS_PRICING_LIST_PRICE, WEB_SALES, WS_PRICING_LIST_PRICE, 0},
      {0,
       0,
       0,
       0,
       WS_PRICING_SALES_PRICE,
       WEB_SALES,
       WS_PRICING_SALES_PRICE,
       0},
      {0,
       0,
       0,
       0,
       WS_PRICING_EXT_DISCOUNT_AMT,
       WEB_SALES,
       WS_PRICING_EXT_DISCOUNT_AMT,
       0},
      {0,
       0,
       0,
       0,
       WS_PRICING_EXT_SALES_PRICE,
       WEB_SALES,
       WS_PRICING_EXT_SALES_PRICE,
       0},
      {0,
       0,
       0,
       0,
       WS_PRICING_EXT_WHOLESALE_COST,
       WEB_SALES,
       WS_PRICING_EXT_WHOLESALE_COST,
       0},
      {0,
       0,
       0,
       0,
       WS_PRICING_EXT_LIST_PRICE,
       WEB_SALES,
       WS_PRICING_EXT_LIST_PRICE,
       0},
      {0, 0, 0, 0, WS_PRICING_EXT_TAX, WEB_SALES, WS_PRICING_EXT_TAX, 0},
      {0, 0, 0, 0, WS_PRICING_COUPON_AMT, WEB_SALES, WS_PRICING_COUPON_AMT, 0},
      {0,
       0,
       0,
       0,
       WS_PRICING_EXT_SHIP_COST,
       WEB_SALES,
       WS_PRICING_EXT_SHIP_COST,
       0},
      {0, 0, 0, 0, WS_PRICING_NET_PAID, WEB_SALES, WS_PRICING_NET_PAID, 0},
      {0,
       0,
       0,
       0,
       WS_PRICING_NET_PAID_INC_TAX,
       WEB_SALES,
       WS_PRICING_NET_PAID_INC_TAX,
       0},
      {0,
       0,
       0,
       0,
       WS_PRICING_NET_PAID_INC_SHIP,
       WEB_SALES,
       WS_PRICING_NET_PAID_INC_SHIP,
       0},
      {0,
       0,
       0,
       0,
       WS_PRICING_NET_PAID_INC_SHIP_TAX,
       WEB_SALES,
       WS_PRICING_NET_PAID_INC_SHIP_TAX,
       0},
      {0, 0, 0, 0, WS_PRICING_NET_PROFIT, WEB_SALES, WS_PRICING_NET_PROFIT, 0},
      {0, 128, 0, 0, WS_PRICING, WEB_SALES, WS_PRICING, 0},
      {0, 32, 0, 0, WS_NULLS, WEB_SALES, WS_NULLS, 0},
      {0, 16, 0, 0, WR_IS_RETURNED, WEB_SALES, WR_IS_RETURNED, 0},
      {0, 0, 0, 0, WS_PERMUTATION, WEB_SALES, WS_PERMUTATION, 0},
      {0, 1, 0, 0, WEB_SITE_SK, WEB_SITE, WEB_SITE_SK, 0},
      {0, 1, 0, 0, WEB_SITE_ID, WEB_SITE, WEB_SITE_ID, 0},
      {0, 1, 0, 0, WEB_REC_START_DATE_ID, WEB_SITE, WEB_REC_START_DATE_ID, 0},
      {0, 1, 0, 0, WEB_REC_END_DATE_ID, WEB_SITE, WEB_REC_END_DATE_ID, 0},
      {0, 1, 0, 0, WEB_NAME, WEB_SITE, WEB_NAME, 0},
      {0, 1, 0, 0, WEB_OPEN_DATE, WEB_SITE, WEB_OPEN_DATE, 0},
      {0, 1, 0, 0, WEB_CLOSE_DATE, WEB_SITE, WEB_CLOSE_DATE, 0},
      {0, 1, 0, 0, WEB_CLASS, WEB_SITE, WEB_CLASS, 0},
      {0, 2, 0, 0, WEB_MANAGER, WEB_SITE, WEB_MANAGER, 0},
      {0, 1, 0, 0, WEB_MARKET_ID, WEB_SITE, WEB_MARKET_ID, 0},
      {0, 20, 0, 0, WEB_MARKET_CLASS, WEB_SITE, WEB_MARKET_CLASS, 0},
      {0, 100, 0, 0, WEB_MARKET_DESC, WEB_SITE, WEB_MARKET_DESC, 0},
      {0, 2, 0, 0, WEB_MARKET_MANAGER, WEB_SITE, WEB_MARKET_MANAGER, 0},
      {0, 1, 0, 0, WEB_COMPANY_ID, WEB_SITE, WEB_COMPANY_ID, 0},
      {0, 1, 0, 0, WEB_COMPANY_NAME, WEB_SITE, WEB_COMPANY_NAME, 0},
      {0, 1, 0, 0, WEB_ADDRESS_STREET_NUM, WEB_SITE, WEB_ADDRESS_STREET_NUM, 0},
      {0,
       1,
       0,
       0,
       WEB_ADDRESS_STREET_NAME1,
       WEB_SITE,
       WEB_ADDRESS_STREET_NAME1,
       0},
      {0,
       1,
       0,
       0,
       WEB_ADDRESS_STREET_TYPE,
       WEB_SITE,
       WEB_ADDRESS_STREET_TYPE,
       0},
      {0, 1, 0, 0, WEB_ADDRESS_SUITE_NUM, WEB_SITE, WEB_ADDRESS_SUITE_NUM, 0},
      {0, 1, 0, 0, WEB_ADDRESS_CITY, WEB_SITE, WEB_ADDRESS_CITY, 0},
      {0, 1, 0, 0, WEB_ADDRESS_COUNTY, WEB_SITE, WEB_ADDRESS_COUNTY, 0},
      {0, 1, 0, 0, WEB_ADDRESS_STATE, WEB_SITE, WEB_ADDRESS_STATE, 0},
      {0, 1, 0, 0, WEB_ADDRESS_ZIP, WEB_SITE, WEB_ADDRESS_ZIP, 0},
      {0, 1, 0, 0, WEB_ADDRESS_COUNTRY, WEB_SITE, WEB_ADDRESS_COUNTRY, 0},
      {0, 1, 0, 0, WEB_ADDRESS_GMT_OFFSET, WEB_SITE, WEB_ADDRESS_GMT_OFFSET, 0},
      {0, 1, 0, 0, WEB_TAX_PERCENTAGE, WEB_SITE, WEB_TAX_PERCENTAGE, 0},
      {0, 2, 0, 0, WEB_NULLS, WEB_SITE, WEB_NULLS, 0},
      {0, 7, 0, 0, WEB_ADDRESS, WEB_SITE, WEB_ADDRESS, 0},
      {0, 70, 0, 0, WEB_SCD, WEB_SITE, WEB_SCD, 0},
      {0, 0, 0, 0, DV_VERSION, DBGEN_VERSION, DV_VERSION, 0},
      {0, 0, 0, 0, DV_CREATE_DATE, DBGEN_VERSION, DV_CREATE_DATE, 0},
      {0, 0, 0, 0, DV_CREATE_TIME, DBGEN_VERSION, DV_CREATE_TIME, 0},
      {0, 0, 0, 0, DV_CMDLINE_ARGS, DBGEN_VERSION, DV_CMDLINE_ARGS, 0},
      {0, 0, 0, 0, VALIDATE_STREAM, DBGEN_VERSION, VALIDATE_STREAM, 0},
      {0, 0, 0, 0, S_BRAND_ID, S_BRAND, S_BRAND_ID, 0},
      {0, 0, 0, 0, S_BRAND_SUBCLASS_ID, S_BRAND, S_BRAND_SUBCLASS_ID, 0},
      {0, 1, 0, 0, S_BRAND_MANAGER_ID, S_BRAND, S_BRAND_MANAGER_ID, 0},
      {0,
       1,
       0,
       0,
       S_BRAND_MANUFACTURER_ID,
       S_BRAND,
       S_BRAND_MANUFACTURER_ID,
       0},
      {0, 6, 0, 0, S_BRAND_NAME, S_BRAND, S_BRAND_NAME, 0},
      {0, 0, 0, 0, S_CADR_ID, S_CUSTOMER_ADDRESS, S_CADR_ID, 0},
      {0,
       1,
       0,
       0,
       S_CADR_ADDRESS_STREET_NUMBER,
       S_CUSTOMER_ADDRESS,
       S_CADR_ADDRESS_STREET_NUMBER,
       0},
      {0,
       1,
       0,
       0,
       S_CADR_ADDRESS_STREET_NAME1,
       S_CUSTOMER_ADDRESS,
       S_CADR_ADDRESS_STREET_NAME1,
       0},
      {0,
       1,
       0,
       0,
       S_CADR_ADDRESS_STREET_NAME2,
       S_CUSTOMER_ADDRESS,
       S_CADR_ADDRESS_STREET_NAME2,
       0},
      {0,
       1,
       0,
       0,
       S_CADR_ADDRESS_STREET_TYPE,
       S_CUSTOMER_ADDRESS,
       S_CADR_ADDRESS_STREET_TYPE,
       0},
      {0,
       1,
       0,
       0,
       S_CADR_ADDRESS_SUITE_NUM,
       S_CUSTOMER_ADDRESS,
       S_CADR_ADDRESS_SUITE_NUM,
       0},
      {0,
       1,
       0,
       0,
       S_CADR_ADDRESS_CITY,
       S_CUSTOMER_ADDRESS,
       S_CADR_ADDRESS_CITY,
       0},
      {0,
       1,
       0,
       0,
       S_CADR_ADDRESS_COUNTY,
       S_CUSTOMER_ADDRESS,
       S_CADR_ADDRESS_COUNTY,
       0},
      {0,
       1,
       0,
       0,
       S_CADR_ADDRESS_STATE,
       S_CUSTOMER_ADDRESS,
       S_CADR_ADDRESS_STATE,
       0},
      {0,
       1,
       0,
       0,
       S_CADR_ADDRESS_ZIP,
       S_CUSTOMER_ADDRESS,
       S_CADR_ADDRESS_ZIP,
       0},
      {0,
       1,
       0,
       0,
       S_CADR_ADDRESS_COUNTRY,
       S_CUSTOMER_ADDRESS,
       S_CADR_ADDRESS_COUNTRY,
       0},
      {0, 7, 0, 0, S_BADDR_ADDRESS, S_CUSTOMER_ADDRESS, CA_ADDRESS, 0},
      {0, 1, 0, 0, S_CALL_CENTER_ID, S_CALL_CENTER, S_CALL_CENTER_ID, 0},
      {0,
       1,
       0,
       0,
       S_CALL_CENTER_DIVISION_ID,
       S_CALL_CENTER,
       S_CALL_CENTER_DIVISION_ID,
       0},
      {0,
       1,
       0,
       0,
       S_CALL_CENTER_OPEN_DATE,
       S_CALL_CENTER,
       S_CALL_CENTER_OPEN_DATE,
       0},
      {0,
       0,
       0,
       0,
       S_CALL_CENTER_CLOSED_DATE,
       S_CALL_CENTER,
       S_CALL_CENTER_CLOSED_DATE,
       0},
      {0, 0, 0, 0, S_CALL_CENTER_NAME, S_CALL_CENTER, S_CALL_CENTER_NAME, 0},
      {0, 0, 0, 0, S_CALL_CENTER_CLASS, S_CALL_CENTER, S_CALL_CENTER_CLASS, 0},
      {0,
       1,
       0,
       0,
       S_CALL_CENTER_EMPLOYEES,
       S_CALL_CENTER,
       S_CALL_CENTER_EMPLOYEES,
       0},
      {0, 1, 0, 0, S_CALL_CENTER_SQFT, S_CALL_CENTER, S_CALL_CENTER_SQFT, 0},
      {0, 1, 0, 0, S_CALL_CENTER_HOURS, S_CALL_CENTER, S_CALL_CENTER_HOURS, 0},
      {0,
       1,
       0,
       0,
       S_CALL_CENTER_MANAGER_ID,
       S_CALL_CENTER,
       S_CALL_CENTER_MANAGER_ID,
       0},
      {0,
       1,
       0,
       0,
       S_CALL_CENTER_MARKET_ID,
       S_CALL_CENTER,
       S_CALL_CENTER_MARKET_ID,
       0},
      {0,
       1,
       0,
       0,
       S_CALL_CENTER_ADDRESS_ID,
       S_CALL_CENTER,
       S_CALL_CENTER_ADDRESS_ID,
       0},
      {0,
       1,
       0,
       0,
       S_CALL_CENTER_TAX_PERCENTAGE,
       S_CALL_CENTER,
       S_CALL_CENTER_TAX_PERCENTAGE,
       0},
      {0, 1, 0, 0, S_CALL_CENTER_SCD, S_CALL_CENTER, S_CALL_CENTER_SCD, 0},
      {0, 0, 0, 0, S_CATALOG_NUMBER, S_CATALOG, S_CATALOG_NUMBER, 0},
      {0, 1, 0, 0, S_CATALOG_START_DATE, S_CATALOG, S_CATALOG_START_DATE, 0},
      {0, 1, 0, 0, S_CATALOG_END_DATE, S_CATALOG, S_CATALOG_END_DATE, 0},
      {0, 10, 0, 0, S_CATALOG_DESC, S_CATALOG, S_CATALOG_DESC, 0},
      {0, 1, 0, 0, S_CATALOG_TYPE, S_CATALOG, S_CATALOG_TYPE, 0},
      {0, 0, 0, 0, S_CORD_ID, S_CATALOG_ORDER, S_CORD_ID, 0},
      {0,
       1,
       0,
       0,
       S_CORD_BILL_CUSTOMER_ID,
       S_CATALOG_ORDER,
       S_CORD_BILL_CUSTOMER_ID,
       0},
      {0,
       2,
       0,
       0,
       S_CORD_SHIP_CUSTOMER_ID,
       S_CATALOG_ORDER,
       S_CORD_SHIP_CUSTOMER_ID,
       0},
      {0, 1, 0, 0, S_CORD_ORDER_DATE, S_CATALOG_ORDER, S_CORD_ORDER_DATE, 0},
      {0, 1, 0, 0, S_CORD_ORDER_TIME, S_CATALOG_ORDER, S_CORD_ORDER_TIME, 0},
      {0,
       1,
       0,
       0,
       S_CORD_SHIP_MODE_ID,
       S_CATALOG_ORDER,
       S_CORD_SHIP_MODE_ID,
       0},
      {0,
       1,
       0,
       0,
       S_CORD_CALL_CENTER_ID,
       S_CATALOG_ORDER,
       S_CORD_CALL_CENTER_ID,
       0},
      {0, 1, 0, 0, S_CLIN_ITEM_ID, S_CATALOG_ORDER, S_CLIN_ITEM_ID, 0},
      {0, 100, 0, 0, S_CORD_COMMENT, S_CATALOG_ORDER, S_CORD_COMMENT, 0},
      {0,
       1,
       0,
       0,
       S_CLIN_ORDER_ID,
       S_CATALOG_ORDER_LINEITEM,
       S_CLIN_ORDER_ID,
       0},
      {0,
       0,
       0,
       0,
       S_CLIN_LINE_NUMBER,
       S_CATALOG_ORDER_LINEITEM,
       S_CLIN_LINE_NUMBER,
       0},
      {0,
       1,
       0,
       0,
       S_CLIN_PROMOTION_ID,
       S_CATALOG_ORDER_LINEITEM,
       S_CLIN_PROMOTION_ID,
       0},
      {0,
       1,
       0,
       0,
       S_CLIN_QUANTITY,
       S_CATALOG_ORDER_LINEITEM,
       S_CLIN_QUANTITY,
       0},
      {0,
       1,
       0,
       0,
       S_CLIN_COUPON_AMT,
       S_CATALOG_ORDER_LINEITEM,
       S_CLIN_COUPON_AMT,
       0},
      {0,
       1,
       0,
       0,
       S_CLIN_WAREHOUSE_ID,
       S_CATALOG_ORDER_LINEITEM,
       S_CLIN_WAREHOUSE_ID,
       0},
      {0,
       1,
       0,
       0,
       S_CLIN_SHIP_DATE,
       S_CATALOG_ORDER_LINEITEM,
       S_CLIN_SHIP_DATE,
       0},
      {0,
       1,
       0,
       0,
       S_CLIN_CATALOG_ID,
       S_CATALOG_ORDER_LINEITEM,
       S_CLIN_CATALOG_ID,
       0},
      {0,
       2,
       0,
       0,
       S_CLIN_CATALOG_PAGE_ID,
       S_CATALOG_ORDER_LINEITEM,
       S_CLIN_CATALOG_PAGE_ID,
       0},
      {0, 8, 0, 0, S_CLIN_PRICING, S_CATALOG_ORDER_LINEITEM, S_CLIN_PRICING, 0},
      {0,
       0,
       0,
       0,
       S_CLIN_SHIP_COST,
       S_CATALOG_ORDER_LINEITEM,
       S_CLIN_SHIP_COST,
       0},
      {0,
       1,
       0,
       0,
       S_CLIN_IS_RETURNED,
       S_CATALOG_ORDER_LINEITEM,
       S_CLIN_IS_RETURNED,
       0},
      {0, 0, 0, 0, S_CLIN_PERMUTE, S_CATALOG_ORDER_LINEITEM, S_CLIN_PERMUTE, 0},
      {0,
       0,
       0,
       0,
       S_CATALOG_PAGE_CATALOG_NUMBER,
       S_CATALOG_PAGE,
       S_CATALOG_PAGE_CATALOG_NUMBER,
       0},
      {0,
       0,
       0,
       0,
       S_CATALOG_PAGE_NUMBER,
       S_CATALOG_PAGE,
       S_CATALOG_PAGE_NUMBER,
       0},
      {0,
       1,
       0,
       0,
       S_CATALOG_PAGE_DEPARTMENT,
       S_CATALOG_PAGE,
       S_CATALOG_PAGE_DEPARTMENT,
       0},
      {0, 1, 0, 0, S_CP_ID, S_CATALOG_PAGE, S_CP_ID, 0},
      {0, 1, 0, 0, S_CP_START_DATE, S_CATALOG_PAGE, S_CP_START_DATE, 0},
      {0, 1, 0, 0, S_CP_END_DATE, S_CATALOG_PAGE, S_CP_END_DATE, 0},
      {0, 100, 0, 0, S_CP_DESCRIPTION, S_CATALOG_PAGE, CP_DESCRIPTION, 0},
      {0, 1, 0, 0, S_CP_TYPE, S_CATALOG_PAGE, S_CP_TYPE, 0},
      {0,
       1,
       0,
       0,
       S_CATALOG_PROMOTIONAL_ITEM_CATALOG_NUMBER,
       S_CATALOG_PROMOTIONAL_ITEM,
       S_CATALOG_PROMOTIONAL_ITEM_CATALOG_NUMBER,
       0},
      {0,
       1,
       0,
       0,
       S_CATALOG_PROMOTIONAL_ITEM_CATALOG_PAGE_NUMBER,
       S_CATALOG_PROMOTIONAL_ITEM,
       S_CATALOG_PROMOTIONAL_ITEM_CATALOG_PAGE_NUMBER,
       0},
      {0,
       1,
       0,
       0,
       S_CATALOG_PROMOTIONAL_ITEM_ITEM_ID,
       S_CATALOG_PROMOTIONAL_ITEM,
       S_CATALOG_PROMOTIONAL_ITEM_ITEM_ID,
       0},
      {0,
       0,
       0,
       0,
       S_CATALOG_PROMOTIONAL_ITEM_PROMOTION_ID,
       S_CATALOG_PROMOTIONAL_ITEM,
       S_CATALOG_PROMOTIONAL_ITEM_PROMOTION_ID,
       0},
      {0,
       9,
       0,
       0,
       S_CRET_CALL_CENTER_ID,
       S_CATALOG_RETURNS,
       S_CRET_CALL_CENTER_ID,
       0},
      {0, 0, 0, 0, S_CRET_ORDER_ID, S_CATALOG_RETURNS, S_CRET_ORDER_ID, 0},
      {0,
       0,
       0,
       0,
       S_CRET_LINE_NUMBER,
       S_CATALOG_RETURNS,
       S_CRET_LINE_NUMBER,
       0},
      {0, 0, 0, 0, S_CRET_ITEM_ID, S_CATALOG_RETURNS, S_CRET_ITEM_ID, 0},
      {0,
       0,
       0,
       0,
       S_CRET_RETURN_CUSTOMER_ID,
       S_CATALOG_RETURNS,
       S_CRET_RETURN_CUSTOMER_ID,
       0},
      {0,
       0,
       0,
       0,
       S_CRET_REFUND_CUSTOMER_ID,
       S_CATALOG_RETURNS,
       S_CRET_REFUND_CUSTOMER_ID,
       0},
      {0, 9, 0, 0, S_CRET_DATE, S_CATALOG_RETURNS, S_CRET_DATE, 0},
      {0, 18, 0, 0, S_CRET_TIME, S_CATALOG_RETURNS, S_CRET_TIME, 0},
      {0, 0, 0, 0, S_CRET_QUANTITY, S_CATALOG_RETURNS, S_CRET_QUANTITY, 0},
      {0, 0, 0, 0, S_CRET_AMOUNT, S_CATALOG_RETURNS, S_CRET_AMOUNT, 0},
      {0, 0, 0, 0, S_CRET_TAX, S_CATALOG_RETURNS, S_CRET_TAX, 0},
      {0, 0, 0, 0, S_CRET_FEE, S_CATALOG_RETURNS, S_CRET_FEE, 0},
      {0, 0, 0, 0, S_CRET_SHIP_COST, S_CATALOG_RETURNS, S_CRET_SHIP_COST, 0},
      {0,
       0,
       0,
       0,
       S_CRET_REFUNDED_CASH,
       S_CATALOG_RETURNS,
       S_CRET_REFUNDED_CASH,
       0},
      {0,
       0,
       0,
       0,
       S_CRET_REVERSED_CHARGE,
       S_CATALOG_RETURNS,
       S_CRET_REVERSED_CHARGE,
       0},
      {0,
       0,
       0,
       0,
       S_CRET_MERCHANT_CREDIT,
       S_CATALOG_RETURNS,
       S_CRET_MERCHANT_CREDIT,
       0},
      {0, 9, 0, 0, S_CRET_REASON_ID, S_CATALOG_RETURNS, S_CRET_REASON_ID, 0},
      {0, 72, 0, 0, S_CRET_PRICING, S_CATALOG_RETURNS, S_CRET_PRICING, 0},
      {0,
       9,
       0,
       0,
       S_CRET_SHIPMODE_ID,
       S_CATALOG_RETURNS,
       S_CRET_SHIPMODE_ID,
       0},
      {0,
       9,
       0,
       0,
       S_CRET_WAREHOUSE_ID,
       S_CATALOG_RETURNS,
       S_CRET_WAREHOUSE_ID,
       0},
      {0,
       0,
       0,
       0,
       S_CRET_CATALOG_PAGE_ID,
       S_CATALOG_RETURNS,
       S_CRET_CATALOG_PAGE_ID,
       0},
      {0, 0, 0, 0, S_CATEGORY_ID, S_CATEGORY, S_CATEGORY_ID, 0},
      {0, 0, 0, 0, S_CATEGORY_NAME, S_CATEGORY, S_CATEGORY_NAME, 0},
      {0, 10, 0, 0, S_CATEGORY_DESC, S_CATEGORY, S_CATEGORY_DESC, 0},
      {0, 0, 0, 0, S_CLASS_ID, S_CLASS, S_CLASS_ID, 0},
      {0, 1, 0, 0, S_CLASS_SUBCAT_ID, S_CLASS, S_CLASS_SUBCAT_ID, 0},
      {0, 10, 0, 0, S_CLASS_DESC, S_CLASS, S_CLASS_DESC, 0},
      {0, 0, 0, 0, S_COMPANY_ID, S_COMPANY, S_COMPANY_ID, 0},
      {0, 0, 0, 0, S_COMPANY_NAME, S_COMPANY, S_COMPANY_NAME, 0},
      {0, 0, 0, 0, S_CUST_ID, S_CUSTOMER, S_CUST_ID, 0},
      {0, 1, 0, 0, S_CUST_SALUTATION, S_CUSTOMER, S_CUST_SALUTATION, 0},
      {0, 1, 0, 0, S_CUST_LAST_NAME, S_CUSTOMER, S_CUST_LAST_NAME, 0},
      {0, 1, 0, 0, S_CUST_FIRST_NAME, S_CUSTOMER, S_CUST_FIRST_NAME, 0},
      {0, 1, 0, 0, S_CUST_PREFERRED_FLAG, S_CUSTOMER, S_CUST_PREFERRED_FLAG, 0},
      {0, 1, 0, 0, S_CUST_BIRTH_DATE, S_CUSTOMER, S_CUST_BIRTH_DATE, 0},
      {0,
       1,
       0,
       0,
       S_CUST_FIRST_PURCHASE_DATE,
       S_CUSTOMER,
       S_CUST_FIRST_PURCHASE_DATE,
       0},
      {0,
       1,
       0,
       0,
       S_CUST_FIRST_SHIPTO_DATE,
       S_CUSTOMER,
       S_CUST_FIRST_SHIPTO_DATE,
       0},
      {0, 1, 0, 0, S_CUST_BIRTH_COUNTRY, S_CUSTOMER, S_CUST_BIRTH_COUNTRY, 0},
      {0, 25, 0, 0, S_CUST_LOGIN, S_CUSTOMER, S_CUST_LOGIN, 0},
      {0, 23, 0, 0, S_CUST_EMAIL, S_CUSTOMER, S_CUST_EMAIL, 0},
      {0, 1, 0, 0, S_CUST_LAST_LOGIN, S_CUSTOMER, S_CUST_LAST_LOGIN, 0},
      {0, 1, 0, 0, S_CUST_LAST_REVIEW, S_CUSTOMER, S_CUST_LAST_REVIEW, 0},
      {0,
       4,
       0,
       0,
       S_CUST_PRIMARY_MACHINE,
       S_CUSTOMER,
       S_CUST_PRIMARY_MACHINE,
       0},
      {0,
       4,
       0,
       0,
       S_CUST_SECONDARY_MACHINE,
       S_CUSTOMER,
       S_CUST_SECONDARY_MACHINE,
       0},
      {0, 7, 0, 0, S_CUST_ADDRESS, S_CUSTOMER, S_CUST_ADDRESS, 0},
      {0,
       1,
       0,
       0,
       S_CUST_ADDRESS_STREET_NUM,
       S_CUSTOMER,
       S_CUST_ADDRESS_STREET_NUM,
       0},
      {0,
       1,
       0,
       0,
       S_CUST_ADDRESS_STREET_NAME1,
       S_CUSTOMER,
       S_CUST_ADDRESS_STREET_NAME1,
       0},
      {0,
       1,
       0,
       0,
       S_CUST_ADDRESS_STREET_NAME2,
       S_CUSTOMER,
       S_CUST_ADDRESS_STREET_NAME2,
       0},
      {0,
       1,
       0,
       0,
       S_CUST_ADDRESS_STREET_TYPE,
       S_CUSTOMER,
       S_CUST_ADDRESS_STREET_TYPE,
       0},
      {0,
       1,
       0,
       0,
       S_CUST_ADDRESS_SUITE_NUM,
       S_CUSTOMER,
       S_CUST_ADDRESS_SUITE_NUM,
       0},
      {0, 1, 0, 0, S_CUST_ADDRESS_CITY, S_CUSTOMER, S_CUST_ADDRESS_CITY, 0},
      {0, 1, 0, 0, S_CUST_ADDRESS_ZIP, S_CUSTOMER, S_CUST_ADDRESS_ZIP, 0},
      {0, 1, 0, 0, S_CUST_ADDRESS_COUNTY, S_CUSTOMER, S_CUST_ADDRESS_COUNTY, 0},
      {0, 1, 0, 0, S_CUST_ADDRESS_STATE, S_CUSTOMER, S_CUST_ADDRESS_STATE, 0},
      {0,
       1,
       0,
       0,
       S_CUST_ADDRESS_COUNTRY,
       S_CUSTOMER,
       S_CUST_ADDRESS_COUNTRY,
       0},
      {0, 1, 0, 0, S_CUST_LOCATION_TYPE, S_CUSTOMER, S_CUST_LOCATION_TYPE, 0},
      {0, 1, 0, 0, S_CUST_GENDER, S_CUSTOMER, S_CUST_GENDER, 0},
      {0, 1, 0, 0, S_CUST_MARITAL_STATUS, S_CUSTOMER, S_CUST_MARITAL_STATUS, 0},
      {0, 1, 0, 0, S_CUST_EDUCATION, S_CUSTOMER, S_CUST_EDUCATION, 0},
      {0, 1, 0, 0, S_CUST_CREDIT_RATING, S_CUSTOMER, S_CUST_CREDIT_RATING, 0},
      {0,
       1,
       0,
       0,
       S_CUST_PURCHASE_ESTIMATE,
       S_CUSTOMER,
       S_CUST_PURCHASE_ESTIMATE,
       0},
      {0, 1, 0, 0, S_CUST_BUY_POTENTIAL, S_CUSTOMER, S_CUST_BUY_POTENTIAL, 0},
      {0, 1, 0, 0, S_CUST_DEPENDENT_CNT, S_CUSTOMER, S_CUST_DEPENDENT_CNT, 0},
      {0, 1, 0, 0, S_CUST_EMPLOYED_CNT, S_CUSTOMER, S_CUST_EMPLOYED_CNT, 0},
      {0, 1, 0, 0, S_CUST_COLLEGE_CNT, S_CUSTOMER, S_CUST_COLLEGE_CNT, 0},
      {0, 1, 0, 0, S_CUST_VEHICLE_CNT, S_CUSTOMER, S_CUST_VEHICLE_CNT, 0},
      {0, 1, 0, 0, S_CUST_INCOME, S_CUSTOMER, S_CUST_INCOME, 0},
      {0, 0, 0, 0, S_DIVISION_ID, S_DIVISION, S_DIVISION_ID, 0},
      {0, 0, 0, 0, S_DIVISION_COMPANY, S_DIVISION, S_DIVISION_COMPANY, 0},
      {0, 0, 0, 0, S_DIVISION_NAME, S_DIVISION, S_DIVISION_NAME, 0},
      {0, 1, 0, 0, S_INVN_WAREHOUSE, S_INVENTORY, S_INVN_WAREHOUSE, 0},
      {0, 1, 0, 0, S_INVN_ITEM, S_INVENTORY, S_INVN_ITEM, 0},
      {0, 2, 0, 0, S_INVN_DATE, S_INVENTORY, S_INVN_DATE, 0},
      {0, 1, 0, 0, S_INVN_QUANTITY, S_INVENTORY, S_INVN_QUANTITY, 0},
      {0, 1, 0, 0, S_ITEM_ID, S_ITEM, S_ITEM_ID, 0},
      {0, 0, 0, 0, S_ITEM_PERMUTE, S_ITEM, S_ITEM_PERMUTE, 0},
      {0, 1, 0, 0, S_ITEM_PRODUCT_ID, S_ITEM, S_ITEM_PRODUCT_ID, 0},
      {0, 200, 0, 0, S_ITEM_DESC, S_ITEM, I_ITEM_DESC, 0},
      {0, 2, 0, 0, S_ITEM_LIST_PRICE, S_ITEM, I_CURRENT_PRICE, 0},
      {0, 1, 0, 0, S_ITEM_WHOLESALE_COST, S_ITEM, I_WHOLESALE_COST, 0},
      {0, 1, 0, 0, S_ITEM_MANAGER_ID, S_ITEM, I_MANAGER_ID, 0},
      {0, 1, 0, 0, S_ITEM_SIZE, S_ITEM, I_SIZE, 0},
      {0, 50, 0, 0, S_ITEM_FORMULATION, S_ITEM, I_FORMULATION, 0},
      {0, 1, 0, 0, S_ITEM_FLAVOR, S_ITEM, I_COLOR, 0},
      {0, 1, 0, 0, S_ITEM_UNITS, S_ITEM, I_UNITS, 0},
      {0, 1, 0, 0, S_ITEM_CONTAINER, S_ITEM, I_CONTAINER, 0},
      {0, 1, 0, 0, S_ITEM_SCD, S_ITEM, I_SCD, 0},
      {0, 0, 0, 0, S_MANAGER_ID, S_MANAGER, S_MANAGER_ID, 0},
      {0, 2, 0, 0, S_MANAGER_NAME, S_MANAGER, S_MANAGER_NAME, 0},
      {0, 0, 0, 0, S_MANUFACTURER_ID, S_MANUFACTURER, S_MANUFACTURER_ID, 0},
      {0, 0, 0, 0, S_MANUFACTURER_NAME, S_MANUFACTURER, S_MANUFACTURER_NAME, 0},
      {0, 0, 0, 0, S_MARKET_ID, S_MARKET, S_MARKET_ID, 0},
      {0, 0, 0, 0, S_MARKET_CLASS_NAME, S_MARKET, S_MARKET_CLASS_NAME, 0},
      {0, 10, 0, 0, S_MARKET_DESC, S_MARKET, S_MARKET_DESC, 0},
      {0, 1, 0, 0, S_MARKET_MANAGER_ID, S_MARKET, S_MARKET_MANAGER_ID, 0},
      {0, 0, 0, 0, S_PRODUCT_ID, S_PRODUCT, S_PRODUCT_ID, 0},
      {0, 1, 0, 0, S_PRODUCT_BRAND_ID, S_PRODUCT, S_PRODUCT_BRAND_ID, 0},
      {0, 0, 0, 0, S_PRODUCT_NAME, S_PRODUCT, S_PRODUCT_NAME, 0},
      {0, 1, 0, 0, S_PRODUCT_TYPE, S_PRODUCT, S_PRODUCT_TYPE, 0},
      {0, 1, 0, 0, S_PROMOTION_ID, S_PROMOTION, S_PROMOTION_ID, 0},
      {0, 1, 0, 0, S_PROMOTION_ITEM_ID, S_PROMOTION, S_PROMOTION_ITEM_ID, 0},
      {0,
       1,
       0,
       0,
       S_PROMOTION_START_DATE,
       S_PROMOTION,
       S_PROMOTION_START_DATE,
       0},
      {0, 1, 0, 0, S_PROMOTION_END_DATE, S_PROMOTION, S_PROMOTION_END_DATE, 0},
      {0, 1, 0, 0, S_PROMOTION_COST, S_PROMOTION, S_PROMOTION_COST, 0},
      {0,
       1,
       0,
       0,
       S_PROMOTION_RESPONSE_TARGET,
       S_PROMOTION,
       S_PROMOTION_RESPONSE_TARGET,
       0},
      {0, 0, 0, 0, S_PROMOTION_DMAIL, S_PROMOTION, S_PROMOTION_DMAIL, 0},
      {0, 0, 0, 0, S_PROMOTION_EMAIL, S_PROMOTION, S_PROMOTION_EMAIL, 0},
      {0, 0, 0, 0, S_PROMOTION_CATALOG, S_PROMOTION, S_PROMOTION_CATALOG, 0},
      {0, 0, 0, 0, S_PROMOTION_TV, S_PROMOTION, S_PROMOTION_TV, 0},
      {0, 0, 0, 0, S_PROMOTION_RADIO, S_PROMOTION, S_PROMOTION_RADIO, 0},
      {0, 0, 0, 0, S_PROMOTION_PRESS, S_PROMOTION, S_PROMOTION_PRESS, 0},
      {0, 0, 0, 0, S_PROMOTION_EVENT, S_PROMOTION, S_PROMOTION_EVENT, 0},
      {0, 0, 0, 0, S_PROMOTION_DEMO, S_PROMOTION, S_PROMOTION_DEMO, 0},
      {0, 100, 0, 0, S_PROMOTION_DETAILS, S_PROMOTION, P_CHANNEL_DETAILS, 0},
      {0, 1, 0, 0, S_PROMOTION_PURPOSE, S_PROMOTION, S_PROMOTION_PURPOSE, 0},
      {0,
       1,
       0,
       0,
       S_PROMOTION_DISCOUNT_ACTIVE,
       S_PROMOTION,
       S_PROMOTION_DISCOUNT_ACTIVE,
       0},
      {0,
       1,
       0,
       0,
       S_PROMOTION_DISCOUNT_PCT,
       S_PROMOTION,
       S_PROMOTION_DISCOUNT_PCT,
       0},
      {0, 0, 0, 0, S_PROMOTION_NAME, S_PROMOTION, S_PROMOTION_NAME, 0},
      {0, 1, 0, 0, S_PROMOTION_BITFIELD, S_PROMOTION, S_PROMOTION_BITFIELD, 0},
      {0, 0, 0, 0, S_PURCHASE_ID, S_PURCHASE, S_PURCHASE_ID, 0},
      {0, 1, 0, 0, S_PURCHASE_STORE_ID, S_PURCHASE, S_PURCHASE_STORE_ID, 0},
      {0,
       1,
       0,
       0,
       S_PURCHASE_CUSTOMER_ID,
       S_PURCHASE,
       S_PURCHASE_CUSTOMER_ID,
       0},
      {0, 1, 0, 0, S_PURCHASE_DATE, S_PURCHASE, S_PURCHASE_DATE, 0},
      {0, 1, 0, 0, S_PURCHASE_TIME, S_PURCHASE, S_PURCHASE_TIME, 0},
      {0, 1, 0, 0, S_PURCHASE_REGISTER, S_PURCHASE, S_PURCHASE_REGISTER, 0},
      {0, 1, 0, 0, S_PURCHASE_CLERK, S_PURCHASE, S_PURCHASE_CLERK, 0},
      {0, 100, 0, 0, S_PURCHASE_COMMENT, S_PURCHASE, S_PURCHASE_COMMENT, 0},
      {0, 7, 0, 0, S_PURCHASE_PRICING, S_PURCHASE, S_PURCHASE_PRICING, 0},
      {0, 1, 0, 0, S_PLINE_ITEM_ID, S_PURCHASE, S_PLINE_ITEM_ID, 0},
      {0,
       12,
       0,
       0,
       S_PLINE_PURCHASE_ID,
       S_PURCHASE_LINEITEM,
       S_PLINE_PURCHASE_ID,
       0},
      {0, 12, 0, 0, S_PLINE_NUMBER, S_PURCHASE_LINEITEM, S_PLINE_NUMBER, 0},
      {0,
       12,
       0,
       0,
       S_PLINE_PROMOTION_ID,
       S_PURCHASE_LINEITEM,
       S_PLINE_PROMOTION_ID,
       0},
      {0,
       12,
       0,
       0,
       S_PLINE_SALE_PRICE,
       S_PURCHASE_LINEITEM,
       S_PLINE_SALE_PRICE,
       0},
      {0, 12, 0, 0, S_PLINE_QUANTITY, S_PURCHASE_LINEITEM, S_PLINE_QUANTITY, 0},
      {0,
       12,
       0,
       0,
       S_PLINE_COUPON_AMT,
       S_PURCHASE_LINEITEM,
       S_PLINE_COUPON_AMT,
       0},
      {0, 1200, 0, 0, S_PLINE_COMMENT, S_PURCHASE_LINEITEM, S_PLINE_COMMENT, 0},
      {0, 96, 0, 0, S_PLINE_PRICING, S_PURCHASE_LINEITEM, S_PLINE_PRICING, 0},
      {0,
       12,
       0,
       0,
       S_PLINE_IS_RETURNED,
       S_PURCHASE_LINEITEM,
       S_PLINE_IS_RETURNED,
       0},
      {0, 0, 0, 0, S_PLINE_PERMUTE, S_PURCHASE_LINEITEM, S_PLINE_PERMUTE, 0},
      {0, 0, 0, 0, S_REASON_ID, S_REASON, S_REASON_ID, 0},
      {0, 10, 0, 0, S_REASON_DESC, S_REASON, S_REASON_DESC, 0},
      {0, 1, 0, 0, S_STORE_ID, S_STORE, S_STORE_ID, 0},
      {0, 1, 0, 0, S_STORE_ADDRESS_ID, S_STORE, S_STORE_ADDRESS_ID, 0},
      {0, 1, 0, 0, S_STORE_DIVISION_ID, S_STORE, S_STORE_DIVISION_ID, 0},
      {0, 1, 0, 0, S_STORE_OPEN_DATE, S_STORE, S_STORE_OPEN_DATE, 0},
      {0, 1, 0, 0, S_STORE_CLOSE_DATE, S_STORE, S_STORE_CLOSE_DATE, 0},
      {0, 0, 0, 0, S_STORE_NAME, S_STORE, S_STORE_NAME, 0},
      {0, 0, 0, 0, S_STORE_CLASS, S_STORE, S_STORE_CLASS, 0},
      {0, 1, 0, 0, S_STORE_EMPLOYEES, S_STORE, S_STORE_EMPLOYEES, 0},
      {0, 1, 0, 0, S_STORE_FLOOR_SPACE, S_STORE, S_STORE_FLOOR_SPACE, 0},
      {0, 1, 0, 0, S_STORE_HOURS, S_STORE, S_STORE_HOURS, 0},
      {0,
       0,
       0,
       0,
       S_STORE_MARKET_MANAGER_ID,
       S_STORE,
       S_STORE_MARKET_MANAGER_ID,
       0},
      {0, 1, 0, 0, S_STORE_MANAGER_ID, S_STORE, S_STORE_MANAGER_ID, 0},
      {0, 1, 0, 0, S_STORE_MARKET_ID, S_STORE, S_STORE_MARKET_ID, 0},
      {0,
       1,
       0,
       0,
       S_STORE_GEOGRAPHY_CLASS,
       S_STORE,
       S_STORE_GEOGRAPHY_CLASS,
       0},
      {0, 1, 0, 0, S_STORE_TAX_PERCENTAGE, S_STORE, S_STORE_TAX_PERCENTAGE, 0},
      {0,
       1,
       0,
       0,
       S_SITM_PROMOTION_ID,
       S_STORE_PROMOTIONAL_ITEM,
       S_SITM_PROMOTION_ID,
       0},
      {0, 1, 0, 0, S_SITM_ITEM_ID, S_STORE_PROMOTIONAL_ITEM, S_SITM_ITEM_ID, 0},
      {0,
       1,
       0,
       0,
       S_SITM_STORE_ID,
       S_STORE_PROMOTIONAL_ITEM,
       S_SITM_STORE_ID,
       0},
      {0, 0, 0, 0, S_SRET_STORE_ID, S_STORE_RETURNS, S_SRET_STORE_ID, 0},
      {0, 0, 0, 0, S_SRET_PURCHASE_ID, S_STORE_RETURNS, S_SRET_PURCHASE_ID, 0},
      {0, 0, 0, 0, S_SRET_LINENUMBER, S_STORE_RETURNS, S_SRET_LINENUMBER, 0},
      {0, 0, 0, 0, S_SRET_ITEM_ID, S_STORE_RETURNS, S_SRET_ITEM_ID, 0},
      {0, 0, 0, 0, S_SRET_CUSTOMER_ID, S_STORE_RETURNS, S_SRET_CUSTOMER_ID, 0},
      {0, 24, 0, 0, S_SRET_RETURN_DATE, S_STORE_RETURNS, S_SRET_RETURN_DATE, 0},
      {0, 12, 0, 0, S_SRET_RETURN_TIME, S_STORE_RETURNS, S_SRET_RETURN_TIME, 0},
      {0,
       0,
       0,
       0,
       S_SRET_TICKET_NUMBER,
       S_STORE_RETURNS,
       S_SRET_TICKET_NUMBER,
       0},
      {0,
       0,
       0,
       0,
       S_SRET_RETURN_QUANTITY,
       S_STORE_RETURNS,
       S_SRET_RETURN_QUANTITY,
       0},
      {0, 0, 0, 0, S_SRET_RETURN_AMT, S_STORE_RETURNS, S_SRET_RETURN_AMT, 0},
      {0, 0, 0, 0, S_SRET_RETURN_TAX, S_STORE_RETURNS, S_SRET_RETURN_TAX, 0},
      {0, 0, 0, 0, S_SRET_RETURN_FEE, S_STORE_RETURNS, S_SRET_RETURN_FEE, 0},
      {0,
       0,
       0,
       0,
       S_SRET_RETURN_SHIP_COST,
       S_STORE_RETURNS,
       S_SRET_RETURN_SHIP_COST,
       0},
      {0,
       0,
       0,
       0,
       S_SRET_REFUNDED_CASH,
       S_STORE_RETURNS,
       S_SRET_REFUNDED_CASH,
       0},
      {0,
       0,
       0,
       0,
       S_SRET_REVERSED_CHARGE,
       S_STORE_RETURNS,
       S_SRET_REVERSED_CHARGE,
       0},
      {0,
       0,
       0,
       0,
       S_SRET_MERCHANT_CREDIT,
       S_STORE_RETURNS,
       S_SRET_MERCHANT_CREDIT,
       0},
      {0, 12, 0, 0, S_SRET_REASON_ID, S_STORE_RETURNS, S_SRET_REASON_ID, 0},
      {0, 84, 0, 0, S_SRET_PRICING, S_STORE_RETURNS, S_SRET_PRICING, 0},
      {0, 0, 0, 0, S_SBCT_ID, S_SUBCATEGORY, S_SBCT_ID, 0},
      {0, 1, 0, 0, S_SBCT_CATEGORY_ID, S_SUBCATEGORY, S_SBCT_CATEGORY_ID, 0},
      {0, 0, 0, 0, S_SBCT_NAME, S_SUBCATEGORY, S_SBCT_NAME, 0},
      {0, 10, 0, 0, S_SBCT_DESC, S_SUBCATEGORY, S_SBCT_DESC, 0},
      {0, 0, 0, 0, S_SUBC_ID, S_SUBCLASS, S_SUBC_ID, 0},
      {0, 1, 0, 0, S_SUBC_CLASS_ID, S_SUBCLASS, S_SUBC_CLASS_ID, 0},
      {0, 0, 0, 0, S_SUBC_NAME, S_SUBCLASS, S_SUBC_NAME, 0},
      {0, 10, 0, 0, S_SUBC_DESC, S_SUBCLASS, S_SUBC_DESC, 0},
      {0, 1, 0, 0, S_WRHS_ID, S_WAREHOUSE, S_WRHS_ID, 0},
      {0, 10, 0, 0, S_WRHS_DESC, S_WAREHOUSE, S_WRHS_DESC, 0},
      {0, 1, 0, 0, S_WRHS_SQFT, S_WAREHOUSE, S_WRHS_SQFT, 0},
      {0, 1, 0, 0, S_WRHS_ADDRESS_ID, S_WAREHOUSE, S_WRHS_ADDRESS_ID, 0},
      {0, 1, 0, 0, S_WORD_ID, S_WEB_ORDER, S_WORD_ID, 0},
      {0,
       1,
       0,
       0,
       S_WORD_BILL_CUSTOMER_ID,
       S_WEB_ORDER,
       S_WORD_BILL_CUSTOMER_ID,
       0},
      {0,
       2,
       0,
       0,
       S_WORD_SHIP_CUSTOMER_ID,
       S_WEB_ORDER,
       S_WORD_SHIP_CUSTOMER_ID,
       0},
      {0, 1, 0, 0, S_WORD_ORDER_DATE, S_WEB_ORDER, S_WORD_ORDER_DATE, 0},
      {0, 1, 0, 0, S_WORD_ORDER_TIME, S_WEB_ORDER, S_WORD_ORDER_TIME, 0},
      {0, 1, 0, 0, S_WORD_SHIP_MODE_ID, S_WEB_ORDER, S_WORD_SHIP_MODE_ID, 0},
      {0, 1, 0, 0, S_WORD_WEB_SITE_ID, S_WEB_ORDER, S_WORD_WEB_SITE_ID, 0},
      {0, 100, 0, 0, S_WORD_COMMENT, S_WEB_ORDER, S_WORD_COMMENT, 0},
      {0, 1, 0, 0, S_WLIN_ITEM_ID, S_WEB_ORDER, S_WLIN_ITEM_ID, 0},
      {0, 12, 0, 0, S_WLIN_ID, S_WEB_ORDER_LINEITEM, S_WLIN_ID, 0},
      {0,
       0,
       0,
       0,
       S_WLIN_LINE_NUMBER,
       S_WEB_ORDER_LINEITEM,
       S_WLIN_LINE_NUMBER,
       0},
      {0,
       12,
       0,
       0,
       S_WLIN_PROMOTION_ID,
       S_WEB_ORDER_LINEITEM,
       S_WLIN_PROMOTION_ID,
       0},
      {0, 12, 0, 0, S_WLIN_QUANTITY, S_WEB_ORDER_LINEITEM, S_WLIN_QUANTITY, 0},
      {0,
       12,
       0,
       0,
       S_WLIN_COUPON_AMT,
       S_WEB_ORDER_LINEITEM,
       S_WLIN_COUPON_AMT,
       0},
      {0,
       12,
       0,
       0,
       S_WLIN_WAREHOUSE_ID,
       S_WEB_ORDER_LINEITEM,
       S_WLIN_WAREHOUSE_ID,
       0},
      {0,
       12,
       0,
       0,
       S_WLIN_SHIP_DATE,
       S_WEB_ORDER_LINEITEM,
       S_WLIN_SHIP_DATE,
       0},
      {0,
       12,
       0,
       0,
       S_WLIN_WEB_PAGE_ID,
       S_WEB_ORDER_LINEITEM,
       S_WLIN_WEB_PAGE_ID,
       0},
      {0, 96, 0, 0, S_WLIN_PRICING, S_WEB_ORDER_LINEITEM, S_WLIN_PRICING, 0},
      {0, 0, 0, 0, S_WLIN_SHIP_COST, S_WEB_ORDER_LINEITEM, S_WLIN_SHIP_COST, 0},
      {0,
       12,
       0,
       0,
       S_WLIN_IS_RETURNED,
       S_WEB_ORDER_LINEITEM,
       S_WLIN_IS_RETURNED,
       0},
      {0, 0, 0, 0, S_WLIN_PERMUTE, S_WEB_ORDER_LINEITEM, S_WLIN_PERMUTE, 0},
      {0, 1, 0, 0, S_WPAG_SITE_ID, S_WEB_PAGE, S_WPAG_SITE_ID, 0},
      {0, 1, 0, 0, S_WPAG_ID, S_WEB_PAGE, S_WPAG_ID, 0},
      {0, 1, 0, 0, S_WPAG_CREATE_DATE, S_WEB_PAGE, S_WPAG_CREATE_DATE, 0},
      {0, 1, 0, 0, S_WPAG_ACCESS_DATE, S_WEB_PAGE, S_WPAG_ACCESS_DATE, 0},
      {0, 1, 0, 0, S_WPAG_AUTOGEN_FLAG, S_WEB_PAGE, S_WPAG_AUTOGEN_FLAG, 0},
      {0, 1, 0, 0, S_WPAG_DEPARTMENT, S_WEB_PAGE, S_WPAG_DEPARTMENT, 0},
      {0, 1, 0, 0, S_WPAG_URL, S_WEB_PAGE, S_WPAG_URL, 0},
      {0, 1, 0, 0, S_WPAG_TYPE, S_WEB_PAGE, S_WPAG_TYPE, 0},
      {0, 1, 0, 0, S_WPAG_CHAR_CNT, S_WEB_PAGE, S_WPAG_CHAR_CNT, 0},
      {0, 1, 0, 0, S_WPAG_LINK_CNT, S_WEB_PAGE, S_WPAG_LINK_CNT, 0},
      {0, 1, 0, 0, S_WPAG_IMAGE_CNT, S_WEB_PAGE, S_WPAG_IMAGE_CNT, 0},
      {0, 1, 0, 0, S_WPAG_MAX_AD_CNT, S_WEB_PAGE, S_WPAG_MAX_AD_CNT, 0},
      {0, 0, 0, 0, S_WPAG_PERMUTE, S_WEB_PAGE, S_WPAG_PERMUTE, 0},
      {0, 1, 0, 0, S_WITM_SITE_ID, S_WEB_PROMOTIONAL_ITEM, S_WITM_SITE_ID, 0},
      {0, 1, 0, 0, S_WITM_PAGE_ID, S_WEB_PROMOTIONAL_ITEM, S_WITM_PAGE_ID, 0},
      {0, 1, 0, 0, S_WITM_ITEM_ID, S_WEB_PROMOTIONAL_ITEM, S_WITM_ITEM_ID, 0},
      {0,
       1,
       0,
       0,
       S_WITM_PROMOTION_ID,
       S_WEB_PROMOTIONAL_ITEM,
       S_WITM_PROMOTION_ID,
       0},
      {0, 0, 0, 0, S_WRET_SITE_ID, S_WEB_RETURNS, S_WRET_SITE_ID, 0},
      {0, 0, 0, 0, S_WRET_ORDER_ID, S_WEB_RETURNS, S_WRET_ORDER_ID, 0},
      {0, 0, 0, 0, S_WRET_LINE_NUMBER, S_WEB_RETURNS, S_WRET_LINE_NUMBER, 0},
      {0, 0, 0, 0, S_WRET_ITEM_ID, S_WEB_RETURNS, S_WRET_ITEM_ID, 0},
      {0,
       0,
       0,
       0,
       S_WRET_RETURN_CUST_ID,
       S_WEB_RETURNS,
       S_WRET_RETURN_CUST_ID,
       0},
      {0,
       0,
       0,
       0,
       S_WRET_REFUND_CUST_ID,
       S_WEB_RETURNS,
       S_WRET_REFUND_CUST_ID,
       0},
      {0, 24, 0, 0, S_WRET_RETURN_DATE, S_WEB_RETURNS, S_WRET_RETURN_DATE, 0},
      {0, 12, 0, 0, S_WRET_RETURN_TIME, S_WEB_RETURNS, S_WRET_RETURN_TIME, 0},
      {0, 12, 0, 0, S_WRET_REASON_ID, S_WEB_RETURNS, S_WRET_REASON_ID, 0},
      {0, 84, 0, 0, S_WRET_PRICING, S_WEB_RETURNS, S_WRET_PRICING, 0},
      {0, 1, 0, 0, S_WSIT_ID, S_WEB_SITE, S_WSIT_ID, 0},
      {0, 1, 0, 0, S_WSIT_OPEN_DATE, S_WEB_SITE, S_WSIT_OPEN_DATE, 0},
      {0, 1, 0, 0, S_WSIT_CLOSE_DATE, S_WEB_SITE, S_WSIT_CLOSE_DATE, 0},
      {0, 0, 0, 0, S_WSIT_NAME, S_WEB_SITE, S_WSIT_NAME, 0},
      {0, 1, 0, 0, S_WSIT_ADDRESS_ID, S_WEB_SITE, S_WSIT_ADDRESS_ID, 0},
      {0, 1, 0, 0, S_WSIT_DIVISION_ID, S_WEB_SITE, S_WSIT_DIVISION_ID, 0},
      {0, 1, 0, 0, S_WSIT_CLASS, S_WEB_SITE, S_WSIT_CLASS, 0},
      {0, 1, 0, 0, S_WSIT_MANAGER_ID, S_WEB_SITE, S_WSIT_MANAGER_ID, 0},
      {0, 1, 0, 0, S_WSIT_MARKET_ID, S_WEB_SITE, S_WSIT_MARKET_ID, 0},
      {0, 1, 0, 0, S_WSIT_TAX_PERCENTAGE, S_WEB_SITE, S_WSIT_TAX_PERCENTAGE, 0},
      {0, 0, 0, 0, S_ZIPG_ZIP, S_ZIPG, S_ZIPG_ZIP, 0},
      {0, 0, 0, 0, S_ZIPG_GMT, S_ZIPG, S_ZIPG_GMT, 0},
      {-1, -1, -1, -1, -1, -1, -1, 0}};

  void Reset();
};

/* must match WriteDist() in dcomp.c */
#define IDX_SIZE (D_NAME_LEN + 7 * sizeof(int))

int dist_op(
    void* dest,
    int op,
    const char* d_name,
    int vset,
    int wset,
    int stream,
    DSDGenContext& dsdGenContext);
#define pick_distribution(dest, dist, v, w, s, dsdGenContext) \
  dist_op(dest, 0, dist, v, w, s, dsdGenContext)
#define dist_member(dest, dist, v, w, dsdGenContext) \
  dist_op(dest, 1, dist, v, w, 0, dsdGenContext)
#define dist_max(dist, w, dsdGenContext) dist->maximums[w - 1]
int dist_weight(
    int* dest,
    const char* d,
    int index,
    int wset,
    DSDGenContext& dsdGenContext);
int distsize(const char* szDistname, DSDGenContext& dsdGenContext);
int dist_type(const char* szDistName, int vset, DSDGenContext& dsdGenContext);
const d_idx_t* find_dist(const char* name);
int IntegrateDist(
    const char* szDistName,
    int nPct,
    int nStartIndex,
    int nWeightSet,
    DSDGenContext& dsdGenContext);
void dump_dist(const char* szName, DSDGenContext& dsdGenContext);
int dist_active(
    const char* szName,
    int nWeightSet,
    DSDGenContext& dsdGenContext);
int DistNameIndex(
    const char* szDist,
    int nNameType,
    const char* szName,
    DSDGenContext& dsdGenContext);
int DistSizeToShiftWidth(
    const char* szDist,
    int nWeightSet,
    DSDGenContext& dsdGenContext);
int MatchDistWeight(
    void* dest,
    const char* szDist,
    int nWeight,
    int nWeightSet,
    int ValueSet,
    DSDGenContext& dsdGenContext);
int findDistValue(
    const char* szValue,
    const char* szDistName,
    int ValueSet,
    DSDGenContext& dsdGenContext);
int di_compare(const void* op1, const void* op2);
int load_dists();

#define DIST_UNIFORM 0x0001
#define DIST_EXPONENTIAL 0x0002
/* sales and returns are special; they must match calendar.dst */
#define DIST_SALES 3
#define DIST_RETURNS 5
#define DIST_CHAR 0x0004
#define DIST_INT 0x0008
#define DIST_NAMES_SET 0xff00

/* DistNameIndex needs to know what sort of name we are trying to match */
#define VALUE_NAME 0x0000
#define WEIGHT_NAME 0x0001
