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

#include "velox/tpcds/gen/TpcdsGen.h"
#include "velox/tpcds/gen/DSDGenIterator.h"
#include "velox/vector/ComplexVector.h"

using namespace facebook::velox;
namespace facebook::velox::tpcds {

namespace {
// The maximum number of rows that can be returned by dsdgen's data generation
// functions, invoked with function DSDGenIterator::genRow(), for any of the
// parent tables (TBL_CATALOG_SALES, TBL_STORE_SALES, or TBL_WEB_SALES) is 16.
static constexpr vector_size_t kMaxParentTableRows = 16;

std::unordered_map<std::string_view, Table> tpcdsTableMap() {
  static const std::unordered_map<std::string_view, Table> map{
      {"call_center", Table::TBL_CALL_CENTER},
      {"catalog_page", Table::TBL_CATALOG_PAGE},
      {"catalog_returns", Table::TBL_CATALOG_RETURNS},
      {"catalog_sales", Table::TBL_CATALOG_SALES},
      {"customer", Table::TBL_CUSTOMER},
      {"customer_address", Table::TBL_CUSTOMER_ADDRESS},
      {"customer_demographics", Table::TBL_CUSTOMER_DEMOGRAPHICS},
      {"date_dim", Table::TBL_DATE_DIM},
      {"household_demographics", Table::TBL_HOUSEHOLD_DEMOGRAPHICS},
      {"income_band", Table::TBL_INCOME_BAND},
      {"inventory", Table::TBL_INVENTORY},
      {"item", Table::TBL_ITEM},
      {"promotion", Table::TBL_PROMOTION},
      {"reason", Table::TBL_REASON},
      {"ship_mode", Table::TBL_SHIP_MODE},
      {"store", Table::TBL_STORE},
      {"store_returns", Table::TBL_STORE_RETURNS},
      {"store_sales", Table::TBL_STORE_SALES},
      {"time_dim", Table::TBL_TIME_DIM},
      {"warehouse", Table::TBL_WAREHOUSE},
      {"web_page", Table::TBL_WEB_PAGE},
      {"web_returns", Table::TBL_WEB_RETURNS},
      {"web_sales", Table::TBL_WEB_SALES},
      {"web_site", Table::TBL_WEB_SITE},
  };
  return map;
}

std::unordered_map<Table, std::string_view> invertTpcdsTableMap() {
  std::unordered_map<Table, std::string_view> inverted;
  for (const auto& [key, value] : tpcdsTableMap()) {
    inverted.emplace(value, key);
  }
  return inverted;
}

size_t getVectorSize(size_t rowCount, size_t maxRows, size_t offset) {
  if (offset >= rowCount) {
    return 0;
  }
  return std::min(rowCount - offset, maxRows);
}

std::vector<VectorPtr> allocateChildVectors(
    const RowTypePtr& type,
    size_t vectorSize,
    memory::MemoryPool* pool) {
  std::vector<VectorPtr> vector;
  vector.reserve(type->size());

  for (const auto& childType : type->children()) {
    vector.emplace_back(BaseVector::create(childType, vectorSize, pool));
  }
  return vector;
}

RowVectorPtr genTpcdsRowVector(
    int32_t tableId,
    memory::MemoryPool* pool,
    RowTypePtr rowType,
    std::vector<std::unique_ptr<TpcdsTableDef>>& tableDef) {
  auto rowCount = tableDef[tableId]->rowIndex;
  for (auto& child : tableDef[tableId]->children) {
    child->resize(rowCount);
  }

  return std::make_shared<RowVector>(
      pool,
      rowType,
      BufferPtr(nullptr),
      rowCount,
      std::move(tableDef[tableId]->children));
}

RowVectorPtr genTpcdsTableData(
    Table table,
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    int32_t parallel,
    int32_t child) {
  //  Create schema and allocate vectors.
  auto rowType = getTableSchema(table);
  auto table_id = static_cast<int>(table);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize =
      getVectorSize(dsdGenIterator.getRowCount(table_id), maxRows, offset);
  auto children = allocateChildVectors(rowType, vectorSize, pool);
  dsdGenIterator.initTableOffset(table_id, offset);
  dsdGenIterator.initializeTable(children, table_id);
  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      rowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsParentAndChildTable(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    int32_t parallel,
    int32_t child,
    Table parentTable,
    Table childTable,
    bool childTableCall) {
  // Whenever a call to generate a table which is either marked as a
  // parent or a child is requested, both the child and parent tables
  // need to be populated.

  auto parentTableType = getTableSchema(parentTable);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t parentTableVectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(parentTable)),
      maxRows,
      offset);

  size_t parentTableUpperBound = parentTableVectorSize * kMaxParentTableRows;
  auto children =
      allocateChildVectors(parentTableType, parentTableUpperBound, pool);

  auto childTableType = getTableSchema(childTable);
  auto childChildren =
      allocateChildVectors(childTableType, parentTableUpperBound, pool);

  auto parentTableId = static_cast<int>(parentTable);
  auto childTableId = static_cast<int>(childTable);

  dsdGenIterator.initTableOffset(parentTableId, offset);
  dsdGenIterator.initializeTable(children, parentTableId);
  dsdGenIterator.initializeTable(childChildren, childTableId);
  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < parentTableVectorSize; ++i) {
    dsdGenIterator.genRow(parentTableId, i + offset + 1);
  }
  if (childTableCall) {
    return genTpcdsRowVector(childTableId, pool, childTableType, tableDef);
  } else {
    return genTpcdsRowVector(parentTableId, pool, parentTableType, tableDef);
  }
}
} // namespace

static const RowTypePtr callCenterType = ROW(
    {"cc_call_center_sk", "cc_call_center_id",
     "cc_rec_start_date", "cc_rec_end_date",
     "cc_closed_date_sk", "cc_open_date_sk",
     "cc_name",           "cc_class",
     "cc_employees",      "cc_sq_ft",
     "cc_hours",          "cc_manager",
     "cc_mkt_id",         "cc_mkt_class",
     "cc_mkt_desc",       "cc_market_manager",
     "cc_division",       "cc_division_name",
     "cc_company",        "cc_company_name",
     "cc_street_number",  "cc_street_name",
     "cc_street_type",    "cc_suite_number",
     "cc_city",           "cc_county",
     "cc_state",          "cc_zip",
     "cc_country",        "cc_gmt_offset",
     "cc_tax_percentage"},
    {BIGINT(),     VARCHAR(), DATE(),    DATE(),    INTEGER(), INTEGER(),
     VARCHAR(),    VARCHAR(), INTEGER(), INTEGER(), VARCHAR(), VARCHAR(),
     INTEGER(),    VARCHAR(), VARCHAR(), VARCHAR(), INTEGER(), VARCHAR(),
     INTEGER(),    VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(),
     VARCHAR(),    VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), DECIMAL(5, 2),
     DECIMAL(5, 2)});

static const RowTypePtr catalogPageType =
    ROW({"cp_catalog_page_sk",
         "cp_catalog_page_id",
         "cp_start_date_sk",
         "cp_end_date_sk",
         "cp_department",
         "cp_catalog_number",
         "cp_catalog_page_number",
         "cp_description",
         "cp_type"},
        {BIGINT(),
         VARCHAR(),
         INTEGER(),
         INTEGER(),
         VARCHAR(),
         INTEGER(),
         INTEGER(),
         VARCHAR(),
         VARCHAR()});

static const RowTypePtr catalogReturnsType = ROW(
    {"cr_returned_date_sk",
     "cr_returned_time_sk",
     "cr_item_sk",
     "cr_refunded_customer_sk",
     "cr_refunded_cdemo_sk",
     "cr_refunded_hdemo_sk",
     "cr_refunded_addr_sk",
     "cr_returning_customer_sk",
     "cr_returning_cdemo_sk",
     "cr_returning_hdemo_sk",
     "cr_returning_addr_sk",
     "cr_call_center_sk",
     "cr_catalog_page_sk",
     "cr_ship_mode_sk",
     "cr_warehouse_sk",
     "cr_reason_sk",
     "cr_order_number",
     "cr_return_quantity",
     "cr_return_amount",
     "cr_return_tax",
     "cr_return_amt_inc_tax",
     "cr_fee",
     "cr_return_ship_cost",
     "cr_refunded_cash",
     "cr_reversed_charge",
     "cr_store_credit",
     "cr_net_loss"},
    {BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     BIGINT(),      BIGINT(),      INTEGER(),     DECIMAL(7, 2), DECIMAL(7, 2),
     DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
     DECIMAL(7, 2), DECIMAL(7, 2)});

static const RowTypePtr catalogSalesType = ROW(
    {"cs_sold_date_sk",
     "cs_sold_time_sk",
     "cs_ship_date_sk",
     "cs_bill_customer_sk",
     "cs_bill_cdemo_sk",
     "cs_bill_hdemo_sk",
     "cs_bill_addr_sk",
     "cs_ship_customer_sk",
     "cs_ship_cdemo_sk",
     "cs_ship_hdemo_sk",
     "cs_ship_addr_sk",
     "cs_call_center_sk",
     "cs_catalog_page_sk",
     "cs_ship_mode_sk",
     "cs_warehouse_sk",
     "cs_item_sk",
     "cs_promo_sk",
     "cs_order_number",
     "cs_quantity",
     "cs_wholesale_cost",
     "cs_list_price",
     "cs_sales_price",
     "cs_ext_discount_amt",
     "cs_ext_sales_price",
     "cs_ext_wholesale_cost",
     "cs_ext_list_price",
     "cs_ext_tax",
     "cs_coupon_amt",
     "cs_ext_ship_cost",
     "cs_net_paid",
     "cs_net_paid_inc_tax",
     "cs_net_paid_inc_ship",
     "cs_net_paid_inc_ship_tax",
     "cs_net_profit"},
    {BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     BIGINT(),      BIGINT(),      BIGINT(),      INTEGER(),     DECIMAL(7, 2),
     DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
     DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
     DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2)});

static const RowTypePtr customerType = ROW(
    {
        "c_customer_sk",
        "c_customer_id",
        "c_current_cdemo_sk",
        "c_current_hdemo_sk",
        "c_current_addr_sk",
        "c_first_shipto_date_sk",
        "c_first_sales_date_sk",
        "c_salutation",
        "c_first_name",
        "c_last_name",
        "c_preferred_cust_flag",
        "c_birth_day",
        "c_birth_month",
        "c_birth_year",
        "c_birth_country",
        "c_login",
        "c_email_address",
        "c_last_review_date_sk",
    },
    {
        BIGINT(),
        VARCHAR(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
        INTEGER(),
        INTEGER(),
        INTEGER(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
        BIGINT(),
    });

static const RowTypePtr customerAddressType =
    ROW({"ca_address_sk",
         "ca_address_id",
         "ca_street_number",
         "ca_street_name",
         "ca_street_type",
         "ca_suite_number",
         "ca_city",
         "ca_county",
         "ca_state",
         "ca_zip",
         "ca_country",
         "ca_gmt_offset",
         "ca_location_type"},
        {BIGINT(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         DECIMAL(5, 2),
         VARCHAR()});

static const RowTypePtr customerDemographicsType =
    ROW({"cd_demo_sk",
         "cd_gender",
         "cd_marital_status",
         "cd_education_status",
         "cd_purchase_estimate",
         "cd_credit_rating",
         "cd_dep_count",
         "cd_dep_employed_count",
         "cd_dep_college_count"},
        {BIGINT(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         INTEGER(),
         VARCHAR(),
         INTEGER(),
         INTEGER(),
         INTEGER()});

static const RowTypePtr dateDimType = ROW(
    {
        "d_date_sk",
        "d_date_id",
        "d_date",
        "d_month_seq",
        "d_week_seq",
        "d_quarter_seq",
        "d_year",
        "d_dow",
        "d_moy",
        "d_dom",
        "d_qoy",
        "d_fy_year",
        "d_fy_quarter_seq",
        "d_fy_week_seq",
        "d_day_name",
        "d_quarter_name",
        "d_holiday",
        "d_weekend",
        "d_following_holiday",
        "d_first_dom",
        "d_last_dom",
        "d_same_day_ly",
        "d_same_day_lq",
        "d_current_day",
        "d_current_week",
        "d_current_month",
        "d_current_quarter",
        "d_current_year",
    },
    {
        BIGINT(),  VARCHAR(), DATE(),    INTEGER(), INTEGER(), INTEGER(),
        INTEGER(), INTEGER(), INTEGER(), INTEGER(), INTEGER(), INTEGER(),
        INTEGER(), INTEGER(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(),
        VARCHAR(), INTEGER(), INTEGER(), INTEGER(), INTEGER(), VARCHAR(),
        VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(),
    });

static const RowTypePtr householdDemographicsType =
    ROW({"hd_demo_sk",
         "hd_income_band_sk",
         "hd_buy_potential",
         "hd_dep_count",
         "hd_vehicle_count"},
        {BIGINT(), BIGINT(), VARCHAR(), INTEGER(), INTEGER()});

static const RowTypePtr incomeBandType =
    ROW({"ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"},
        {BIGINT(), INTEGER(), INTEGER()});

static const RowTypePtr inventoryType = ROW(
    {"inv_date_sk", "inv_item_sk", "inv_warehouse_sk", "inv_quantity_on_hand"},
    {BIGINT(), BIGINT(), BIGINT(), INTEGER()});

static const RowTypePtr itemType = ROW(
    {"i_item_sk",     "i_item_id",       "i_rec_start_date", "i_rec_end_date",
     "i_item_desc",   "i_current_price", "i_wholesale_cost", "i_brand_id",
     "i_brand",       "i_class_id",      "i_class",          "i_category_id",
     "i_category",    "i_manufact_id",   "i_manufact",       "i_size",
     "i_formulation", "i_color",         "i_units",          "i_container",
     "i_manager_id",  "i_product_name"},
    {BIGINT(),      VARCHAR(), DATE(),    DATE(),    VARCHAR(), DECIMAL(7, 2),
     DECIMAL(7, 2), INTEGER(), VARCHAR(), INTEGER(), VARCHAR(), INTEGER(),
     VARCHAR(),     INTEGER(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(),
     VARCHAR(),     VARCHAR(), INTEGER(), VARCHAR()});

static const RowTypePtr promotionType =
    ROW({"p_promo_sk",
         "p_promo_id",
         "p_start_date_sk",
         "p_end_date_sk",
         "p_item_sk",
         "p_cost",
         "p_response_targe",
         "p_promo_name",
         "p_channel_dmail",
         "p_channel_email",
         "p_channel_catalog",
         "p_channel_tv",
         "p_channel_radio",
         "p_channel_press",
         "p_channel_event",
         "p_channel_demo",
         "p_channel_details",
         "p_purpose",
         "p_discount_active"},
        {BIGINT(),
         VARCHAR(),
         BIGINT(),
         BIGINT(),
         BIGINT(),
         DECIMAL(15, 2),
         INTEGER(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR()});

static const RowTypePtr reasonType =
    ROW({"r_reason_sk", "r_reason_id", "r_reason_desc"},
        {BIGINT(), VARCHAR(), VARCHAR()});

static const RowTypePtr shipModeType =
    ROW({"sm_ship_mode_sk",
         "sm_ship_mode_id",
         "sm_type",
         "sm_code",
         "sm_carrier",
         "sm_contract"},
        {BIGINT(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR()});

static const RowTypePtr storeType = ROW(
    {
        "s_store_sk",
        "s_store_id",
        "s_rec_start_date",
        "s_rec_end_date",
        "s_closed_date_sk",
        "s_store_name",
        "s_number_employees",
        "s_floor_space",
        "s_hours",
        "s_manager",
        "s_market_id",
        "s_geography_class",
        "s_market_desc",
        "s_market_manager",
        "s_division_id",
        "s_division_name",
        "s_company_id",
        "s_company_name",
        "s_street_number",
        "s_street_name",
        "s_street_type",
        "s_suite_number",
        "s_city",
        "s_county",
        "s_state",
        "s_zip",
        "s_country",
        "s_gmt_offset",
        "s_tax_precentage",
    },
    {
        BIGINT(),  VARCHAR(), DATE(),        DATE(),        BIGINT(),
        VARCHAR(), INTEGER(), INTEGER(),     VARCHAR(),     VARCHAR(),
        INTEGER(), VARCHAR(), VARCHAR(),     VARCHAR(),     INTEGER(),
        VARCHAR(), INTEGER(), VARCHAR(),     VARCHAR(),     VARCHAR(),
        VARCHAR(), VARCHAR(), VARCHAR(),     VARCHAR(),     VARCHAR(),
        VARCHAR(), VARCHAR(), DECIMAL(5, 2), DECIMAL(5, 2),
    });

static const RowTypePtr storeReturnsType =
    ROW({"sr_returned_date_sk",
         "sr_return_time_sk",
         "sr_item_sk",
         "sr_customer_sk",
         "sr_cdemo_sk",
         "sr_hdemo_sk",
         "sr_addr_sk",
         "sr_store_sk",
         "sr_reason_sk",
         "sr_ticket_number",
         "sr_return_quantity",
         "sr_return_amt",
         "sr_return_tax",
         "sr_return_amt_inc_tax",
         "sr_fee",
         "sr_return_ship_cost",
         "sr_refunded_cash",
         "sr_reversed_charge",
         "sr_store_credit",
         "sr_net_loss"},
        {BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
         BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
         BIGINT(),      BIGINT(),      INTEGER(),     DECIMAL(7, 2),
         DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
         DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2)});

static const RowTypePtr storeSalesType = ROW(
    {"ss_sold_date_sk",     "ss_sold_time_sk",       "ss_item_sk",
     "ss_customer_sk",      "ss_cdemo_sk",           "ss_hdemo_sk",
     "ss_addr_sk",          "ss_store_sk",           "ss_promo_sk",
     "ss_ticket_number",    "ss_quantity",           "ss_wholesale_cost",
     "ss_list_price",       "ss_sales_price",        "ss_ext_discount_amt",
     "ss_ext_sales_price",  "ss_ext_wholesale_cost", "ss_ext_list_price",
     "ss_ext_tax",          "ss_coupon_amt",         "ss_net_paid",
     "ss_net_paid_inc_tax", "ss_net_profit"},
    {BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     INTEGER(),     DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
     DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
     DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2)});

static const RowTypePtr timeDimType =
    ROW({"t_time_sk",
         "t_time_id",
         "t_time",
         "t_hour",
         "t_minute",
         "t_second",
         "t_am_pm",
         "t_shift",
         "t_sub_shift",
         "t_meal_time"},
        {BIGINT(),
         VARCHAR(),
         INTEGER(),
         INTEGER(),
         INTEGER(),
         INTEGER(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR()});

static const RowTypePtr warehouseType =
    ROW({"w_warehouse_sk",
         "w_warehouse_id",
         "w_warehouse_name",
         "w_warehouse_sq_ft",
         "w_street_number",
         "w_street_name",
         "w_street_type",
         "w_suite_number",
         "w_city",
         "w_county",
         "w_state",
         "w_zip",
         "w_country",
         "w_gmt_offset"},
        {BIGINT(),
         VARCHAR(),
         VARCHAR(),
         INTEGER(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         DECIMAL(5, 2)});

static const RowTypePtr webPageType =
    ROW({"wp_web_page_sk",
         "wp_web_page_id",
         "wp_rec_start_date",
         "wp_rec_end_date",
         "wp_creation_date_sk",
         "wp_access_date_sk",
         "wp_autogen_flag",
         "wp_customer_sk",
         "wp_url",
         "wp_type",
         "wp_char_count",
         "wp_link_count",
         "wp_image_count",
         "wp_max_ad_count"},
        {BIGINT(),
         VARCHAR(),
         DATE(),
         DATE(),
         BIGINT(),
         BIGINT(),
         VARCHAR(),
         BIGINT(),
         VARCHAR(),
         VARCHAR(),
         INTEGER(),
         INTEGER(),
         INTEGER(),
         INTEGER()});

static const RowTypePtr webReturnsType = ROW(
    {"wr_returned_date_sk",
     "wr_returned_time_sk",
     "wr_item_sk",
     "wr_refunded_customer_sk",
     "wr_refunded_cdemo_sk",
     "wr_refunded_hdemo_sk",
     "wr_refunded_addr_sk",
     "wr_returning_customer_sk",
     "wr_returning_cdemo_sk",
     "wr_returning_hdemo_sk",
     "wr_returning_addr_sk",
     "wr_web_page_sk",
     "wr_reason_sk",
     "wr_order_number",
     "wr_return_quantity",
     "wr_return_amt",
     "wr_return_tax",
     "wr_return_amt_inc_tax",
     "wr_fee",
     "wr_return_ship_cost",
     "wr_refunded_cash",
     "wr_reversed_charge",
     "wr_account_credit",
     "wr_net_loss"},
    {BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      INTEGER(),
     DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
     DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2)});

static const RowTypePtr webSalesType = ROW(
    {"ws_sold_date_sk",
     "ws_sold_time_sk",
     "ws_ship_date_sk",
     "ws_item_sk",
     "ws_bill_customer_sk",
     "ws_bill_cdemo_sk",
     "ws_bill_hdemo_sk",
     "ws_bill_addr_sk",
     "ws_ship_customer_sk",
     "ws_ship_cdemo_sk",
     "ws_ship_hdemo_sk",
     "ws_ship_addr_sk",
     "ws_web_page_sk",
     "ws_web_site_sk",
     "ws_ship_mode_sk",
     "ws_warehouse_sk",
     "ws_promo_sk",
     "ws_order_number",
     "ws_quantity",
     "ws_wholesale_cost",
     "ws_list_price",
     "ws_sales_price",
     "ws_ext_discount_amt",
     "ws_ext_sales_price",
     "ws_ext_wholesale_cost",
     "ws_ext_list_price",
     "ws_ext_tax",
     "ws_coupon_amt",
     "ws_ext_ship_cost",
     "ws_net_paid",
     "ws_net_paid_inc_tax",
     "ws_net_paid_inc_ship",
     "ws_net_paid_inc_ship_tax",
     "ws_net_profit"},
    {BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),      BIGINT(),
     BIGINT(),      BIGINT(),      BIGINT(),      INTEGER(),     DECIMAL(7, 2),
     DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
     DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
     DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2)});

static const RowTypePtr websiteType = ROW(
    {"web_site_sk",        "web_site_id",       "web_rec_start_date",
     "web_rec_end_date",   "web_name",          "web_open_date_sk",
     "web_close_date_sk",  "web_class",         "web_manager",
     "web_mkt_id",         "web_mkt_class",     "web_mkt_desc",
     "web_market_manager", "web_company_id",    "web_company_name",
     "web_street_number",  "web_street_name",   "web_street_type",
     "web_suite_number",   "web_city",          "web_county",
     "web_state",          "web_zip",           "web_country",
     "web_gmt_offset",     "web_tax_percentage"},
    {BIGINT(),      VARCHAR(),    DATE(),    DATE(),    VARCHAR(), BIGINT(),
     BIGINT(),      VARCHAR(),    VARCHAR(), INTEGER(), VARCHAR(), VARCHAR(),
     VARCHAR(),     INTEGER(),    VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(),
     VARCHAR(),     VARCHAR(),    VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(),
     DECIMAL(5, 2), DECIMAL(5, 2)});

const RowTypePtr getTableSchema(Table table) {
  switch (table) {
    case Table::TBL_CALL_CENTER:
      return callCenterType;
    case Table::TBL_CATALOG_PAGE:
      return catalogPageType;
    case Table::TBL_CATALOG_RETURNS:
      return catalogReturnsType;
    case Table::TBL_CATALOG_SALES:
      return catalogSalesType;
    case Table::TBL_CUSTOMER:
      return customerType;
    case Table::TBL_CUSTOMER_ADDRESS:
      return customerAddressType;
    case Table::TBL_CUSTOMER_DEMOGRAPHICS:
      return customerDemographicsType;
    case Table::TBL_DATE_DIM:
      return dateDimType;
    case Table::TBL_HOUSEHOLD_DEMOGRAPHICS:
      return householdDemographicsType;
    case Table::TBL_INCOME_BAND:
      return incomeBandType;
    case Table::TBL_INVENTORY:
      return inventoryType;
    case Table::TBL_ITEM:
      return itemType;
    case Table::TBL_PROMOTION:
      return promotionType;
    case Table::TBL_REASON:
      return reasonType;
    case Table::TBL_SHIP_MODE:
      return shipModeType;
    case Table::TBL_STORE:
      return storeType;
    case Table::TBL_STORE_RETURNS:
      return storeReturnsType;
    case Table::TBL_STORE_SALES:
      return storeSalesType;
    case Table::TBL_TIME_DIM:
      return timeDimType;
    case Table::TBL_WAREHOUSE:
      return warehouseType;
    case Table::TBL_WEB_PAGE:
      return webPageType;
    case Table::TBL_WEB_RETURNS:
      return webReturnsType;
    case Table::TBL_WEB_SALES:
      return webSalesType;
    case Table::TBL_WEB_SITE:
      return websiteType;
    default:
      VELOX_UNREACHABLE();
  }
}

std::string_view toTableName(Table table) {
  auto inverted = invertTpcdsTableMap();
  auto it = inverted.find(table);
  if (it != inverted.end()) {
    return inverted.at(table);
  }
  VELOX_UNREACHABLE("Invalid TPC-DS table: {}", static_cast<uint8_t>(table));
}

Table fromTableName(std::string_view tableName) {
  auto map = tpcdsTableMap();
  auto it = map.find(tableName);
  if (it != map.end()) {
    return it->second;
  }
  VELOX_UNREACHABLE("Invalid TPC-DS table name: {}", tableName);
}

TypePtr resolveTpcdsColumn(Table table, const std::string& columnName) {
  return getTableSchema(table)->findChild(columnName);
}

RowVectorPtr genTpcdsData(
    Table table,
    size_t maxRows,
    size_t offset,
    memory::MemoryPool* pool,
    double scaleFactor,
    int32_t parallel,
    int32_t child) {
  switch (table) {
    Table parentTable, childTable;
    case Table::TBL_CATALOG_RETURNS:
    case Table::TBL_CATALOG_SALES:
      parentTable = Table::TBL_CATALOG_SALES;
      childTable = Table::TBL_CATALOG_RETURNS;
      return genTpcdsParentAndChildTable(
          pool,
          maxRows,
          offset,
          scaleFactor,
          parallel,
          child,
          parentTable,
          childTable,
          table == childTable);
    case Table::TBL_WEB_RETURNS:
    case Table::TBL_WEB_SALES:
      parentTable = Table::TBL_WEB_SALES;
      childTable = Table::TBL_WEB_RETURNS;
      return genTpcdsParentAndChildTable(
          pool,
          maxRows,
          offset,
          scaleFactor,
          parallel,
          child,
          parentTable,
          childTable,
          table == childTable);
    case Table::TBL_STORE_RETURNS:
    case Table::TBL_STORE_SALES:
      parentTable = Table::TBL_STORE_SALES;
      childTable = Table::TBL_STORE_RETURNS;
      return genTpcdsParentAndChildTable(
          pool,
          maxRows,
          offset,
          scaleFactor,
          parallel,
          child,
          parentTable,
          childTable,
          table == childTable);
    case Table::TBL_CALL_CENTER:
    case Table::TBL_CATALOG_PAGE:
    case Table::TBL_CUSTOMER:
    case Table::TBL_CUSTOMER_ADDRESS:
    case Table::TBL_CUSTOMER_DEMOGRAPHICS:
    case Table::TBL_DATE_DIM:
    case Table::TBL_HOUSEHOLD_DEMOGRAPHICS:
    case Table::TBL_INCOME_BAND:
    case Table::TBL_INVENTORY:
    case Table::TBL_ITEM:
    case Table::TBL_PROMOTION:
    case Table::TBL_REASON:
    case Table::TBL_SHIP_MODE:
    case Table::TBL_STORE:
    case Table::TBL_TIME_DIM:
    case Table::TBL_WAREHOUSE:
    case Table::TBL_WEB_PAGE:
    case Table::TBL_WEB_SITE:
      return genTpcdsTableData(
          table, pool, maxRows, offset, scaleFactor, parallel, child);
    default:
      VELOX_UNREACHABLE();
  }
}
} // namespace facebook::velox::tpcds
