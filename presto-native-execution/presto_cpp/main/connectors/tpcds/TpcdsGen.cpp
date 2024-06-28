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

#include "presto_cpp/main/connectors/tpcds/TpcdsGen.h"
#include "presto_cpp/main/connectors/tpcds/DSDGenIterator.h"

#define CALL_CENTER 0
#define DBGEN_VERSION 24
namespace facebook::velox::tpcds {

namespace {
size_t getVectorSize(size_t rowCount, size_t maxRows, size_t offset) {
  if (offset >= rowCount) {
    return 0;
  }
  return std::min(rowCount - offset, maxRows);
}

std::vector<VectorPtr> allocateVectors(
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

} // namespace

RowTypePtr getTableSchema(Table table) {
  switch (table) {
    case Table::TBL_CALL_CENTER: {
      static RowTypePtr type =
          ROW({"cc_call_center_sk", "cc_call_center_id",
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
              {INTEGER(),
               VARCHAR(),
               DATE(),
               DATE(),
               INTEGER(),
               INTEGER(),
               VARCHAR(),
               VARCHAR(),
               INTEGER(),
               INTEGER(),
               VARCHAR(),
               VARCHAR(),
               INTEGER(),
               VARCHAR(),
               VARCHAR(),
               VARCHAR(),
               INTEGER(),
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
               VARCHAR(),
               // TODO: this should be DECIMAL(5, 2) and not integer, recheck
               //  dsdgen variable
               DECIMAL(5, 2),
               DECIMAL(5, 2)});
      return type;
    }
    case Table::TBL_CATALOG_PAGE: {
      static RowTypePtr type =
          ROW({"cp_catalog_page_sk",
               "cp_catalog_page_id",
               "cp_start_date_sk",
               "cp_end_date_sk",
               "cp_department",
               "cp_catalog_number",
               "cp_catalog_page_number",
               "cp_description",
               "cp_type"},
              {INTEGER(),
               VARCHAR(),
               INTEGER(),
               INTEGER(),
               VARCHAR(),
               INTEGER(),
               INTEGER(),
               VARCHAR(),
               VARCHAR()});
      return type;
    }
    case Table::TBL_CATALOG_RETURNS: {
      static RowTypePtr type =
          ROW({"cr_returned_date_sk",
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
              {INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     DECIMAL(7, 2), DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2)});
      return type;
    }
    case Table::TBL_CATALOG_SALES: {
      static RowTypePtr type =
          ROW({"cs_sold_date_sk",
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
              {INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2)});
      return type;
    }
    case Table::TBL_CUSTOMER: {
      static RowTypePtr type = ROW(
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
              INTEGER(),
              VARCHAR(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
              INTEGER(),
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
              INTEGER(),
          });
      return type;
    }
    case Table::TBL_CUSTOMER_ADDRESS: {
      static RowTypePtr type =
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
              {INTEGER(),
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
               // todo: change to DECIMAL(5, 2)
               INTEGER(),
               VARCHAR()});
      return type;
    }
    case Table::TBL_CUSTOMER_DEMOGRAPHICS: {
      static RowTypePtr type =
          ROW({"cd_demo_sk",
               "cd_gender",
               "cd_marital_status",
               "cd_education_status",
               "cd_purchase_estimate",
               "cd_credit_rating",
               "cd_dep_count",
               "cd_dep_employed_count",
               "cd_dep_college_count"},
              {INTEGER(),
               VARCHAR(),
               VARCHAR(),
               VARCHAR(),
               INTEGER(),
               VARCHAR(),
               INTEGER(),
               INTEGER(),
               INTEGER()});
      return type;
    }
    case Table::TBL_DATE_DIM: {
      static RowTypePtr type = ROW(
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
              INTEGER(), VARCHAR(), DATE(),    INTEGER(), INTEGER(), INTEGER(),
              INTEGER(), INTEGER(), INTEGER(), INTEGER(), INTEGER(), INTEGER(),
              INTEGER(), INTEGER(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(),
              VARCHAR(), INTEGER(), INTEGER(), INTEGER(), INTEGER(), VARCHAR(),
              VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(),
          });
      return type;
    }
    case Table::TBL_HOUSEHOLD_DEMOGRAHICS: {
      static RowTypePtr type =
          ROW({"hd_demo_sk",
               "hd_income_band_sk",
               "hd_buy_potential",
               "hd_dep_count",
               "hd_vehicle_count"},
              {INTEGER(), INTEGER(), VARCHAR(), INTEGER(), INTEGER()});
      return type;
    }
    case Table::TBL_INCOME_BAND: {
      static RowTypePtr type =
          ROW({"ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"},
              {INTEGER(), INTEGER(), INTEGER()});
      return type;
    }
    case Table::TBL_INVENTORY: {
      static RowTypePtr type =
          ROW({"inv_date_sk",
               "inv_item_sk",
               "inv_warehouse_sk",
               "inv_quantity_on_hand"},
              {INTEGER(), INTEGER(), INTEGER(), INTEGER()});
      return type;
    }
    case Table::TBL_ITEM: {
      static RowTypePtr type =
          ROW({"i_item_sk",        "i_item_id",     "i_rec_start_date",
               "i_rec_end_date",   "i_item_desc",   "i_current_price",
               "i_wholesale_cost", "i_brand_id",    "i_brand",
               "i_class_id",       "i_class",       "i_category_id",
               "i_category",       "i_manufact_id", "i_manufact",
               "i_size",           "i_formulation", "i_color",
               "i_units",          "i_container",   "i_manager_id",
               "i_product_name"},
              {INTEGER(),     VARCHAR(),     DATE(),    DATE(),    VARCHAR(),
               DECIMAL(7, 2), DECIMAL(7, 2), INTEGER(), VARCHAR(), INTEGER(),
               VARCHAR(),     INTEGER(),     VARCHAR(), INTEGER(), VARCHAR(),
               VARCHAR(),     VARCHAR(),     VARCHAR(), VARCHAR(), VARCHAR(),
               INTEGER(),     VARCHAR()});
      return type;
    }
    case Table::TBL_PROMOTION: {
      static RowTypePtr type =
          ROW({"p_promo_sk",
               "p_promo_id",
               "p_start_date_sk",
               "p_end_date_sk",
               "p_item_sk",
               "p_cost",
               "p_response_target",
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
              {INTEGER(),
               VARCHAR(),
               INTEGER(),
               INTEGER(),
               INTEGER(),
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
      return type;
    }
    case Table::TBL_REASON: {
      static RowTypePtr type =
          ROW({"r_reason_sk", "r_reason_id", "r_reason_desc"},
              {INTEGER(), VARCHAR(), VARCHAR()});
      return type;
    }
    case Table::TBL_SHIP_MODE: {
      static RowTypePtr type = ROW(
          {"sm_ship_mode_sk",
           "sm_ship_mode_id",
           "sm_type",
           "sm_code",
           "sm_carrier",
           "sm_contract"},
          {INTEGER(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR()});
      return type;
    }
    case Table::TBL_STORE: {
      static RowTypePtr type = ROW(
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
              INTEGER(), VARCHAR(), DATE(),        DATE(),        INTEGER(),
              VARCHAR(), INTEGER(), INTEGER(),     VARCHAR(),     VARCHAR(),
              INTEGER(), VARCHAR(), VARCHAR(),     VARCHAR(),     INTEGER(),
              VARCHAR(), INTEGER(), VARCHAR(),     VARCHAR(),     VARCHAR(),
              VARCHAR(), VARCHAR(), VARCHAR(),     VARCHAR(),     VARCHAR(),
              VARCHAR(), VARCHAR(), DECIMAL(5, 2), DECIMAL(5, 2),
          });
      return type;
    }
    case Table::TBL_STORE_RETURNS: {
      static RowTypePtr type =
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
              {INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2)});
      return type;
    }
    case Table::TBL_STORE_SALES: {
      static RowTypePtr type =
          ROW({"ss_sold_date_sk",
               "ss_sold_time_sk",
               "ss_item_sk",
               "ss_customer_sk",
               "ss_cdemo_sk",
               "ss_hdemo_sk",
               "ss_addr_sk",
               "ss_store_sk",
               "ss_promo_sk",
               "ss_ticket_number",
               "ss_quantity",
               "ss_wholesale_cost",
               "ss_list_price",
               "ss_sales_price",
               "ss_ext_discount_amt",
               "ss_ext_sales_price",
               "ss_ext_wholesale_cost",
               "ss_ext_list_price",
               "ss_ext_tax",
               "ss_coupon_amt",
               "ss_net_paid",
               "ss_net_paid_inc_tax",
               "ss_net_profit"},
              {INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2)});
      return type;
    }
    case Table::TBL_TIME: {
      static RowTypePtr type =
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
              {INTEGER(),
               VARCHAR(),
               INTEGER(),
               INTEGER(),
               INTEGER(),
               INTEGER(),
               VARCHAR(),
               VARCHAR(),
               VARCHAR(),
               VARCHAR()});
      return type;
    }
    case Table::TBL_WAREHOUSE: {
      static RowTypePtr type =
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
              {INTEGER(),
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
               // todo: change to DECIMAL(5, 2)
               INTEGER()});
      return type;
    }
    case Table::TBL_WEB_PAGE: {
      static RowTypePtr type =
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
              {INTEGER(),
               VARCHAR(),
               DATE(),
               DATE(),
               INTEGER(),
               INTEGER(),
               VARCHAR(),
               INTEGER(),
               VARCHAR(),
               VARCHAR(),
               INTEGER(),
               INTEGER(),
               INTEGER(),
               INTEGER()});
      return type;
    }
    case Table::TBL_WEB_RETURNS: {
      static RowTypePtr type =
          ROW({"wr_returned_date_sk",
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
              {INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2)});
      return type;
    }
    case Table::TBL_WEB_SALES: {
      static RowTypePtr type =
          ROW({"ws_sold_date_sk",
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
              {INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     INTEGER(),
               INTEGER(),     INTEGER(),     INTEGER(),     DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2), DECIMAL(7, 2),
               DECIMAL(7, 2), DECIMAL(7, 2)});
      return type;
    }
    case Table::TBL_WEBSITE: {
      static RowTypePtr type =
          ROW({"web_site_sk",        "web_site_id",       "web_rec_start_date",
               "web_rec_end_date",   "web_name",          "web_open_date_sk",
               "web_close_date_sk",  "web_class",         "web_manager",
               "web_mkt_id",         "web_mkt_class",     "web_mkt_desc",
               "web_market_manager", "web_company_id",    "web_company_name",
               "web_street_number",  "web_street_name",   "web_street_type",
               "web_suite_number",   "web_city",          "web_county",
               "web_state",          "web_zip",           "web_country",
               "web_gmt_offset",     "web_tax_percentage"},
              {INTEGER(),    VARCHAR(), DATE(),    DATE(),    VARCHAR(),
               INTEGER(),    INTEGER(), VARCHAR(), VARCHAR(), INTEGER(),
               VARCHAR(),    VARCHAR(), VARCHAR(), INTEGER(), VARCHAR(),
               VARCHAR(),    VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(),
               VARCHAR(),    VARCHAR(), VARCHAR(), VARCHAR(), DECIMAL(5, 2),
               DECIMAL(5, 2)});
      return type;
    }
    default:
      return nullptr;
  }
  return nullptr; // make gcc happy.
}

std::string toTableName(Table table) {
  switch (table) {
    case Table::TBL_CALL_CENTER:
      return "call_center";
    case Table::TBL_CATALOG_PAGE:
      return "catalog_page";
    case Table::TBL_CATALOG_RETURNS:
      return "catalog_returns";
    case Table::TBL_CATALOG_SALES:
      return "catalog_sales";
    case Table::TBL_CUSTOMER:
      return "customer";
    case Table::TBL_CUSTOMER_ADDRESS:
      return "customer_address";
    case Table::TBL_CUSTOMER_DEMOGRAPHICS:
      return "customer_demographics";
    case Table::TBL_DATE_DIM:
      return "date_dim";
    case Table::TBL_HOUSEHOLD_DEMOGRAHICS:
      return "household_demographics";
    case Table::TBL_INCOME_BAND:
      return "income_band";
    case Table::TBL_INVENTORY:
      return "inventory";
    case Table::TBL_ITEM:
      return "item";
    case Table::TBL_PROMOTION:
      return "promotion";
    case Table::TBL_REASON:
      return "reason";
    case Table::TBL_SHIP_MODE:
      return "ship_mode";
    case Table::TBL_STORE:
      return "store";
    case Table::TBL_STORE_RETURNS:
      return "store_returns";
    case Table::TBL_STORE_SALES:
      return "store_sales";
    case Table::TBL_TIME:
      return "time";
    case Table::TBL_WAREHOUSE:
      return "warehouse";
    case Table::TBL_WEB_PAGE:
      return "web_page";
    case Table::TBL_WEB_RETURNS:
      return "web_returns";
    case Table::TBL_WEB_SALES:
      return "web_sales";
    case Table::TBL_WEBSITE:
      return "website";
    default:
      return "";
  }
  return ""; // make gcc happy.
}

TypePtr resolveTpcdsColumn(Table table, const std::string& columnName) {
  return getTableSchema(table)->findChild(columnName);
}

Table fromTableName(std::string_view tableName) {
  static std::unordered_map<std::string_view, Table> map{
      {"call_center", Table::TBL_CALL_CENTER},
      {"catalog_page", Table::TBL_CATALOG_PAGE},
      {"catalog_returns", Table::TBL_CATALOG_RETURNS},
      {"catalog_sales", Table::TBL_CATALOG_SALES},
      {"customer", Table::TBL_CUSTOMER},
      {"date_dim", Table::TBL_DATE_DIM},
      {"household_demographics", Table::TBL_HOUSEHOLD_DEMOGRAHICS},
      {"income_band", Table::TBL_INCOME_BAND},
      {"inventory", Table::TBL_INVENTORY},
      {"item", Table::TBL_ITEM},
      {"promotion", Table::TBL_PROMOTION},
      {"reason", Table::TBL_REASON},
      {"ship_mode", Table::TBL_SHIP_MODE},
      {"store", Table::TBL_STORE},
      {"store_returns", Table::TBL_STORE_RETURNS},
      {"store_sales", Table::TBL_STORE_SALES},
      {"time", Table::TBL_TIME},
      {"warehouse", Table::TBL_WAREHOUSE},
      {"web_page", Table::TBL_WEB_PAGE},
      {"web_returns", Table::TBL_WEB_RETURNS},
      {"web_sales", Table::TBL_WEB_SALES},
      {"website", Table::TBL_WEBSITE},
  };

  auto it = map.find(tableName);
  if (it != map.end()) {
    return it->second;
  }
  throw std::invalid_argument(
      fmt::format("Invalid TPC-DS table name: '{}'", tableName));
}

RowVectorPtr genTpcdsCallCenter(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  //  Create schema and allocate vector->childAts.
  auto callCenterRowType = getTableSchema(Table::TBL_CALL_CENTER);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_CALL_CENTER)),
      maxRows,
      offset);
  auto children = allocateVectors(callCenterRowType, vectorSize, pool);
  auto table_id = static_cast<int>(Table::TBL_CALL_CENTER);
  dsdGenIterator.initTableOffset(table_id, offset);
  dsdGenIterator.initializeTable(children, table_id);
  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      callCenterRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsCatalogPage(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto catalogPageRowType = getTableSchema(Table::TBL_CATALOG_PAGE);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_CATALOG_PAGE)),
      maxRows,
      offset);
  auto children = allocateVectors(catalogPageRowType, vectorSize, pool);
  auto table_id = static_cast<int>(Table::TBL_CATALOG_PAGE);
  dsdGenIterator.initTableOffset(table_id, offset);
  dsdGenIterator.initializeTable(children, table_id);
  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      catalogPageRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsCatalogReturns(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto catalogSalesRowType = getTableSchema(Table::TBL_CATALOG_SALES);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t catalogSalesVectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_CATALOG_SALES)),
      maxRows,
      offset);
  // todo: Verify this vector size.
  size_t catalogSalesUpperBound = catalogSalesVectorSize * 16;
  auto children =
      allocateVectors(catalogSalesRowType, catalogSalesUpperBound, pool);

  // This table is a dependent table on store_sales, this table will
  // be populated when store_sales is called so we call that first.
  // Create schema and allocate vectors.
  auto catalogReturnsRowType = getTableSchema(Table::TBL_CATALOG_RETURNS);
  auto childChildren =
      allocateVectors(catalogReturnsRowType, catalogSalesUpperBound, pool);

  auto table_id = static_cast<int>(Table::TBL_CATALOG_SALES);
  auto child_table_id = static_cast<int>(Table::TBL_CATALOG_RETURNS);

  dsdGenIterator.initializeTable(children, table_id);
  dsdGenIterator.initializeTable(childChildren, child_table_id);
  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < catalogSalesVectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }

  auto catalogReturnsRowCount = tableDef[child_table_id]->rowIndex;
  for (auto& child : tableDef[child_table_id]->children) {
    child->resize(catalogReturnsRowCount);
  }

  return std::make_shared<RowVector>(
      pool,
      catalogReturnsRowType,
      BufferPtr(nullptr),
      catalogReturnsRowCount,
      std::move(tableDef[child_table_id]->children));
}

RowVectorPtr genTpcdsCatalogSales(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto catalogSalesRowType = getTableSchema(Table::TBL_CATALOG_SALES);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t catalogSalesVectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_CATALOG_SALES)),
      maxRows,
      offset);
  // todo: Verify this vector size.
  size_t catalogSalesUpperBound = catalogSalesVectorSize * 16;
  auto children =
      allocateVectors(catalogSalesRowType, catalogSalesUpperBound, pool);

  // This table is a dependent table on store_sales, this table will
  // be populated when store_sales is called so we call that first.
  // Create schema and allocate vectors.
  auto catalogReturnsRowType = getTableSchema(Table::TBL_CATALOG_RETURNS);
  auto childChildren =
      allocateVectors(catalogReturnsRowType, catalogSalesUpperBound, pool);

  auto table_id = static_cast<int>(Table::TBL_CATALOG_SALES);
  auto child_table_id = static_cast<int>(Table::TBL_CATALOG_RETURNS);

  dsdGenIterator.initializeTable(children, table_id);
  dsdGenIterator.initializeTable(childChildren, child_table_id);
  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < catalogSalesVectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }

  auto catalogSalesRowCount = tableDef[table_id]->rowIndex;
  for (auto& child : tableDef[table_id]->children) {
    child->resize(catalogSalesRowCount);
  }
  return std::make_shared<RowVector>(
      pool,
      catalogSalesRowType,
      BufferPtr(nullptr),
      catalogSalesRowCount,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsCustomer(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  //  Create schema and allocate vector->childAts.
  auto customerRowType = getTableSchema(Table::TBL_CUSTOMER);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_CUSTOMER)),
      maxRows,
      offset);
  auto children = allocateVectors(customerRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_CUSTOMER);
  dsdGenIterator.initTableOffset(table_id, offset);
  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      customerRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsCustomerAddress(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto customerAddressRowType = getTableSchema(Table::TBL_CUSTOMER_ADDRESS);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_CUSTOMER_ADDRESS)),
      maxRows,
      offset);
  auto children = allocateVectors(customerAddressRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_CUSTOMER_ADDRESS);
  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      customerAddressRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsCustomerDemographics(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto customerDemographicsRowType =
      getTableSchema(Table::TBL_CUSTOMER_DEMOGRAPHICS);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(
          static_cast<int>(Table::TBL_CUSTOMER_DEMOGRAPHICS)),
      maxRows,
      offset);
  auto children =
      allocateVectors(customerDemographicsRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_CUSTOMER_DEMOGRAPHICS);
  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      customerDemographicsRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsDateDim(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  //  Create schema and allocate vector->childAts.
  auto dateDimRowType = getTableSchema(Table::TBL_DATE_DIM);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_DATE_DIM)),
      maxRows,
      offset);
  auto children = allocateVectors(dateDimRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_DATE_DIM);
  dsdGenIterator.initTableOffset(table_id, offset);
  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      dateDimRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsHouseholdDemographics(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto householdDemographicsRowType =
      getTableSchema(Table::TBL_HOUSEHOLD_DEMOGRAHICS);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(
          static_cast<int>(Table::TBL_HOUSEHOLD_DEMOGRAHICS)),
      maxRows,
      offset);
  auto children =
      allocateVectors(householdDemographicsRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_HOUSEHOLD_DEMOGRAHICS);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      householdDemographicsRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsIncomeBand(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto incomeBandRowType = getTableSchema(Table::TBL_INCOME_BAND);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_INCOME_BAND)),
      maxRows,
      offset);
  auto children = allocateVectors(incomeBandRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_INCOME_BAND);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      incomeBandRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsInventory(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto inventoryRowType = getTableSchema(Table::TBL_INVENTORY);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_INVENTORY)),
      maxRows,
      offset);
  auto children = allocateVectors(inventoryRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_INVENTORY);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      inventoryRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsItem(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto itemRowType = getTableSchema(Table::TBL_ITEM);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_ITEM)),
      maxRows,
      offset);
  auto children = allocateVectors(itemRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_ITEM);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      itemRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsPromotion(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto promotionRowType = getTableSchema(Table::TBL_PROMOTION);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_PROMOTION)),
      maxRows,
      offset);
  auto children = allocateVectors(promotionRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_PROMOTION);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      promotionRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsReason(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto reasonRowType = getTableSchema(Table::TBL_REASON);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_REASON)),
      maxRows,
      offset);
  auto children = allocateVectors(reasonRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_REASON);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      reasonRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsShipMode(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto shipModeRowType = getTableSchema(Table::TBL_SHIP_MODE);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_SHIP_MODE)),
      maxRows,
      offset);
  auto children = allocateVectors(shipModeRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_SHIP_MODE);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      shipModeRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsStore(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto storeRowType = getTableSchema(Table::TBL_STORE);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_STORE)),
      maxRows,
      offset);
  auto children = allocateVectors(storeRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_STORE);
  dsdGenIterator.initTableOffset(table_id, offset);
  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      storeRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsStoreReturns(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  // Create schema and allocate vectors.
  auto storeSalesRowType = getTableSchema(Table::TBL_STORE_SALES);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  // todo: Verify this vector size, for now using storeSalesVectorSize.
  size_t storeSalesVectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_STORE_SALES)),
      maxRows,
      offset);
  // todo: Verify this vector size, for now using storeSalesVectorSize.
  size_t storeSalesUpperBound = storeSalesVectorSize * 16;
  auto children =
      allocateVectors(storeSalesRowType, storeSalesUpperBound, pool);

  // This table is a dependent table on store_sales, this table will
  // be populated when store_sales is called so we call that first.
  // Create schema and allocate vectors.
  auto storeReturnsRowType = getTableSchema(Table::TBL_STORE_RETURNS);
  auto childChildren =
      allocateVectors(storeReturnsRowType, storeSalesUpperBound, pool);

  auto table_id = static_cast<int>(Table::TBL_STORE_SALES);
  auto child_table_id = static_cast<int>(Table::TBL_STORE_RETURNS);
  
  dsdGenIterator.initTableOffset(table_id, offset);
  dsdGenIterator.initializeTable(children, table_id);
  dsdGenIterator.initializeTable(childChildren, child_table_id);
  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < storeSalesVectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }

  auto storeReturnsRowCount = tableDef[child_table_id]->rowIndex;
  for (auto& child : tableDef[child_table_id]->children) {
    child->resize(storeReturnsRowCount);
  }

  return std::make_shared<RowVector>(
      pool,
      storeReturnsRowType,
      BufferPtr(nullptr),
      storeReturnsRowCount,
      std::move(tableDef[child_table_id]->children));
}

RowVectorPtr genTpcdsStoreSales(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  // Create schema and allocate vectors.
  auto storeSalesRowType = getTableSchema(Table::TBL_STORE_SALES);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  // todo: Verify this vector size, for now using storeSalesVectorSize.
  size_t storeSalesVectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_STORE_SALES)),
      maxRows,
      offset);
  // todo: Verify this vector size, for now using storeSalesVectorSize.
  size_t storeSalesUpperBound = storeSalesVectorSize * 16;
  auto children =
      allocateVectors(storeSalesRowType, storeSalesUpperBound, pool);

  // This table is a dependent table on store_sales, this table will
  // be populated when store_sales is called so we call that first.
  // Create schema and allocate vectors.
  auto storeReturnsRowType = getTableSchema(Table::TBL_STORE_RETURNS);
  auto childChildren =
      allocateVectors(storeReturnsRowType, storeSalesUpperBound, pool);

  auto table_id = static_cast<int>(Table::TBL_STORE_SALES);
  auto child_table_id = static_cast<int>(Table::TBL_STORE_RETURNS);
  dsdGenIterator.initTableOffset(table_id, offset);
  dsdGenIterator.initializeTable(children, table_id);
  dsdGenIterator.initializeTable(childChildren, child_table_id);
  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < storeSalesVectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }

  auto storeSalesRowCount = tableDef[table_id]->rowIndex;
  for (auto& child : tableDef[table_id]->children) {
    child->resize(storeSalesRowCount);
  }
  return std::make_shared<RowVector>(
      pool,
      storeSalesRowType,
      BufferPtr(nullptr),
      storeSalesRowCount,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsTime(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto timeRowType = getTableSchema(Table::TBL_TIME);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_TIME)),
      maxRows,
      offset);
  auto children = allocateVectors(timeRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_TIME);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      timeRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsWarehouse(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto warehouseRowType = getTableSchema(Table::TBL_WAREHOUSE);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_WAREHOUSE)),
      maxRows,
      offset);
  auto children = allocateVectors(warehouseRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_WAREHOUSE);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      warehouseRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsWebpage(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto webpageRowType = getTableSchema(Table::TBL_WEB_PAGE);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_WEB_PAGE)),
      maxRows,
      offset);
  auto children = allocateVectors(webpageRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_WEB_PAGE);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      webpageRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsWebReturns(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto webReturnsRowType = getTableSchema(Table::TBL_WEB_RETURNS);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_WEB_RETURNS)),
      maxRows,
      offset);
  auto children = allocateVectors(webReturnsRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_WEB_RETURNS);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      webReturnsRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsWebSales(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto webSalesRowType = getTableSchema(Table::TBL_WEB_SALES);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_WEB_SALES)),
      maxRows,
      offset);
  auto children = allocateVectors(webSalesRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_WEB_SALES);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      webSalesRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}

RowVectorPtr genTpcdsWebsite(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  auto websiteRowType = getTableSchema(Table::TBL_WEBSITE);
  DSDGenIterator dsdGenIterator(scaleFactor, parallel, child);
  size_t vectorSize = getVectorSize(
      dsdGenIterator.getRowCount(static_cast<int>(Table::TBL_WEBSITE)),
      maxRows,
      offset);
  auto children = allocateVectors(websiteRowType, vectorSize, pool);

  auto table_id = static_cast<int>(Table::TBL_WEBSITE);

  dsdGenIterator.initializeTable(children, table_id);

  auto& tableDef = dsdGenIterator.getTableDefs();
  for (size_t i = 0; i < vectorSize; ++i) {
    dsdGenIterator.genRow(table_id, i + offset + 1);
  }
  return std::make_shared<RowVector>(
      pool,
      websiteRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(tableDef[table_id]->children));
}
} // namespace facebook::velox::tpcds
