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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_EXECUTION_TIME;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_RUN_TIME;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractTestNativeTpcdsQueries
        extends AbstractTestQueryFramework
{
    String storageFormat = "DWRF";
    Session session;

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        this.session = Session.builder(queryRunner.getDefaultSession())
                .setSchema("tpcds")
                .setSystemProperty(QUERY_MAX_RUN_TIME, "2m")
                .setSystemProperty(QUERY_MAX_EXECUTION_TIME, "2m")
                .build();

        createTpcdsCallCenter(queryRunner, session, storageFormat);
        createTpcdsCatalogPage(queryRunner, session);
        createTpcdsCatalogReturns(queryRunner, session);
        createTpcdsCatalogSales(queryRunner, session);
        createTpcdsCustomer(queryRunner, session);
        createTpcdsCustomerAddress(queryRunner, session);
        createTpcdsCustomerDemographics(queryRunner, session);
        createTpcdsDateDim(queryRunner, session, storageFormat);
        createTpcdsHouseholdDemographics(queryRunner, session);
        createTpcdsIncomeBand(queryRunner, session);
        createTpcdsInventory(queryRunner, session);
        createTpcdsItem(queryRunner, session, storageFormat);
        createTpcdsPromotion(queryRunner, session);
        createTpcdsReason(queryRunner, session);
        createTpcdsShipMode(queryRunner, session);
        createTpcdsStore(queryRunner, session, storageFormat);
        createTpcdsStoreReturns(queryRunner, session);
        createTpcdsStoreSales(queryRunner, session);
        createTpcdsTimeDim(queryRunner, session);
        createTpcdsWarehouse(queryRunner, session);
        createTpcdsWebPage(queryRunner, session, storageFormat);
        createTpcdsWebReturns(queryRunner, session);
        createTpcdsWebSales(queryRunner, session);
        createTpcdsWebSite(queryRunner, session, storageFormat);
    }

    private static void createTpcdsCallCenter(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "call_center")) {
            switch (storageFormat) {
                case "PARQUET":
                    queryRunner.execute(session, "CREATE TABLE call_center AS " +
                            "SELECT cc_call_center_sk, cast(cc_call_center_id as varchar) as cc_call_center_id, cc_rec_start_date, cc_rec_end_date, " +
                            "   cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cast(cc_hours as varchar) as cc_hours, " +
                            "   cc_manager, cc_mkt_id, cast(cc_mkt_class as varchar) as cc_mkt_class, cc_mkt_desc, cc_market_manager,  " +
                            "   cc_division, cc_division_name, cc_company, cast(cc_company_name as varchar) as cc_company_name," +
                            "   cast(cc_street_number as varchar ) as cc_street_number, cc_street_name, cast(cc_street_type as varchar) as cc_street_type, " +
                            "   cast(cc_suite_number as varchar) as cc_suite_number, cc_city, cc_county, cast(cc_state as varchar) as cc_state, " +
                            "   cast(cc_zip as varchar) as cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage " +
                            "FROM tpcds.tiny.call_center");
                    break;
                case "DWRF":
                    queryRunner.execute(session, "CREATE TABLE call_center AS " +
                            "SELECT cc_call_center_sk, cast(cc_call_center_id as varchar) as cc_call_center_id, cast(cc_rec_start_date as varchar) as cc_rec_start_date, " +
                            "   cast(cc_rec_end_date as varchar) as cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft," +
                            "   cast(cc_hours as varchar) as cc_hours, cc_manager, cc_mkt_id, cast(cc_mkt_class as varchar) as cc_mkt_class, cc_mkt_desc, cc_market_manager, " +
                            "   cc_division, cc_division_name, cc_company, cast(cc_company_name as varchar) as cc_company_name," +
                            "   cast(cc_street_number as varchar ) as cc_street_number, cc_street_name, cast(cc_street_type as varchar) as cc_street_type, " +
                            "   cast(cc_suite_number as varchar) as cc_suite_number, cc_city, cc_county, cast(cc_state as varchar) as cc_state, " +
                            "   cast(cc_zip as varchar) as cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage " +
                            "FROM tpcds.tiny.call_center");
                    break;
            }
        }
    }

    private static void createTpcdsCatalogPage(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_page")) {
            queryRunner.execute(session, "CREATE TABLE catalog_page AS " +
                    "SELECT cp_catalog_page_sk, cast(cp_catalog_page_id as varchar) as cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, " +
                    "   cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type " +
                    "FROM tpcds.tiny.catalog_page");
        }
    }

    private static void createTpcdsCatalogReturns(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_returns")) {
            queryRunner.execute(session, "CREATE TABLE catalog_returns AS SELECT * FROM tpcds.tiny.catalog_returns");
        }
    }

    private static void createTpcdsCatalogSales(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_sales")) {
            queryRunner.execute(session, "CREATE TABLE catalog_sales AS SELECT * FROM tpcds.tiny.catalog_sales");
        }
    }

    private static void createTpcdsCustomer(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer")) {
            queryRunner.execute(session, "CREATE TABLE customer AS " +
                    "SELECT c_customer_sk, cast(c_customer_id as varchar) as c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, " +
                    "   c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, cast(c_salutation as varchar) as c_salutation, " +
                    "   cast(c_first_name as varchar) as c_first_name, cast(c_last_name as varchar) as c_last_name, " +
                    "   cast(c_preferred_cust_flag as varchar) as c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, " +
                    "   c_birth_country, cast(c_login as varchar) as c_login, cast(c_email_address as varchar) as c_email_address,  " +
                    "   c_last_review_date_sk " +
                    "FROM tpcds.tiny.customer");
        }
    }

    private static void createTpcdsCustomerAddress(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer_address")) {
            queryRunner.execute(session, "CREATE TABLE customer_address AS " +
                    "SELECT ca_address_sk, cast(ca_address_id as varchar) as ca_address_id, cast(ca_street_number as varchar) as ca_street_number,  " +
                    "   ca_street_name, cast(ca_street_type as varchar) as ca_street_type, cast(ca_suite_number as varchar) as ca_suite_number,  " +
                    "   ca_city, ca_county, cast(ca_state as varchar) as ca_state, cast(ca_zip as varchar) as ca_zip, " +
                    "   ca_country, ca_gmt_offset, cast(ca_location_type as varchar) as ca_location_type " +
                    "FROM tpcds.tiny.customer_address");
        }
    }

    private static void createTpcdsCustomerDemographics(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer_demographics")) {
            queryRunner.execute(session, "CREATE TABLE customer_demographics AS " +
                    "SELECT cd_demo_sk, cast(cd_gender as varchar) as cd_gender, cast(cd_marital_status as varchar) as cd_marital_status,  " +
                    "   cast(cd_education_status as varchar) as cd_education_status, cd_purchase_estimate,  " +
                    "   cast(cd_credit_rating as varchar) as cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count " +
                    "FROM tpcds.tiny.customer_demographics");
        }
    }

    private static void createTpcdsDateDim(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "date_dim")) {
            switch (storageFormat) {
                case "PARQUET":
                    queryRunner.execute(session, "CREATE TABLE date_dim AS " +
                            "SELECT d_date_sk, cast(d_date_id as varchar) as d_date_id, d_date, " +
                            "   d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, " +
                            "   d_fy_quarter_seq, d_fy_week_seq, cast(d_day_name as varchar) as d_day_name, cast(d_quarter_name as varchar) as d_quarter_name, " +
                            "   cast(d_holiday as varchar) as d_holiday,  cast(d_weekend as varchar) as d_weekend, " +
                            "   cast(d_following_holiday as varchar) as d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq,  " +
                            "   cast(d_current_day as varchar) as d_current_day, cast(d_current_week as varchar) as d_current_week, " +
                            "   cast(d_current_month as varchar) as d_current_month,  cast(d_current_quarter as varchar) as d_current_quarter, " +
                            "   cast(d_current_year as varchar) as d_current_year " +
                            "FROM tpcds.tiny.date_dim");
                    break;
                case "DWRF":
                    queryRunner.execute(session, "CREATE TABLE date_dim AS " +
                            "SELECT d_date_sk, cast(d_date_id as varchar) as d_date_id, cast(d_date as varchar) as d_date, " +
                            "   d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, " +
                            "   d_fy_quarter_seq, d_fy_week_seq, cast(d_day_name as varchar) as d_day_name, cast(d_quarter_name as varchar) as d_quarter_name, " +
                            "   cast(d_holiday as varchar) as d_holiday,  cast(d_weekend as varchar) as d_weekend, " +
                            "   cast(d_following_holiday as varchar) as d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq,  " +
                            "   cast(d_current_day as varchar) as d_current_day, cast(d_current_week as varchar) as d_current_week, " +
                            "   cast(d_current_month as varchar) as d_current_month,  cast(d_current_quarter as varchar) as d_current_quarter, " +
                            "   cast(d_current_year as varchar) as d_current_year " +
                            "FROM tpcds.tiny.date_dim");
                    break;
            }
        }
    }

    private static void createTpcdsHouseholdDemographics(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "household_demographics")) {
            queryRunner.execute(session, "CREATE TABLE household_demographics AS " +
                    "SELECT hd_demo_sk, hd_income_band_sk, cast(hd_buy_potential as varchar) as hd_buy_potential, hd_dep_count, hd_vehicle_count " +
                    "FROM tpcds.tiny.household_demographics");
        }
    }

    private static void createTpcdsIncomeBand(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "income_band")) {
            queryRunner.execute(session, "CREATE TABLE income_band AS " +
                    "SELECT * FROM tpcds.tiny.income_band");
        }
    }

    private static void createTpcdsInventory(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "inventory")) {
            queryRunner.execute(session, "CREATE TABLE inventory AS " +
                    "SELECT * FROM tpcds.tiny.inventory");
        }
    }

    private static void createTpcdsItem(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "item")) {
            switch (storageFormat) {
                case "PARQUET":
                    queryRunner.execute(session, "CREATE TABLE item AS " +
                            "SELECT i_item_sk, cast(i_item_id as varchar) as i_item_id, i_rec_start_date, i_rec_end_date, " +
                            "   i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, cast(i_brand as varchar) as i_brand, " +
                            "   i_class_id,  cast(i_class as varchar) as i_class, i_category_id, cast(i_category as varchar) as i_category, i_manufact_id, " +
                            "   cast(i_manufact as varchar) as i_manufact, cast(i_size as varchar) as i_size, cast(i_formulation as varchar) as i_formulation, " +
                            "   cast(i_color as varchar) as i_color, cast(i_units as varchar) as i_units, cast(i_container as varchar) as i_container, i_manager_id, " +
                            "   cast(i_product_name as varchar) as i_product_name " +
                            "FROM tpcds.tiny.item");
                    break;
                case "DRWF":
                    queryRunner.execute(session, "CREATE TABLE item AS " +
                            "SELECT i_item_sk, cast(i_item_id as varchar) as i_item_id, cast(i_rec_start_date as varchar) as i_rec_start_date, " +
                            "   cast(i_rec_end_date as varchar) as i_rec_end_date, i_item_desc, cast(i_current_price as double) as i_current_price, " +
                            "   cast(i_wholesale_cost as double) as i_wholesale_cost, i_brand_id, cast(i_brand as varchar) as i_brand, " +
                            "   i_class_id,  cast(i_class as varchar) as i_class, i_category_id, cast(i_category as varchar) as i_category, i_manufact_id, " +
                            "   cast(i_manufact as varchar) as i_manufact, cast(i_size as varchar) as i_size, cast(i_formulation as varchar) as i_formulation, " +
                            "   cast(i_color as varchar) as i_color, cast(i_units as varchar) as i_units, cast(i_container as varchar) as i_container, i_manager_id, " +
                            "   cast(i_product_name as varchar) as i_product_name " +
                            "FROM tpcds.tiny.item");
                    break;
            }
        }
    }

    private static void createTpcdsPromotion(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "promotion")) {
            queryRunner.execute(session, "CREATE TABLE promotion AS " +
                    "SELECT p_promo_sk, cast(p_promo_id as varchar) as p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, " +
                    "   p_cost, p_response_targe, cast(p_promo_name as varchar) as p_promo_name, " +
                    "   cast(p_channel_dmail as varchar) as p_channel_dmail, cast(p_channel_email as varchar) as p_channel_email, " +
                    "   cast(p_channel_catalog as varchar) as p_channel_catalog, cast(p_channel_tv as varchar) as p_channel_tv, " +
                    "   cast(p_channel_radio as varchar) as p_channel_radio, cast(p_channel_press as varchar) as p_channel_press, " +
                    "   cast(p_channel_event as varchar) as p_channel_event, cast(p_channel_demo as varchar) as p_channel_demo, p_channel_details, " +
                    "   cast(p_purpose as varchar) as p_purpose, cast(p_discount_active as varchar) as p_discount_active " +
                    "FROM tpcds.tiny.promotion");
        }
    }

    private static void createTpcdsReason(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "reason")) {
            queryRunner.execute(session, "CREATE TABLE reason AS " +
                    "SELECT r_reason_sk, cast(r_reason_id as varchar) as r_reason_id, cast(r_reason_desc as varchar) as r_reason_desc " +
                    "FROM tpcds.tiny.reason");
        }
    }

    private static void createTpcdsShipMode(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "ship_mode")) {
            queryRunner.execute(session, "CREATE TABLE ship_mode AS " +
                    "SELECT sm_ship_mode_sk, cast(sm_ship_mode_id as varchar) as sm_ship_mode_id, cast(sm_type as varchar) as sm_type, " +
                    "   cast(sm_code as varchar) as sm_code, cast(sm_carrier as varchar) as sm_carrier, cast(sm_contract as varchar) as sm_contract " +
                    "FROM tpcds.tiny.ship_mode");
        }
    }

    private static void createTpcdsStore(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "store")) {
            switch (storageFormat) {
                case "PARQUET":
                    queryRunner.execute(session, "CREATE TABLE store AS " +
                            "SELECT s_store_sk, cast(s_store_id as varchar) as s_store_id, s_rec_start_date, s_rec_end_date, " +
                            "   s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, cast(s_hours as varchar) as s_hours, " +
                            "   s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, " +
                            "   s_company_id, s_company_name, s_street_number, s_street_name, cast(s_street_type as varchar) as s_street_type, " +
                            "   cast(s_suite_number as varchar) as s_suite_number, s_city, s_county, cast(s_state as varchar ) as s_state, " +
                            "   cast(s_zip as varchar) as s_zip, s_country, s_gmt_offset, s_tax_precentage " +
                            "FROM tpcds.tiny.store");
                    break;
                case "DRWF":
                    queryRunner.execute(session, "CREATE TABLE store AS " +
                            "SELECT s_store_sk, cast(s_store_id as varchar) as s_store_id, cast(s_rec_start_date as varchar) as s_rec_start_date, " +
                            "   cast(s_rec_end_date as varchar) as s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, " +
                            "   cast(s_hours as varchar) as s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, " +
                            "   s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, " +
                            "   cast(s_street_type as varchar) as s_street_type, cast(s_suite_number as varchar) as s_suite_number, s_city, s_county, " +
                            "   cast(s_state as varchar ) as s_state, cast(s_zip as varchar) as s_zip, s_country, " +
                            "   cast(s_gmt_offset as double) as s_gmt_offset, " +
                            "   cast(s_tax_precentage as double) as s_tax_precentage " +
                            "FROM tpcds.tiny.store");
                    break;
            }
        }
    }

    private static void createTpcdsStoreReturns(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "store_returns")) {
            queryRunner.execute(session, "CREATE TABLE store_returns AS " +
                    "SELECT * FROM tpcds.tiny.store_returns");
        }
    }

    private static void createTpcdsStoreSales(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "store_sales")) {
            queryRunner.execute(session, "CREATE TABLE store_sales AS " +
                    "SELECT * FROM tpcds.tiny.store_sales");
        }
    }

    private static void createTpcdsTimeDim(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "time_dim")) {
            queryRunner.execute(session, "CREATE TABLE time_dim AS " +
                    "SELECT t_time_sk, cast(t_time_id as varchar) as t_time_id, t_time, t_hour, t_minute, t_second,  " +
                    "   cast(t_am_pm as varchar) as t_am_pm, cast(t_shift as varchar) as t_shift, " +
                    "   cast(t_sub_shift as varchar) as t_sub_shift, cast(t_meal_time as varchar) as t_meal_time " +
                    "FROM tpcds.tiny.time_dim");
        }
    }

    private static void createTpcdsWarehouse(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "warehouse")) {
            queryRunner.execute(session, "CREATE TABLE warehouse AS " +
                    "SELECT w_warehouse_sk, cast(w_warehouse_id as varchar) as w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, " +
                    "   cast(w_street_number as varchar) as w_street_number, w_street_name, cast(w_street_type as varchar) as w_street_type, " +
                    "   cast(w_suite_number as varchar) as w_suite_number, w_city, w_county, cast(w_state as varchar) as w_state," +
                    "   cast(w_zip as varchar) as w_zip, w_country, w_gmt_offset " +
                    "FROM tpcds.tiny.warehouse");
        }
    }

    private static void createTpcdsWebPage(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "web_page")) {
            switch (storageFormat) {
                case "PARQUET":
                    queryRunner.execute(session, "CREATE TABLE web_page AS " +
                            "SELECT wp_web_page_sk, cast(wp_web_page_id as varchar) as wp_web_page_id, wp_rec_start_date, wp_rec_end_date, " +
                            "   wp_creation_date_sk, wp_access_date_sk, cast(wp_autogen_flag as varchar) as wp_autogen_flag, wp_customer_sk, " +
                            "   wp_url, cast(wp_type as varchar) as wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count " +
                            "FROM tpcds.tiny.web_page");
                    break;
                case "DWRF":
                    queryRunner.execute(session, "CREATE TABLE web_page AS " +
                            "SELECT wp_web_page_sk, cast(wp_web_page_id as varchar) as wp_web_page_id, cast(wp_rec_start_date as varchar) as wp_rec_start_date, " +
                            "   cast(wp_rec_end_date as varchar) as wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, " +
                            "   cast(wp_autogen_flag as varchar) as wp_autogen_flag, wp_customer_sk, wp_url, cast(wp_type as varchar) as wp_type, " +
                            "   wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count " +
                            "FROM tpcds.tiny.web_page");
                    break;
            }
        }
    }

    private static void createTpcdsWebReturns(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "web_returns")) {
            queryRunner.execute(session, "CREATE TABLE web_returns AS " +
                    "SELECT * FROM tpcds.tiny.web_returns");
        }
    }

    private static void createTpcdsWebSales(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "web_sales")) {
            queryRunner.execute(session, "CREATE TABLE web_sales AS " +
                    "SELECT * FROM tpcds.tiny.web_sales");
        }
    }

    private static void createTpcdsWebSite(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "web_site")) {
            switch (storageFormat) {
                case "PARQUET":
                    queryRunner.execute(session, "CREATE TABLE web_site AS " +
                            "SELECT web_site_sk, cast(web_site_id as varchar) as web_site_id, web_rec_start_date, web_rec_end_date, web_name, " +
                            "   web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, " +
                            "   web_company_id, cast(web_company_name as varchar) as web_company_name, cast(web_street_number as varchar) as web_street_number, " +
                            "   web_street_name, cast(web_street_type as varchar) as web_street_type, " +
                            "   cast(web_suite_number as varchar) as web_suite_number, web_city, web_county, cast(web_state as varchar) as web_state, " +
                            "   cast(web_zip as varchar) as web_zip, web_country, web_gmt_offset, web_tax_percentage " +
                            "FROM tpcds.tiny.web_site");
                    break;
                case "DWRF":
                    queryRunner.execute(session, "CREATE TABLE web_site AS " +
                            "SELECT web_site_sk, cast(web_site_id as varchar) as web_site_id, cast(web_rec_start_date as varchar) as web_rec_start_date, " +
                            "   cast(web_rec_end_date as varchar) as web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, " +
                            "   web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, cast(web_company_name as varchar) as web_company_name, " +
                            "   cast(web_street_number as varchar) as web_street_number, web_street_name, cast(web_street_type as varchar) as web_street_type, " +
                            "   cast(web_suite_number as varchar) as web_suite_number, web_city, web_county, cast(web_state as varchar) as web_state, " +
                            "   cast(web_zip as varchar) as web_zip, web_country, cast(web_gmt_offset as double) as web_gmt_offset, " +
                            "   cast(web_tax_percentage as double) as web_tax_percentage " +
                            "FROM tpcds.tiny.web_site");
                    break;
            }
        }
    }

    protected static String getTpcdsQuery(String q)
            throws IOException
    {
        String sql = Resources.toString(Resources.getResource("tpcds/queries/q" + q + ".sql"), UTF_8);
        sql = sql.replaceAll("\\$\\{database\\}\\.\\$\\{schema\\}\\.", "");
        return sql;
    }

    @Test
    public void testTpcdsQ1()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("01"));
    }

    // TODO(spershin): Enable test when fixed.
    // GitHub issue: https://github.com/facebookincubator/velox/issues/5412
    @Test (enabled = false)
    public void testTpcdsQ2()
            throws Exception
    {
        assertQueryFails(session, getTpcdsQuery("02"), "[\\s\\S]*Scalar function presto\\.default\\.round not registered with arguments[\\s\\S]*");
    }

    @Test
    public void testTpcdsQ3()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("03"));
    }

    @Test
    public void testTpcdsQ4()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("04"));
    }

    @Test
    public void testTpcdsQ5()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("05"));
    }

    @Test
    public void testTpcdsQ6()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("06"));
    }

    @Test
    public void testTpcdsQ7()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("07"));
    }

    @Test
    public void testTpcdsQ8()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("08"));
    }

    @Test
    public void testTpcdsQ9()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("09"));
    }

    @Test
    public void testTpcdsQ10()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("10"));
    }

    @Test
    public void testTpcdsQ11()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("11"));
    }

    @Test
    public void testTpcdsQ12()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("12"));
    }

    @Test
    public void testTpcdsQ13()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("13"));
    }

    @Test
    public void testTpcdsQ14_1()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("14_1"));
    }

    @Test
    public void testTpcdsQ14_2()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("14_2"));
    }

    @Test
    public void testTpcdsQ15()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("15"));
    }

    @Test
    public void testTpcdsQ16()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("16"));
    }

    @Test
    public void testTpcdsQ17()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("17"));
    }

    @Test
    public void testTpcdsQ18()
            throws Exception
    {
        // Results not equal:
        // Actual rows (up to 100 of 0 extra rows shown, 0 rows in total):
        //
        //Expected rows (up to 100 of 1 missing rows shown, 1 rows in total):
        //    [null, null, null, null, null, null, null, null, null, null, null]
        assertQuerySucceeds(session, getTpcdsQuery("18"));
    }

    @Test
    public void testTpcdsQ19()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("19"));
    }

    @Test
    public void testTpcdsQ20()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("20"));
    }

    @Test
    public void testTpcdsQ21()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("21"));
    }

    @Test
    public void testTpcdsQ22()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("22"));
    }

    @Test
    public void testTpcdsQ23_1()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("23_1"));
    }

    @Test
    public void testTpcdsQ23_2()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("23_2"));
    }

    @Test
    public void testTpcdsQ24_1()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("24_1"));
    }

    @Test
    public void testTpcdsQ24_2()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("24_2"));
    }

    @Test
    public void testTpcdsQ25()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("25"));
    }

    @Test
    public void testTpcdsQ26()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("26"));
    }

    @Test
    public void testTpcdsQ27()
            throws Exception
    {
        // Results not equal
        // Actual rows (up to 100 of 0 extra rows shown, 0 rows in total):
        //
        //Expected rows (up to 100 of 1 missing rows shown, 1 rows in total):
        //    [null, null, 1, null, null, null, null]
        assertQuerySucceeds(session, getTpcdsQuery("27"));
    }

    @Test
    public void testTpcdsQ28()
            throws Exception
    {
        // Results not equal
        // Actual rows (up to 100 of 1 extra rows shown, 1 rows in total):
        //    [77.93, 1468, 1468, 69.55, 1518, 1518, 134.06, 1167, 1167, 81.56, 1258, 1258, 60.27, 1523, 1523, 38.99, 1322, 1322]
        //Expected rows (up to 100 of 1 missing rows shown, 1 rows in total):
        //    [77.93, 1468, 1345, 69.55, 1518, 1331, 134.06, 1167, 1107, 81.56, 1258, 1158, 60.27, 1523, 1342, 38.99, 1322, 1152]
        assertQuerySucceeds(session, getTpcdsQuery("28"));
    }

    @Test
    public void testTpcdsQ29()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("29"));
    }

    @Test
    public void testTpcdsQ30()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("30"));
    }

    @Test
    public void testTpcdsQ31()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("31"));
    }

    @Test
    public void testTpcdsQ32()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("32"));
    }

    @Test
    public void testTpcdsQ33()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("33"));
    }

    @Test
    public void testTpcdsQ34()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("34"));
    }

    @Test
    public void testTpcdsQ35()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("35"));
    }

    @Test
    public void testTpcdsQ36()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("36"));
    }

    @Test
    public void testTpcdsQ37()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("37"));
    }

    @Test
    public void testTpcdsQ38()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("38"));
    }

    @Test
    public void testTpcdsQ39_1()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("39_1"));
    }

    @Test
    public void testTpcdsQ39_2()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("39_2"));
    }

    @Test
    public void testTpcdsQ40()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("40"));
    }

    @Test
    public void testTpcdsQ41()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("41"));
    }

    @Test
    public void testTpcdsQ42()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("42"));
    }

    @Test
    public void testTpcdsQ43()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("43"));
    }

    @Test
    public void testTpcdsQ44()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("44"));
    }

    @Test
    public void testTpcdsQ45()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("45"));
    }

    @Test
    public void testTpcdsQ46()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("46"));
    }

    @Test
    public void testTpcdsQ47()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("47"));
    }

    @Test
    public void testTpcdsQ48()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("48"));
    }

    @Test
    public void testTpcdsQ49()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("49"));
    }

    @Test
    public void testTpcdsQ50()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("50"));
    }

    @Test
    public void testTpcdsQ51()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("51"));
    }

    @Test
    public void testTpcdsQ52()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("52"));
    }

    @Test
    public void testTpcdsQ53()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("53"));
    }

    @Test
    public void testTpcdsQ54()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("54"));
    }

    @Test
    public void testTpcdsQ55()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("55"));
    }

    @Test
    public void testTpcdsQ56()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("56"));
    }

    @Test
    public void testTpcdsQ57()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("57"));
    }

    @Test
    public void testTpcdsQ58()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("58"));
    }

    @Test
    public void testTpcdsQ59()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("59"));
    }

    @Test
    public void testTpcdsQ60()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("60"));
    }

    @Test
    public void testTpcdsQ61()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("61"));
    }

    @Test
    public void testTpcdsQ62()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("62"));
    }

    @Test
    public void testTpcdsQ63()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("63"));
    }

    @Test
    public void testTpcdsQ64()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("64"));
    }

    @Test
    public void testTpcdsQ65()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("65"));
    }

    @Test
    public void testTpcdsQ66()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("66"));
    }

    @Test
    public void testTpcdsQ67()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("67"));
    }

    @Test
    public void testTpcdsQ68()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("68"));
    }

    @Test
    public void testTpcdsQ69()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("69"));
    }

    @Test
    public void testTpcdsQ70()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("70"));
    }

    @Test
    public void testTpcdsQ71()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("71"));
    }

    @Test
    public void testTpcdsQ72()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("72"));
    }

    @Test
    public void testTpcdsQ73()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("73"));
    }

    @Test
    public void testTpcdsQ74()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("74"));
    }

    @Test
    public void testTpcdsQ75()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("75"));
    }

    @Test
    public void testTpcdsQ76()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("76"));
    }

    @Test
    public void testTpcdsQ77()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("77"));
    }

    // TODO(spershin): Enable test when fixed.
    // GitHub issue: https://github.com/facebookincubator/velox/issues/5412
    @Test (enabled = false)
    public void testTpcdsQ78()
            throws Exception
    {
        assertQueryFails(session, getTpcdsQuery("78"), "[\\s\\S]*Scalar function presto\\.default\\.round not registered with arguments[\\s\\S]*");
    }

    @Test
    public void testTpcdsQ79()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("79"));
    }

    @Test
    public void testTpcdsQ80()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("80"));
    }

    @Test
    public void testTpcdsQ81()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("81"));
    }

    @Test
    public void testTpcdsQ82()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("82"));
    }

    @Test
    public void testTpcdsQ83()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("83"));
    }

    @Test
    public void testTpcdsQ84()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("84"));
    }

    @Test
    public void testTpcdsQ85()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("85"));
    }

    @Test
    public void testTpcdsQ86()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("86"));
    }

    @Test
    public void testTpcdsQ87()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("87"));
    }

    @Test
    public void testTpcdsQ88()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("88"));
    }

    @Test
    public void testTpcdsQ89()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("89"));
    }

    @Test
    public void testTpcdsQ90()
            throws Exception
    {
        assertQueryFails(session, getTpcdsQuery("90"), "[\\s\\S]*Division by zero[\\s\\S]*");
    }

    @Test
    public void testTpcdsQ91()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("91"));
    }

    @Test
    public void testTpcdsQ92()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("92"));
    }

    @Test
    public void testTpcdsQ93()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("93"));
    }

    @Test
    public void testTpcdsQ94()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("94"));
    }

    @Test
    public void testTpcdsQ95()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("95"));
    }

    @Test
    public void testTpcdsQ96()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("96"));
    }

    @Test
    public void testTpcdsQ97()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("97"));
    }

    @Test
    public void testTpcdsQ98()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("98"));
    }

    @Test
    public void testTpcdsQ99()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("99"));
    }
}
