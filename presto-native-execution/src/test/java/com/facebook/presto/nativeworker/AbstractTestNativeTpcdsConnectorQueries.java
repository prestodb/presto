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
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

public abstract class AbstractTestNativeTpcdsConnectorQueries
        extends AbstractTestQueryFramework
{
    @Override
    public Session getSession()
    {
        return Session.builder(super.getSession()).setCatalog("tpcds").setSchema("tiny").build();
    }

    @Test
    public void testTpcdsTinyTablesRowCount()
    {
        Session session = getSession();
        assertQuery(session, "SELECT cr_returned_date_sk, cr_returned_time_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk," +
                "cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss FROM catalog_returns");
        assertQuery(session, "SELECT cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, " +
                "cs_warehouse_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, " +
                "cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit FROM catalog_sales");
        assertQuery(session, "SELECT ss_sold_date_sk, ss_sold_time_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, " +
                "ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit FROM store_sales");
        assertQuery(session, "SELECT sr_returned_date_sk, sr_return_time_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, " +
                "sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, " +
                "sr_store_credit, sr_net_loss FROM store_returns");
        assertQuery(session, "SELECT ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk," +
                "ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk," +
                "ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price," +
                "ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax," +
                "ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax," +
                "ws_net_profit FROM web_sales");
        assertQuery(session, "SELECT wr_returned_date_sk, wr_returned_time_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, " +
                "wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax," +
                "wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss FROM web_returns");
        assertQuery(session, "SELECT * FROM inventory");
        assertQuery(session, "SELECT * FROM item");
        assertQuery(session, "SELECT ca_address_sk, ca_address_id, ca_street_number, " +
                "ca_street_name, ca_street_type, ca_suite_number," +
                "ca_city, ca_county, ca_state, ca_country, ca_gmt_offset, ca_location_type FROM customer_address");
        assertQuery(session, "SELECT * FROM customer_demographics");
        assertQuery(session, "SELECT * FROM call_center");

        /// TODO: Validate c_email_address once https://github.com/facebookincubator/velox/issues/14966 is fixed.
        assertQuery(session, "SELECT c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, " +
                "c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, " +
                "c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, " +
                "c_login, c_last_review_date_sk FROM customer");
        assertQuery(session, "SELECT * FROM web_site");
        assertQuery(session, "SELECT * FROM web_page");
        assertQuery(session, "SELECT * FROM promotion");
        assertQuery(session, "SELECT * FROM reason");
        assertQuery(session, "SELECT * FROM store");
        assertQuery(session, "SELECT * FROM income_band");
        assertQuery(session, "SELECT * FROM household_demographics");
        assertQuery(session, "SELECT * FROM warehouse");
        assertQuery(session, "SELECT * FROM catalog_page");
        assertQuery(session, "SELECT * FROM date_dim");
        assertQuery(session, "SELECT * FROM time_dim");
        assertQuery(session, "SELECT * FROM ship_mode");
    }

    @Test
    public void testTpcdsBasicQueries()
    {
        Session session = getSession();
        assertQuery(session, "SELECT cc_call_center_sk, cc_name, cc_manager, cc_mkt_id, cc_mkt_class FROM call_center");
        assertQuery(session, "SELECT ss_store_sk, SUM(ss_net_paid) AS total_sales " +
                "FROM store_sales  GROUP BY ss_store_sk ORDER BY total_sales DESC LIMIT 10");
        assertQuery(session, "SELECT sr_item_sk, SUM(sr_return_quantity) AS total_returns " +
                "FROM store_returns WHERE sr_item_sk = 12345 GROUP BY sr_item_sk");
        assertQuery(session, "SELECT ws_order_number, SUM(ws_net_paid) AS total_paid FROM web_sales " +
                "WHERE ws_sold_date_sk BETWEEN 2451180 AND 2451545 GROUP BY ws_order_number");
        assertQuery(session, "SELECT inv_item_sk, inv_quantity_on_hand FROM inventory WHERE inv_quantity_on_hand > 1000 " +
                "ORDER BY inv_quantity_on_hand DESC");
        assertQuery(session, "SELECT SUM(ss_net_paid) AS total_revenue FROM store_sales, promotion " +
                "WHERE p_promo_sk = 100 GROUP BY p_promo_sk");
        assertQuery(session, "SELECT c.c_customer_id FROM customer c " +
                "JOIN customer_demographics cd ON  c.c_customer_sk = cd.cd_demo_sk WHERE cd_purchase_estimate > 5000");
        assertQuery(session, "SELECT cd_gender, AVG(cd_purchase_estimate) AS avg_purchase_estimate FROM customer_demographics" +
                " GROUP BY cd_gender ORDER BY avg_purchase_estimate DESC");

        // No row passes the filter.
        assertQuery(session,
                "SELECT s_store_sk, s_store_id, s_number_employees FROM store WHERE s_number_employees > 1000");
    }
}
