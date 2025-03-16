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
        assertQuery(session, "SELECT * FROM catalog_returns");
        assertQuery(session, "SELECT * FROM catalog_sales");
        assertQuery(session, "SELECT * FROM store_sales");
        assertQuery(session, "SELECT * FROM store_returns");
        assertQuery(session, "SELECT * FROM web_sales");
        assertQuery(session, "SELECT * FROM web_returns");
        assertQuery(session, "SELECT * FROM inventory");
        assertQuery(session, "SELECT * FROM item");
        assertQuery(session, "SELECT * FROM customer_address");
        assertQuery(session, "SELECT * FROM customer_demographics");
        assertQuery(session, "SELECT * FROM call_center");
        assertQuery(session, "SELECT * FROM customer");
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
