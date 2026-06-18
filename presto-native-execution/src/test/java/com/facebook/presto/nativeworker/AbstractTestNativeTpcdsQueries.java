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
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_EXECUTION_TIME;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_RUN_TIME;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestNativeTpcdsQueries
        extends AbstractTestQueryFramework
{
    protected String storageFormat = "DWRF";
    Session session;
    String[] tpcdsTableNames = {"call_center", "catalog_page", "catalog_returns", "catalog_sales",
            "customer", "customer_address", "customer_demographics", "date_dim", "household_demographics",
            "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
            "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
            "web_sales", "web_site"};
    Map<String, Long> deletedRowsMap = new HashMap<>();

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        this.session = Session.builder(queryRunner.getDefaultSession())
                .setSchema("tpcds")
                .setSystemProperty(QUERY_MAX_RUN_TIME, "2m")
                .setSystemProperty(QUERY_MAX_EXECUTION_TIME, "2m")
                .build();
        dropTables();
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

    private void dropTables()
    {
        for (String table : tpcdsTableNames) {
            assertUpdate(session, "DROP TABLE IF EXISTS " + table);
        }
    }

    private static void createTpcdsCallCenter(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "call_center")) {
            switch (storageFormat) {
                case "PARQUET":
                case "ORC":
                case "TEXTFILE":
                    queryRunner.execute(session, "CREATE TABLE call_center AS " +
                            "SELECT * FROM tpcds.tiny.call_center");
                    break;
                case "DWRF":
                    queryRunner.execute(session, "CREATE TABLE call_center AS " +
                            "SELECT cc_call_center_sk, cc_call_center_id, cast(cc_rec_start_date as varchar) as cc_rec_start_date, " +
                            "   cast(cc_rec_end_date as varchar) as cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, " +
                            "   cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, " +
                            "   cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, " +
                            "   cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage " +
                            "FROM tpcds.tiny.call_center");
                    break;
            }
        }
    }

    private static void createTpcdsCatalogPage(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_page")) {
            queryRunner.execute(session, "CREATE TABLE catalog_page AS " +
                    "SELECT * FROM tpcds.tiny.catalog_page");
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
                    "SELECT * FROM tpcds.tiny.customer");
        }
    }

    private static void createTpcdsCustomerAddress(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer_address")) {
            queryRunner.execute(session, "CREATE TABLE customer_address AS " +
                    "SELECT * FROM tpcds.tiny.customer_address");
        }
    }

    private static void createTpcdsCustomerDemographics(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer_demographics")) {
            queryRunner.execute(session, "CREATE TABLE customer_demographics AS " +
                    "SELECT * FROM tpcds.tiny.customer_demographics");
        }
    }

    private static void createTpcdsDateDim(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "date_dim")) {
            switch (storageFormat) {
                case "PARQUET":
                case "ORC":
                case "TEXTFILE":
                    queryRunner.execute(session, "CREATE TABLE date_dim AS " +
                            "SELECT * FROM tpcds.tiny.date_dim");
                    break;
                case "DWRF":
                    queryRunner.execute(session, "CREATE TABLE date_dim AS " +
                            "SELECT d_date_sk, d_date_id, cast(d_date as varchar) as d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, " +
                            "   d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday,  d_weekend, d_following_holiday, d_first_dom, " +
                            "   d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month,  d_current_quarter, d_current_year " +
                            "FROM tpcds.tiny.date_dim");
                    break;
            }
        }
    }

    private static void createTpcdsHouseholdDemographics(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "household_demographics")) {
            queryRunner.execute(session, "CREATE TABLE household_demographics AS " +
                    "SELECT * FROM tpcds.tiny.household_demographics");
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
                case "ORC":
                case "TEXTFILE":
                    queryRunner.execute(session, "CREATE TABLE item AS " +
                            "SELECT * FROM tpcds.tiny.item");
                    break;
                case "DRWF":
                    queryRunner.execute(session, "CREATE TABLE item AS " +
                            "SELECT i_item_sk, i_item_id, cast(i_rec_start_date as varchar) as i_rec_start_date, cast(i_rec_end_date as varchar) as i_rec_end_date, " +
                            "   i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id,  i_class, i_category_id, " +
                            "   i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name " +
                            "FROM tpcds.tiny.item");
                    break;
            }
        }
    }

    private static void createTpcdsPromotion(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "promotion")) {
            queryRunner.execute(session, "CREATE TABLE promotion AS " +
                    "SELECT * FROM tpcds.tiny.promotion");
        }
    }

    private static void createTpcdsReason(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "reason")) {
            queryRunner.execute(session, "CREATE TABLE reason AS " +
                    "SELECT * FROM tpcds.tiny.reason");
        }
    }

    private static void createTpcdsShipMode(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "ship_mode")) {
            queryRunner.execute(session, "CREATE TABLE ship_mode AS " +
                    "SELECT * FROM tpcds.tiny.ship_mode");
        }
    }

    private static void createTpcdsStore(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "store")) {
            switch (storageFormat) {
                case "PARQUET":
                case "ORC":
                case "TEXTFILE":
                    queryRunner.execute(session, "CREATE TABLE store AS " +
                            "SELECT * FROM tpcds.tiny.store");
                    break;
                case "DRWF":
                    queryRunner.execute(session, "CREATE TABLE store AS " +
                            "SELECT s_store_sk, cast(s_rec_start_date as varchar) as s_rec_start_date, cast(s_rec_end_date as varchar) as s_rec_end_date, " +
                            "   s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, " +
                            "   s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, " +
                            "   s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, " +
                            "   cast(s_gmt_offset as double) as s_gmt_offset, cast(s_tax_precentage as double) as s_tax_precentage " +
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
                    "SELECT * FROM tpcds.tiny.time_dim");
        }
    }

    private static void createTpcdsWarehouse(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "warehouse")) {
            queryRunner.execute(session, "CREATE TABLE warehouse AS " +
                    "SELECT * FROM tpcds.tiny.warehouse");
        }
    }

    private static void createTpcdsWebPage(QueryRunner queryRunner, Session session, String storageFormat)
    {
        if (!queryRunner.tableExists(session, "web_page")) {
            switch (storageFormat) {
                case "PARQUET":
                case "ORC":
                case "TEXTFILE":
                    queryRunner.execute(session, "CREATE TABLE web_page AS " +
                            "SELECT * FROM tpcds.tiny.web_page");
                    break;
                case "DWRF":
                    queryRunner.execute(session, "CREATE TABLE web_page AS " +
                            "SELECT wp_web_page_sk, wp_web_page_id, cast(wp_rec_start_date as varchar) as wp_rec_start_date, " +
                            "   cast(wp_rec_end_date as varchar) as wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, " +
                            "   wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, " +
                            "   wp_max_ad_count " +
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
                case "ORC":
                case "TEXTFILE":
                    queryRunner.execute(session, "CREATE TABLE web_site AS " +
                            "SELECT * FROM tpcds.tiny.web_site");
                    break;
                case "DWRF":
                    queryRunner.execute(session, "CREATE TABLE web_site AS " +
                            "SELECT web_site_sk, web_site_id, cast(web_rec_start_date as varchar) as web_rec_start_date, " +
                            "   cast(web_rec_end_date as varchar) as web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, " +
                            "   web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, " +
                            "   web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, as web_zip, web_country, " +
                            "   cast(web_gmt_offset as double) as web_gmt_offset, cast(web_tax_percentage as double) as web_tax_percentage " +
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

    @Test
    public void testTpcdsQ2()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("02"));
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
        assertQueryFails(session, getTpcdsQuery("12"), "[\\s\\S]*Division by zero[\\s\\S]*");
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
        // TODO After https://github.com/facebookincubator/velox/pull/11067 merged,
        // we can enable this test for ORC.
        if (!storageFormat.equals("ORC")) {
            assertQuery(session, getTpcdsQuery("21"));
        }
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
        // TODO After https://github.com/facebookincubator/velox/pull/11067 merged,
        // we can enable this test for ORC.
        if (!storageFormat.equals("ORC")) {
            assertQuery(session, getTpcdsQuery("33"));
        }
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
        // TODO After https://github.com/facebookincubator/velox/pull/11067 merged,
        // we can enable this test for ORC.
        if (!storageFormat.equals("ORC")) {
            assertQuery(session, getTpcdsQuery("37"));
        }
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
        // TODO After https://github.com/facebookincubator/velox/pull/11067 merged,
        // we can enable this test for ORC.
        if (!storageFormat.equals("ORC")) {
            assertQuery(session, getTpcdsQuery("40"));
        }
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
        // TODO After https://github.com/facebookincubator/velox/pull/11067 merged,
        // we can enable this test for ORC.
        if (!storageFormat.equals("ORC")) {
            assertQuery(session, getTpcdsQuery("43"));
        }
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
        // TODO After https://github.com/facebookincubator/velox/pull/11067 merged,
        // we can enable this test for ORC.
        if (!storageFormat.equals("ORC")) {
            assertQuery(session, getTpcdsQuery("49"));
        }
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
        // TODO After https://github.com/facebookincubator/velox/pull/11067 merged,
        // we can enable this test for ORC.
        if (!storageFormat.equals("ORC")) {
            assertQuery(session, getTpcdsQuery("56"));
        }
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
        // TODO After https://github.com/facebookincubator/velox/pull/11067 merged,
        // we can enable this test for ORC.
        if (!storageFormat.equals("ORC")) {
            assertQuery(session, getTpcdsQuery("60"));
        }
    }

    @Test
    public void testTpcdsQ61()
            throws Exception
    {
        // TODO After https://github.com/facebookincubator/velox/pull/11067 merged,
        // we can enable this test for ORC.
        if (!storageFormat.equals("ORC")) {
            assertQuery(session, getTpcdsQuery("61"));
        }
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

    // TODO This test often fails in CI only. Tracked by https://github.com/prestodb/presto/issues/20271
    @Ignore
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

    @Test
    public void testTpcdsQ78()
            throws Exception
    {
        assertQuery(session, getTpcdsQuery("78"));
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
        // TODO After https://github.com/facebookincubator/velox/pull/11067 merged,
        // we can enable this test for ORC.
        if (!storageFormat.equals("ORC")) {
            assertQuery(session, getTpcdsQuery("80"));
        }
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
        // TODO After https://github.com/facebookincubator/velox/pull/11067 merged,
        // we can enable this test for ORC.
        if (!storageFormat.equals("ORC")) {
            assertQuery(session, getTpcdsQuery("82"));
        }
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
        // TODO After https://github.com/facebookincubator/velox/pull/11067 merged,
        // we can enable this test for ORC.
        if (!storageFormat.equals("ORC")) {
            assertQuery(session, getTpcdsQuery("91"));
        }
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

    protected void doDeletes()
    {
        DF_CS();
        DF_SS();
        DF_WS();
        DF_I();
    }

    private void DF_CS()
    {
        assertUpdateExpected(session, "DELETE FROM catalog_returns " +
                        "WHERE  cr_order_number IN (SELECT cs_order_number " +
                        "                           FROM   catalog_sales, " +
                        "                                  date_dim " +
                        "                           WHERE  cs_sold_date_sk = d_date_sk " +
                        "                                  AND d_date BETWEEN date'2000-05-20' AND " +
                        "                                                     date'2000-05-21') ",
                7L);

        assertUpdateExpected(session, "DELETE FROM catalog_sales " +
                        "WHERE  cs_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2000-05-20' AND date'2000-05-21') " +
                        "       AND cs_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2000-05-20' AND " +
                        "                                                     date'2000-05-21') ",
                54L);

        assertUpdateExpected(session, "DELETE FROM catalog_returns " +
                        "WHERE  cr_order_number IN (SELECT cs_order_number " +
                        "                           FROM   catalog_sales, " +
                        "                                  date_dim " +
                        "                           WHERE  cs_sold_date_sk = d_date_sk " +
                        "                                  AND d_date BETWEEN date'1999-09-18' AND " +
                        "                                                     date'1999-09-19') ",
                12L);

        assertUpdateExpected(session, "DELETE FROM catalog_sales " +
                        "WHERE  cs_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'1999-09-18' AND date'1999-09-19') " +
                        "       AND cs_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'1999-09-18' AND " +
                        "                                                     date'1999-09-19') ",
                123L);

        assertUpdateExpected(session, "DELETE FROM catalog_returns " +
                        "WHERE  cr_order_number IN (SELECT cs_order_number " +
                        "                           FROM   catalog_sales, " +
                        "                                  date_dim " +
                        "                           WHERE  cs_sold_date_sk = d_date_sk " +
                        "                                  AND d_date BETWEEN date'2002-11-12' AND " +
                        "                                                     date'2002-11-13') ",
                15L);

        assertUpdateExpected(session, "DELETE FROM catalog_sales " +
                        "WHERE  cs_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2002-11-12' AND date'2002-11-13') " +
                        "       AND cs_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2002-11-12' AND" +
                        "                                                     date'2002-11-13') ",
                197L);
        deletedRowsMap.put("catalog_sales", 374L);
        deletedRowsMap.put("catalog_returns", 34L);
    }

    private void DF_SS()
    {
        assertUpdateExpected(session, "DELETE FROM store_returns " +
                        "WHERE  sr_ticket_number IN (SELECT ss_ticket_number " +
                        "                            FROM   store_sales, " +
                        "                                   date_dim " +
                        "                            WHERE  ss_sold_date_sk = d_date_sk " +
                        "                                   AND d_date BETWEEN date'2000-05-20' AND " +
                        "                                                      date'2000-05-21') ",
                4L);

        assertUpdateExpected(session, "DELETE FROM store_sales " +
                        "WHERE  ss_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2000-05-20' AND date'2000-05-21') " +
                        "       AND ss_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2000-05-20' AND " +
                        "                                                     date'2000-05-21') ",
                64L);

        assertUpdateExpected(session, "DELETE FROM store_returns " +
                        "WHERE  sr_ticket_number IN (SELECT ss_ticket_number " +
                        "                            FROM   store_sales, " +
                        "                                   date_dim " +
                        "                            WHERE  ss_sold_date_sk = d_date_sk " +
                        "                                   AND d_date BETWEEN date'1999-09-18' AND " +
                        "                                                      date'1999-09-19') ",
                13L);

        assertUpdateExpected(session, "DELETE FROM store_sales " +
                        "WHERE  ss_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'1999-09-18' AND date'1999-09-19') " +
                        "       AND ss_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'1999-09-18' AND " +
                        "                                                     date'1999-09-19') ",
                111L);

        assertUpdateExpected(session, "DELETE FROM store_returns " +
                        "WHERE  sr_ticket_number IN (SELECT ss_ticket_number " +
                        "                            FROM   store_sales, " +
                        "                                   date_dim " +
                        "                            WHERE  ss_sold_date_sk = d_date_sk " +
                        "                                   AND d_date BETWEEN date'2002-11-12' AND " +
                        "                                                      date'2002-11-13') ",
                20L);

        assertUpdateExpected(session, "DELETE FROM store_sales " +
                        "WHERE  ss_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2002-11-12' AND date'2002-11-13') " +
                        "       AND ss_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2002-11-12' AND " +
                        "                                                     date'2002-11-13') ",
                185L);
        deletedRowsMap.put("store_sales", 360L);
        deletedRowsMap.put("store_returns", 37L);
    }

    private void DF_WS()
    {
        assertUpdateExpected(session, "DELETE FROM web_returns " +
                        "WHERE  wr_order_number IN (SELECT ws_order_number " +
                        "                           FROM   web_sales, " +
                        "                                  date_dim " +
                        "                           WHERE  ws_sold_date_sk = d_date_sk " +
                        "                                  AND d_date BETWEEN date'2000-05-20' AND " +
                        "                                                     date'2000-05-21') ",
                0L);

        assertUpdateExpected(session, "DELETE FROM web_sales " +
                        "WHERE  ws_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2000-05-20' AND date'2000-05-21') " +
                        "       AND ws_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2000-05-20' AND " +
                        "                                                     date'2000-05-21') ",
                0L);

        assertUpdateExpected(session, "DELETE FROM web_returns " +
                        "WHERE  wr_order_number IN (SELECT ws_order_number " +
                        "                           FROM   web_sales, " +
                        "                                  date_dim " +
                        "                           WHERE  ws_sold_date_sk = d_date_sk " +
                        "                                  AND d_date BETWEEN date'1999-09-18' AND " +
                        "                                                     date'1999-09-19') ",
                0L);

        assertUpdateExpected(session, "DELETE FROM web_sales " +
                        "WHERE  ws_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'1999-09-18' AND date'1999-09-19') " +
                        "       AND ws_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'1999-09-18' AND " +
                        "                                                     date'1999-09-19') ",
                22L);

        assertUpdateExpected(session, "DELETE FROM web_returns " +
                        "WHERE  wr_order_number IN (SELECT ws_order_number " +
                        "                           FROM   web_sales, " +
                        "                                  date_dim " +
                        "                           WHERE  ws_sold_date_sk = d_date_sk " +
                        "                                  AND d_date BETWEEN date'2002-11-12' AND " +
                        "                                                     date'2002-11-13') ",
                4L);

        assertUpdateExpected(session, "DELETE FROM web_sales " +
                        "WHERE  ws_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2002-11-12' AND date'2002-11-13') " +
                        "       AND ws_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2002-11-12' AND " +
                        "                                                     date'2002-11-13') ",
                42L);
        deletedRowsMap.put("web_sales", 64L);
        deletedRowsMap.put("web_returns", 4L);
    }

    private void DF_I()
    {
        assertUpdateExpected(session, "DELETE FROM inventory " +
                "WHERE  inv_date_sk >= (SELECT Min(d_date_sk) " +
                "FROM   date_dim " +
                "WHERE  d_date BETWEEN date'2000-05-18' AND date'2000-05-25') " +
                "AND inv_date_sk <= (SELECT Max(d_date_sk) " +
                "FROM   date_dim " +
                "WHERE  d_date BETWEEN date'2000-05-18' AND date'2000-05-25') ", 2002L);

        assertUpdateExpected(session, "DELETE FROM inventory " +
                "WHERE  inv_date_sk >= (SELECT Min(d_date_sk) " +
                "                       FROM   date_dim " +
                "                       WHERE  d_date BETWEEN date'1999-09-16' AND date'1999-09-23') " +
                "       AND inv_date_sk <= (SELECT Max(d_date_sk) " +
                "                           FROM   date_dim " +
                "                           WHERE  d_date BETWEEN date'1999-09-16' AND date'1999-09-23') ", 2002L);

        assertUpdateExpected(session, "DELETE FROM inventory " +
                "WHERE  inv_date_sk >= (SELECT Min(d_date_sk) " +
                "                       FROM   date_dim " +
                "                       WHERE  d_date BETWEEN date'2002-11-14' AND date'2002-11-21') " +
                "       AND inv_date_sk <= (SELECT Max(d_date_sk) " +
                "                           FROM   date_dim " +
                "                           WHERE  d_date BETWEEN date'2002-11-14' AND date'2002-11-21') ", 2002L);
        deletedRowsMap.put("inventory", 6006L);
    }

    private String getCountQuery(String tableName)
    {
        return "SELECT COUNT(*) FROM " + tableName;
    }

    protected void verifyDeletes()
    {
        Session tpcdsConnSession = Session.builder(session)
                .setCatalog("tpcds")
                .setSchema("tiny")
                .build();
        for (Map.Entry<String, Long> entry : deletedRowsMap.entrySet()) {
            String tableName = entry.getKey();
            Long numDeletedRows = entry.getValue();
            String countQuery = getCountQuery(tableName);
            Long originalRowcount = (long) computeScalarExpected(tpcdsConnSession, countQuery);
            Long postDeleteRowcount = (long) computeScalar(session, countQuery);
            assertEquals(originalRowcount - postDeleteRowcount, numDeletedRows);
        }
    }

    protected void runAllQueries() throws Exception
    {
        testTpcdsQ1();
        testTpcdsQ2();
        testTpcdsQ3();
        testTpcdsQ4();
        testTpcdsQ5();
        testTpcdsQ6();
        testTpcdsQ7();
        testTpcdsQ8();
        testTpcdsQ9();
        testTpcdsQ10();
        testTpcdsQ11();
        testTpcdsQ12();
        testTpcdsQ13();
        testTpcdsQ14_1();
        testTpcdsQ14_2();
        testTpcdsQ15();
        testTpcdsQ16();
        testTpcdsQ17();
        testTpcdsQ18();
        testTpcdsQ19();
        testTpcdsQ20();
        testTpcdsQ21();
        testTpcdsQ22();
        testTpcdsQ23_1();
        testTpcdsQ23_2();
        testTpcdsQ24_1();
        testTpcdsQ24_2();
        testTpcdsQ25();
        testTpcdsQ26();
        testTpcdsQ27();
        testTpcdsQ28();
        testTpcdsQ29();
        testTpcdsQ30();
        testTpcdsQ31();
        testTpcdsQ32();
        testTpcdsQ33();
        testTpcdsQ34();
        testTpcdsQ35();
        testTpcdsQ36();
        testTpcdsQ37();
        testTpcdsQ38();
        testTpcdsQ39_1();
        testTpcdsQ39_2();
        testTpcdsQ40();
        testTpcdsQ41();
        testTpcdsQ42();
        testTpcdsQ43();
        testTpcdsQ44();
        testTpcdsQ45();
        testTpcdsQ46();
        testTpcdsQ47();
        testTpcdsQ48();
        testTpcdsQ49();
        testTpcdsQ50();
        testTpcdsQ51();
        testTpcdsQ52();
        testTpcdsQ53();
        testTpcdsQ54();
        testTpcdsQ55();
        testTpcdsQ56();
        testTpcdsQ57();
        testTpcdsQ58();
        testTpcdsQ59();
        testTpcdsQ60();
        testTpcdsQ61();
        testTpcdsQ62();
        testTpcdsQ63();
        testTpcdsQ65();
        testTpcdsQ66();
        testTpcdsQ67();
        testTpcdsQ68();
        testTpcdsQ69();
        testTpcdsQ70();
        testTpcdsQ71();
        testTpcdsQ72();
        testTpcdsQ73();
        testTpcdsQ74();
        testTpcdsQ75();
        testTpcdsQ76();
        testTpcdsQ77();
        testTpcdsQ78();
        testTpcdsQ79();
        testTpcdsQ80();
        testTpcdsQ81();
        testTpcdsQ82();
        testTpcdsQ83();
        testTpcdsQ84();
        testTpcdsQ85();
        testTpcdsQ86();
        testTpcdsQ87();
        testTpcdsQ88();
        testTpcdsQ89();
        testTpcdsQ90();
        testTpcdsQ91();
        testTpcdsQ92();
        testTpcdsQ93();
        testTpcdsQ94();
        testTpcdsQ95();
        testTpcdsQ96();
        testTpcdsQ97();
        testTpcdsQ98();
        testTpcdsQ99();
    }
}
