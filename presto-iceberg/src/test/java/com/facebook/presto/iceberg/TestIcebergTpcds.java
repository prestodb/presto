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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.io.Resources;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_STAGE_COUNT;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests the Iceberg V3 stack against all 99 TPC-DS benchmark queries.
 *
 * <p>This test creates Iceberg Parquet tables from the TPC-DS {@code tiny} schema,
 * then runs all 99 official TPC-DS queries (plus multi-part variants for Q14, Q23, Q24, Q39)
 * against those tables.
 *
 * <p>Queries are loaded from SQL files in {@code src/test/resources/tpcds/queries/}
 * and validated for successful execution against Iceberg tables. Due to CHAR→VARCHAR
 * type conversion required by Iceberg (which does not support CHAR(n) types), exact result
 * comparison with the TPC-DS source connector is not always possible. Instead, we validate
 * that each query executes successfully, ensuring the full Iceberg V3 read path works correctly.
 *
 * <p>The test exercises the full Iceberg read/write path including:
 * <ul>
 *   <li>Table creation (CTAS through IcebergPageSink) for all 24 TPC-DS tables</li>
 *   <li>All standard SQL types used in TPC-DS (integer, decimal, varchar/char, date)</li>
 *   <li>Complex joins (multi-table star schema joins, self-joins)</li>
 *   <li>Aggregations (GROUP BY, HAVING, COUNT, SUM, AVG, ROLLUP)</li>
 *   <li>Window functions (ROW_NUMBER, RANK, SUM OVER)</li>
 *   <li>Subqueries, CTEs, INTERSECT, UNION ALL, EXISTS</li>
 *   <li>Predicate pushdown (date ranges, equality filters, IN lists)</li>
 * </ul>
 */
@Test(singleThreaded = true)
public class TestIcebergTpcds
        extends AbstractTestQueryFramework
{
    private static final String[] TPCDS_TABLES = {
            "call_center",
            "catalog_page",
            "catalog_returns",
            "catalog_sales",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "household_demographics",
            "income_band",
            "inventory",
            "item",
            "promotion",
            "reason",
            "ship_mode",
            "store",
            "store_returns",
            "store_sales",
            "time_dim",
            "warehouse",
            "web_page",
            "web_returns",
            "web_sales",
            "web_site"
    };

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCreateTpchTables(false)
                .setSchemaName("tpcds")
                .build()
                .getQueryRunner();
    }

    @BeforeClass
    public void createTpcdsTables()
    {
        for (String table : TPCDS_TABLES) {
            MaterializedResult columns = getQueryRunner().execute(
                    tpcdsSession(),
                    "DESCRIBE tpcds.tiny." + table);

            StringBuilder selectColumns = new StringBuilder();
            for (int i = 0; i < columns.getRowCount(); i++) {
                if (i > 0) {
                    selectColumns.append(", ");
                }
                String colName = (String) columns.getMaterializedRows().get(i).getField(0);
                String colType = (String) columns.getMaterializedRows().get(i).getField(1);
                if (colType.startsWith("char")) {
                    selectColumns.append("CAST(TRIM(\"").append(colName).append("\") AS VARCHAR) AS \"").append(colName).append("\"");
                }
                else {
                    selectColumns.append("\"").append(colName).append("\"");
                }
            }

            getQueryRunner().execute(
                    tpcdsSession(),
                    "CREATE TABLE IF NOT EXISTS " + table + " AS SELECT " + selectColumns + " FROM tpcds.tiny." + table);
        }
    }

    @AfterClass(alwaysRun = true)
    public void dropTpcdsTables()
    {
        for (String table : TPCDS_TABLES) {
            getQueryRunner().execute(tpcdsSession(), "DROP TABLE IF EXISTS " + table);
        }
    }

    private Session tpcdsSession()
    {
        return testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("tpcds")
                .setSystemProperty(QUERY_MAX_STAGE_COUNT, "200")
                .build();
    }

    private static String getTpcdsQuery(String q)
            throws IOException
    {
        String sql = Resources.toString(Resources.getResource("tpcds/queries/q" + q + ".sql"), UTF_8);
        sql = sql.replaceAll("\\$\\{database\\}\\.\\$\\{schema\\}\\.", "");
        return sql;
    }

    // ---- Table creation validation ----

    @Test
    public void testAllTablesCreated()
    {
        for (String table : TPCDS_TABLES) {
            MaterializedResult result = computeActual(tpcdsSession(), "SELECT count(*) FROM " + table);
            long count = (long) result.getOnlyValue();
            assertTrue(count >= 0, table + " should be readable");
        }
    }

    @Test
    public void testRowCountsMatchSource()
    {
        for (String table : TPCDS_TABLES) {
            MaterializedResult icebergResult = computeActual(tpcdsSession(), "SELECT count(*) FROM " + table);
            MaterializedResult tpcdsResult = computeActual("SELECT count(*) FROM tpcds.tiny." + table);
            assertEquals(icebergResult.getOnlyValue(), tpcdsResult.getOnlyValue(),
                    "Row count mismatch for " + table);
        }
    }

    // ---- All 99 TPC-DS Queries ----

    @Test
    public void testTpcdsQ01()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("01"));
    }

    @Test
    public void testTpcdsQ02()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("02"));
    }

    @Test
    public void testTpcdsQ03()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("03"));
    }

    @Test
    public void testTpcdsQ04()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("04"));
    }

    @Test
    public void testTpcdsQ05()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("05"));
    }

    @Test
    public void testTpcdsQ06()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("06"));
    }

    @Test
    public void testTpcdsQ07()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("07"));
    }

    @Test
    public void testTpcdsQ08()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("08"));
    }

    @Test
    public void testTpcdsQ09()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("09"));
    }

    @Test
    public void testTpcdsQ10()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("10"));
    }

    @Test
    public void testTpcdsQ11()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("11"));
    }

    @Test
    public void testTpcdsQ12()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("12"));
    }

    @Test
    public void testTpcdsQ13()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("13"));
    }

    @Test
    public void testTpcdsQ14_1()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("14_1"));
    }

    @Test
    public void testTpcdsQ14_2()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("14_2"));
    }

    @Test
    public void testTpcdsQ15()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("15"));
    }

    @Test
    public void testTpcdsQ16()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("16"));
    }

    @Test
    public void testTpcdsQ17()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("17"));
    }

    @Test
    public void testTpcdsQ18()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("18"));
    }

    @Test
    public void testTpcdsQ19()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("19"));
    }

    @Test
    public void testTpcdsQ20()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("20"));
    }

    @Test
    public void testTpcdsQ21()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("21"));
    }

    @Test
    public void testTpcdsQ22()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("22"));
    }

    @Test
    public void testTpcdsQ23_1()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("23_1"));
    }

    @Test
    public void testTpcdsQ23_2()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("23_2"));
    }

    @Test
    public void testTpcdsQ24_1()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("24_1"));
    }

    @Test
    public void testTpcdsQ24_2()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("24_2"));
    }

    @Test
    public void testTpcdsQ25()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("25"));
    }

    @Test
    public void testTpcdsQ26()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("26"));
    }

    @Test
    public void testTpcdsQ27()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("27"));
    }

    @Test
    public void testTpcdsQ28()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("28"));
    }

    @Test
    public void testTpcdsQ29()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("29"));
    }

    @Test
    public void testTpcdsQ30()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("30"));
    }

    @Test
    public void testTpcdsQ31()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("31"));
    }

    @Test
    public void testTpcdsQ32()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("32"));
    }

    @Test
    public void testTpcdsQ33()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("33"));
    }

    @Test
    public void testTpcdsQ34()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("34"));
    }

    @Test
    public void testTpcdsQ35()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("35"));
    }

    @Test
    public void testTpcdsQ36()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("36"));
    }

    @Test
    public void testTpcdsQ37()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("37"));
    }

    @Test
    public void testTpcdsQ38()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("38"));
    }

    @Test
    public void testTpcdsQ39_1()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("39_1"));
    }

    @Test
    public void testTpcdsQ39_2()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("39_2"));
    }

    @Test
    public void testTpcdsQ40()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("40"));
    }

    @Test
    public void testTpcdsQ41()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("41"));
    }

    @Test
    public void testTpcdsQ42()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("42"));
    }

    @Test
    public void testTpcdsQ43()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("43"));
    }

    @Test
    public void testTpcdsQ44()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("44"));
    }

    @Test
    public void testTpcdsQ45()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("45"));
    }

    @Test
    public void testTpcdsQ46()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("46"));
    }

    @Test
    public void testTpcdsQ47()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("47"));
    }

    @Test
    public void testTpcdsQ48()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("48"));
    }

    @Test
    public void testTpcdsQ49()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("49"));
    }

    @Test
    public void testTpcdsQ50()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("50"));
    }

    @Test
    public void testTpcdsQ51()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("51"));
    }

    @Test
    public void testTpcdsQ52()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("52"));
    }

    @Test
    public void testTpcdsQ53()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("53"));
    }

    @Test
    public void testTpcdsQ54()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("54"));
    }

    @Test
    public void testTpcdsQ55()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("55"));
    }

    @Test
    public void testTpcdsQ56()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("56"));
    }

    @Test
    public void testTpcdsQ57()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("57"));
    }

    @Test
    public void testTpcdsQ58()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("58"));
    }

    @Test
    public void testTpcdsQ59()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("59"));
    }

    @Test
    public void testTpcdsQ60()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("60"));
    }

    @Test
    public void testTpcdsQ61()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("61"));
    }

    @Test
    public void testTpcdsQ62()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("62"));
    }

    @Test
    public void testTpcdsQ63()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("63"));
    }

    @Test
    public void testTpcdsQ64()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("64"));
    }

    @Test
    public void testTpcdsQ65()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("65"));
    }

    @Test
    public void testTpcdsQ66()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("66"));
    }

    @Test
    public void testTpcdsQ67()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("67"));
    }

    @Test
    public void testTpcdsQ68()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("68"));
    }

    @Test
    public void testTpcdsQ69()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("69"));
    }

    @Test
    public void testTpcdsQ70()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("70"));
    }

    @Test
    public void testTpcdsQ71()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("71"));
    }

    @Test
    public void testTpcdsQ72()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("72"));
    }

    @Test
    public void testTpcdsQ73()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("73"));
    }

    @Test
    public void testTpcdsQ74()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("74"));
    }

    @Test
    public void testTpcdsQ75()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("75"));
    }

    @Test
    public void testTpcdsQ76()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("76"));
    }

    @Test
    public void testTpcdsQ77()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("77"));
    }

    @Test
    public void testTpcdsQ78()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("78"));
    }

    @Test
    public void testTpcdsQ79()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("79"));
    }

    @Test
    public void testTpcdsQ80()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("80"));
    }

    @Test
    public void testTpcdsQ81()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("81"));
    }

    @Test
    public void testTpcdsQ82()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("82"));
    }

    @Test
    public void testTpcdsQ83()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("83"));
    }

    @Test
    public void testTpcdsQ84()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("84"));
    }

    @Test
    public void testTpcdsQ85()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("85"));
    }

    @Test
    public void testTpcdsQ86()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("86"));
    }

    @Test
    public void testTpcdsQ87()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("87"));
    }

    @Test
    public void testTpcdsQ88()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("88"));
    }

    @Test
    public void testTpcdsQ89()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("89"));
    }

    @Test
    public void testTpcdsQ90()
            throws Exception
    {
        // Q90 causes division by zero on tpcds.tiny dataset
        assertQueryFails(tpcdsSession(), getTpcdsQuery("90"), "[\\s\\S]*Division by zero[\\s\\S]*");
    }

    @Test
    public void testTpcdsQ91()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("91"));
    }

    @Test
    public void testTpcdsQ92()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("92"));
    }

    @Test
    public void testTpcdsQ93()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("93"));
    }

    @Test
    public void testTpcdsQ94()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("94"));
    }

    @Test
    public void testTpcdsQ95()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("95"));
    }

    @Test
    public void testTpcdsQ96()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("96"));
    }

    @Test
    public void testTpcdsQ97()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("97"));
    }

    @Test
    public void testTpcdsQ98()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("98"));
    }

    @Test
    public void testTpcdsQ99()
            throws Exception
    {
        assertQuerySucceeds(tpcdsSession(), getTpcdsQuery("99"));
    }
}
