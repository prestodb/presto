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
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

@Test(groups = "parquet")
public class TestPrestoNativeIcebergTpcdsQueriesParquetUsingThrift
        extends AbstractTestNativeTpcdsQueries
{
    Map<String, Long> deletedRowsMap = new HashMap<>();
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeIcebergQueryRunner(true, "PARQUET");
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        this.storageFormat = "PARQUET";
        return PrestoNativeQueryRunnerUtils.createJavaIcebergQueryRunner("PARQUET");
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

    private void doDeletes()
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

    private void verifyDeletes()
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

    @Test
    public void doDeletesAndQuery() throws Exception
    {
        doDeletes();
        verifyDeletes();
        runAllQueries();
    }
}
