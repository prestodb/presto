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
package com.facebook.presto.sql.fingerprint;

import com.facebook.presto.spi.QueryFingerprint;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class FingerprintCreatorTest
{
    @Test
    public void testFilteringConditions()
    {
        assertQueriesHaveSameFingerprint(
                "select * from test_table where city_id = '10'",
                "select * from test_table where city_id = '30'");

        String hash1 = assertQueriesHaveSameFingerprint(
                "select * from test_table where city_id IN ('10')",
                "select * from test_table where city_id IN ('30')");

        // Number of args in IN clause shouldn't matter for fingerprint.
        String hash2 = assertQueriesHaveSameFingerprint(
                "select * from test_table where demand_uuid IN ('10', '123')",
                "select * from test_table where demand_uuid IN ('10')");

        // column names are different in the filter conditions above.
        assertNotEquals(hash1, hash2);

        // Ensure hash is consistent by comparing it to its known value.
        assertEquals("7b74eb056b270900", hash1);
    }

    @Test
    public void testSubQueries()
    {
        assertQueriesHaveSameFingerprint(
                "select * from test_table where demand_uuid IN (select demand_uuid from test_table_2 where col_name = 'foo' AND workflow_uuid IN ('jane doe'))",
                "select * from test_table where demand_uuid IN (select demand_uuid from test_table_2 where col_name = 'bar' AND workflow_uuid IN ('john doe'))");

        assertQueriesHaveSameFingerprint(
                "with tmp as (select demand_uuid from my_table where workflow_uuid in ('foo') and col_name = 'foo') select * from my_table limit 10",
                "with tmp as (select demand_uuid from my_table where workflow_uuid in ('bar') and col_name = 'bar') select * from my_table limit 11");

        assertQueriesHaveSameFingerprint(
                "with tmp as (select demand_uuid from my_table where workflow_uuid in ('foo') and col_name = 'foo' limit 10) select * from my_table limit 10",
                "with tmp as (select demand_uuid from my_table where workflow_uuid in ('bar') and col_name = 'bar' limit 11) select * from my_table limit 11");
    }

    @Test
    public void testAggregation()
    {
        assertQueriesHaveSameFingerprint(
                "select col1, col2, count(*) from test_table group by col1, col2 limit 10",
                "select col1, col2, count(*) from test_table group by col1, col2 limit 100");

        assertQueriesHaveSameFingerprint(
                "select col1, col2, sum(col3) / count(*) from test_table group by col1, col2 limit 10",
                "select col1, col2, sum(col3) / count(*) from test_table group by col1, col2 limit 100");
    }

    @Test
    public void testDateTimeFunctions()
            throws InterruptedException
    {
        String intervalQueryTemplate = "select count(*) from my_table where seconds_since_epoch >= cast(to_unixtime(now() - interval '%d' hour) as bigint)";
        QueryFingerprint fp1 = getQueryFingerprint(String.format(intervalQueryTemplate, 1));
        // Sleep for a second.. this test ensures that fingerprinting doesn't explode date-time functions like now().
        Thread.sleep(1 * 1000);
        String hash1 = assertQueriesHaveSameFingerprint(
                String.format(intervalQueryTemplate, 1),
                String.format(intervalQueryTemplate, 3));
        assertEquals(fp1.getQueryHash(), hash1);
    }

    @Test
    public void testMiscellaneousCases()
    {
        // One query has column aliasing, the other doesn't. This should give different fingerprints.
        assertQueriesHaveDifferentFingerprint(
                "select city_id from test_table",
                "select other_col AS city_id from test_table");

        // The two different forms of column aliasing give the same fingerprint.
        assertQueriesHaveSameFingerprint(
                "select other_col city_id from test_table",
                "select other_col AS city_id from test_table");

        // Ensure case of words in the query yield same fingerprint.
        assertQueriesHaveSameFingerprint(
                "select * from test_table limit 10",
                "select * from test_table lIMIT 10");

        assertQueriesHaveSameFingerprint(
                "SELECT * from test_table limit 10",
                "select * FROM test_table lIMIT 10");

        // We will give different fingerprints if we have different case in column names or table-names.
        // Case-1: Different case in column name.
        assertQueriesHaveDifferentFingerprint(
                "select other_Col from test_table limit 10",
                "select other_col from test_table limit 10");

        // Case-2: Different case in table name.
        assertQueriesHaveDifferentFingerprint(
                "select other_col from Test_table limit 10",
                "select other_col from test_table limit 10");

        // Ensure whitespaces are ignored and yield same fingerprint.
        assertQueriesHaveSameFingerprint(
                "select * from    test_table limit 10",
                "select * from test_table limit 10");

        assertQueriesHaveSameFingerprint(
                "select * from    test_table limit 10",
                "select * from test_table \n" +
                        "\tlimit 10");
    }

    @Test
    public void testSampledProductionQueries()
    {
        String q1 = "SELECT\n" +
                "\torderUUID,\n" +
                "\tstoreUUID,\n" +
                "\tnumberOfItems,\n" +
                "\tsubtotal,\n" +
                "\tcreatedOrderTimestamp,\n" +
                "\tlatitude,\n" +
                "\tlongitude\n" +
                "\tFROM\n" +
                "\trta.rta.order_eats_near_you_created\n" +
                "\tWHERE\n" +
                "\t createdordertimestamp > CAST(to_unixtime(now() - INTERVAL '5' MINUTE) AS BIGINT)\n" +
                "\tAND numberofitems > 0\n" +
                "\tAND ST_Distance(\n" +
                "\tto_spherical_geography(location_st_point),\n" +
                "\tto_spherical_geography(ST_Point(-0.2167281, 51.5283726))\n" +
                "\t) < 16000\n" +
                "\tORDER BY createdOrderTimestamp DESC\n" +
                "\tLIMIT 10";
        // Same query as above but for different interval length and geo-point.
        String q2 = "SELECT\n" +
                "\torderUUID,\n" +
                "\tstoreUUID,\n" +
                "\tnumberOfItems,\n" +
                "\tsubtotal,\n" +
                "\tcreatedOrderTimestamp,\n" +
                "\tlatitude,\n" +
                "\tlongitude\n" +
                "\tFROM\n" +
                "\trta.rta.order_eats_near_you_created\n" +
                "\tWHERE\n" +
                "\t createdordertimestamp > CAST(to_unixtime(now() - INTERVAL '2' MINUTE) AS BIGINT)\n" +
                "\tAND numberofitems > 0\n" +
                "\tAND ST_Distance(\n" +
                "\tto_spherical_geography(location_st_point),\n" +
                "\tto_spherical_geography(ST_Point(-10, 20))\n" +
                "\t) < 16000\n" +
                "\tORDER BY createdOrderTimestamp DESC\n" +
                "\tLIMIT 10";
        assertQueriesHaveSameFingerprint(q1, q2);
    }

    /*
     * Takes in two queries and asserts that they have equal query fingerprints.
     * Returns the common hash of the queries.
     */
    private String assertQueriesHaveSameFingerprint(String q1, String q2)
    {
        QueryFingerprint fp1 = getQueryFingerprint(q1);
        QueryFingerprint fp2 = getQueryFingerprint(q2);
        assertEquals(fp1.getFingerprint(), fp2.getFingerprint());
        assertEquals(fp1.getQueryHash(), fp2.getQueryHash());
        return fp1.getQueryHash();
    }

    /*
     * Takes in two queries and asserts that they have equal query fingerprints.
     * Returns the common hash of the queries.
     */
    private String assertQueriesHaveDifferentFingerprint(String q1, String q2)
    {
        QueryFingerprint fp1 = getQueryFingerprint(q1);
        QueryFingerprint fp2 = getQueryFingerprint(q2);
        assertNotEquals(fp1.getFingerprint(), fp2.getFingerprint());
        assertNotEquals(fp1.getQueryHash(), fp2.getQueryHash());
        return fp1.getQueryHash();
    }

    private QueryFingerprint getQueryFingerprint(String query)
    {
        SqlParser sqlParser = new SqlParser();
        Statement statement = sqlParser.createStatement(query, ParsingOptions.builder()
                .setDecimalLiteralTreatment(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE)
                .build());
        return FingerprintCreator.createFingerPrint(statement);
    }
}
