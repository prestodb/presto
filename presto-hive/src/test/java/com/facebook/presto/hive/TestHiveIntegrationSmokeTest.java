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
package com.facebook.presto.hive;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;

import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.facebook.presto.hive.HiveQueryRunner.createSampledSession;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public class TestHiveIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    public TestHiveIntegrationSmokeTest()
            throws Exception
    {
        super(createQueryRunner(ORDERS), createSampledSession());
    }

    @Test
    public void testInformationSchemaTablesWithoutEqualityConstraint()
            throws Exception
    {
        @Language("SQL") String actual = "" +
                "SELECT lower(table_name) " +
                "FROM information_schema.tables " +
                "WHERE table_catalog = 'hive' AND table_schema LIKE 'tpch' AND table_name LIKE '%orders'";

        @Language("SQL") String expected = "" +
                "SELECT lower(table_name) " +
                "FROM information_schema.tables " +
                "WHERE table_name LIKE '%ORDERS'";

        assertQuery(actual, expected);
    }

    @Test
    public void testInformationSchemaColumnsWithoutEqualityConstraint()
            throws Exception
    {
        @Language("SQL") String actual = "" +
                "SELECT lower(table_name), lower(column_name) " +
                "FROM information_schema.columns " +
                "WHERE table_catalog = 'hive' AND table_schema = 'tpch' AND table_name LIKE '%orders%'";

        @Language("SQL") String expected = "" +
                "SELECT lower(table_name), lower(column_name) " +
                "FROM information_schema.columns " +
                "WHERE table_name LIKE '%ORDERS%'";

        assertQuery(actual, expected);
    }

    @Test
    public void createTableWithEveryType()
            throws Exception
    {
        String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast('bar' as varbinary) _varbinary" +
                ", 1 _bigint" +
                ", 3.14 _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp";

        assertQuery(query, "SELECT 1");

        MaterializedResult results = queryRunner.execute(getSession(), "SELECT * FROM test_types_table").toJdbcTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 3.14);
        assertEquals(row.getField(4), true);
        assertEquals(row.getField(5), new Date(new DateTime(1980, 5, 7, 0, 0, 0, UTC).getMillis()));
        assertEquals(row.getField(6), new Timestamp(new DateTime(1980, 5, 7, 11, 22, 33, 456, UTC).getMillis()));
        assertQueryTrue("DROP TABLE test_types_table");

        assertFalse(queryRunner.tableExists(getSession(), "test_types_table"));
    }

    // TODO: These should be moved to another class, when more connectors support arrays
    @Test
    public void testArrays()
            throws Exception
    {
        assertQuery("CREATE TABLE tmp_array1 AS SELECT ARRAY[1, 2, NULL] AS col", "SELECT 1");
        assertQuery("SELECT col[2] FROM tmp_array1", "SELECT 2");
        assertQuery("SELECT col[3] FROM tmp_array1", "SELECT NULL");

        assertQuery("CREATE TABLE tmp_array2 AS SELECT ARRAY[1.0, 2.5, 3.5] AS col", "SELECT 1");
        assertQuery("SELECT col[2] FROM tmp_array2", "SELECT 2.5");

        assertQuery("CREATE TABLE tmp_array3 AS SELECT ARRAY['puppies', 'kittens', NULL] AS col", "SELECT 1");
        assertQuery("SELECT col[2] FROM tmp_array3", "SELECT 'kittens'");
        assertQuery("SELECT col[3] FROM tmp_array3", "SELECT NULL");

        assertQuery("CREATE TABLE tmp_array4 AS SELECT ARRAY[TRUE, NULL] AS col", "SELECT 1");
        assertQuery("SELECT col[1] FROM tmp_array4", "SELECT TRUE");
        assertQuery("SELECT col[2] FROM tmp_array4", "SELECT NULL");

        assertQuery("CREATE TABLE tmp_array5 AS SELECT ARRAY[ARRAY[1, 2], NULL, ARRAY[3, 4]] AS col", "SELECT 1");
        assertQuery("SELECT col[1][2] FROM tmp_array5", "SELECT 2");

        assertQuery("CREATE TABLE tmp_array6 AS SELECT ARRAY[ARRAY['\"hi\"'], NULL, ARRAY['puppies']] AS col", "SELECT 1");
        assertQuery("SELECT col[1][1] FROM tmp_array6", "SELECT '\"hi\"'");
        assertQuery("SELECT col[3][1] FROM tmp_array6", "SELECT 'puppies'");
    }

    @Test
    public void testTemporalArrays()
            throws Exception
    {
        assertQuery("CREATE TABLE tmp_array7 AS SELECT ARRAY[DATE '2014-09-30'] AS col", "SELECT 1");
        assertOneNotNullResult("SELECT col[1] FROM tmp_array7");
        assertQuery("CREATE TABLE tmp_array8 AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", "SELECT 1");
        assertOneNotNullResult("SELECT col[1] FROM tmp_array8");
    }

    @Test
    public void testMaps()
            throws Exception
    {
        assertQuery("CREATE TABLE tmp_map1 AS SELECT MAP(ARRAY[0,1], ARRAY[2,NULL]) AS col", "SELECT 1");
        assertQuery("SELECT col[0] FROM tmp_map1", "SELECT 2");
        assertQuery("SELECT col[1] FROM tmp_map1", "SELECT NULL");

        assertQuery("CREATE TABLE tmp_map2 AS SELECT MAP(ARRAY[1.0], ARRAY[2.5]) AS col", "SELECT 1");
        assertQuery("SELECT col[1.0] FROM tmp_map2", "SELECT 2.5");

        assertQuery("CREATE TABLE tmp_map3 AS SELECT MAP(ARRAY['puppies'], ARRAY['kittens']) AS col", "SELECT 1");
        assertQuery("SELECT col['puppies'] FROM tmp_map3", "SELECT 'kittens'");

        assertQuery("CREATE TABLE tmp_map4 AS SELECT MAP(ARRAY[TRUE], ARRAY[FALSE]) AS col", "SELECT 1");
        assertQuery("SELECT col[TRUE] FROM tmp_map4", "SELECT FALSE");

        assertQuery("CREATE TABLE tmp_map5 AS SELECT MAP(ARRAY[1.0], ARRAY[ARRAY[1, 2]]) AS col", "SELECT 1");
        assertQuery("SELECT col[1.0][2] FROM tmp_map5", "SELECT 2");

        assertQuery("CREATE TABLE tmp_map6 AS SELECT MAP(ARRAY[DATE '2014-09-30'], ARRAY[DATE '2014-09-29']) AS col", "SELECT 1");
        assertOneNotNullResult("SELECT col[DATE '2014-09-30'] FROM tmp_map6");
        assertQuery("CREATE TABLE tmp_map7 AS SELECT MAP(ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'], ARRAY[TIMESTAMP '2001-08-22 03:04:05.321']) AS col", "SELECT 1");
        assertOneNotNullResult("SELECT col[TIMESTAMP '2001-08-22 03:04:05.321'] FROM tmp_map7");
    }

    private void assertOneNotNullResult(@Language("SQL") String query)
    {
        MaterializedResult results = queryRunner.execute(getSession(), query).toJdbcTypes();
        assertEquals(results.getRowCount(), 1);
        assertEquals(results.getMaterializedRows().get(0).getFieldCount(), 1);
        assertNotNull(results.getMaterializedRows().get(0).getField(0));
    }
}
