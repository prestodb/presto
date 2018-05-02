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
package com.facebook.presto.plugin.phoenix;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test
public class TestPhoenixIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    public TestPhoenixIntegrationSmokeTest()
            throws Exception
    {
        super(() -> PhoenixQueryRunner.createPhoenixQueryRunner(ImmutableMap.of(), ImmutableList.of(ORDERS)));
    }

    @Test
    public void testMultipleSomeColumnsRangesPredicate()
    {
        assertQuery("SELECT orderkey, shippriority, clerk, totalprice, custkey  FROM ORDERS WHERE orderkey BETWEEN 10 AND 50 or orderkey BETWEEN 100 AND 150");
    }

    @Test
    public void testCreateTableWithProperties()
    {
        assertUpdate("CREATE TABLE test_create_table_as_if_not_exists (created_date timestamp, a bigint, b double, c varchar(10), d varchar(10)) with(rowkeys = ARRAY['created_date row_timestamp','a', 'b', 'c'], SALT_BUCKETS=10)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));
        assertTableColumnNames("test_create_table_as_if_not_exists", "created_date", "a", "b", "c", "d");
    }

    @Test
    public void testCreateTableWithPresplits()
    {
        assertUpdate("CREATE TABLE test_create_presplits_table_as_if_not_exists (rid varchar(10), val1 varchar(10)) with(rowkeys = ARRAY['rid'], SPLIT_ON='\"1\",\"2\",\"3\"')");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_presplits_table_as_if_not_exists"));
        assertTableColumnNames("test_create_presplits_table_as_if_not_exists", "rid", "val1");
    }

    @Test
    public void tesdtDuplicateKeyUpdateColumns()
    {
        assertUpdate("CREATE TABLE test_dup_columns WITH (ROWKEYS = ARRAY['RID']) AS SELECT 'key' as RID, 100 AS COL1, 200 AS COL2, 300 AS COL3 ", 1);
        assertQuery("SELECT col1 FROM test_dup_columns where rid = 'key'", "SELECT 100");
        assertQuery("SELECT col2 FROM test_dup_columns where rid = 'key'", "SELECT 200");
        assertQuery("SELECT col3 FROM test_dup_columns where rid = 'key'", "SELECT 300");

        assertUpdate("INSERT INTO test_dup_columns VALUES('key', 1000, 2000, 3000)", 1);
        assertQuery("SELECT col1 FROM test_dup_columns where rid = 'key'", "SELECT 1000");
        assertQuery("SELECT col2 FROM test_dup_columns where rid = 'key'", "SELECT 2000");
        assertQuery("SELECT col3 FROM test_dup_columns where rid = 'key'", "SELECT 3000");

        Session session = testSessionBuilder()
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setCatalogSessionProperty("phoenix", "duplicate_key_update_columns", "col1 and col3").build();

        assertUpdate(session, "INSERT INTO test_dup_columns VALUES('key', 10000, 20000, 30000)", 1);
        assertQuery(session, "SELECT col1 FROM test_dup_columns where rid = 'key'", "SELECT 11000");
        assertQuery(session, "SELECT col2 FROM test_dup_columns where rid = 'key'", "SELECT 2000");
        assertQuery(session, "SELECT col3 FROM test_dup_columns where rid = 'key'", "SELECT 33000");

        session = testSessionBuilder()
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get()).build();
        assertUpdate(session, "INSERT INTO test_dup_columns VALUES('key', 1000, 2000, 3000)", 1);
        assertQuery(session, "SELECT col1 FROM test_dup_columns where rid = 'key'", "SELECT 1000");
        assertQuery(session, "SELECT col2 FROM test_dup_columns where rid = 'key'", "SELECT 2000");
        assertQuery(session, "SELECT col3 FROM test_dup_columns where rid = 'key'", "SELECT 3000");
    }

    @Test
    public void createTableWithEveryType()
            throws Exception
    {
        @Language("SQL")
        String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' col_varchar" +
                ", cast('bar' as varbinary) col_varbinary" +
                ", cast(1 as bigint) col_bigint" +
                ", 2 col_integer" +
                ", CAST('3.14' AS DOUBLE) col_double" +
                ", true col_boolean" +
                ", DATE '1980-05-07' col_date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' col_timestamp" +
                ", CAST('3.14' AS DECIMAL(3,2)) col_decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) col_decimal_long" +
                ", CAST('bar' AS CHAR(10)) col_char";

        assertUpdate(query, 1);

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_types_table where CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = col_decimal_long").toTestTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 2);
        assertEquals(row.getField(4), 3.14);
        assertEquals(row.getField(5), true);
        assertEquals(row.getField(6), LocalDate.of(1980, 5, 7));
        assertEquals(row.getField(7), LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertEquals(row.getField(8), new BigDecimal("3.14"));
        assertEquals(row.getField(9), new BigDecimal("12345678901234567890.0123456789"));
        assertEquals(row.getField(10), "bar       ");
        assertUpdate("DROP TABLE test_types_table");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_types_table"));
    }

    @Test
    public void testArrays()
    {
        assertUpdate("CREATE TABLE tmp_array1 AS SELECT 'key' as rkey, ARRAY[1, 2, NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array1", "SELECT 2");
        assertQuery("SELECT col[3] FROM tmp_array1", "SELECT 0");

        assertUpdate("CREATE TABLE tmp_array2 AS SELECT 'key' as rkey, ARRAY[1.0E0, 2.5E0, 3.5E0] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array2", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_array3 AS SELECT 'key' as rkey, ARRAY['puppies', 'kittens', NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array3", "SELECT 'kittens'");
        assertQuery("SELECT col[3] FROM tmp_array3", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array4 AS SELECT 'key' as rkey, ARRAY[TRUE, NULL] AS col", 1);
        assertQuery("SELECT col[1] FROM tmp_array4", "SELECT TRUE");
        assertQuery("SELECT col[2] FROM tmp_array4", "SELECT FALSE");
    }

    @Test
    public void testTemporalArrays()
    {
        assertUpdate("CREATE TABLE tmp_array7 AS SELECT 'key' as rkey, ARRAY[DATE '2014-09-30'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array7");
        assertUpdate("CREATE TABLE tmp_array8 AS SELECT 'key' as rkey, ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array8");
    }

    private void assertOneNotNullResult(String query)
    {
        MaterializedResult results = getQueryRunner().execute(getSession(), query).toTestTypes();
        assertEquals(results.getRowCount(), 1);
        assertEquals(results.getMaterializedRows().get(0).getFieldCount(), 1);
        assertNotNull(results.getMaterializedRows().get(0).getField(0));
    }
}
