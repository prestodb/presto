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
package com.facebook.presto.raptor;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.facebook.presto.raptor.RaptorColumnHandle.SHARD_UUID_COLUMN_TYPE;
import static com.facebook.presto.raptor.RaptorQueryRunner.createRaptorQueryRunner;
import static com.facebook.presto.raptor.RaptorQueryRunner.createSampledSession;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

public class TestRaptorIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    @SuppressWarnings("unused")
    public TestRaptorIntegrationSmokeTest()
            throws Exception
    {
        this(createRaptorQueryRunner(ImmutableMap.of(), true, false));
    }

    protected TestRaptorIntegrationSmokeTest(QueryRunner queryRunner)
    {
        super(queryRunner, createSampledSession());
    }

    @Test
    public void testCreateArrayTable()
            throws Exception
    {
        assertUpdate("CREATE TABLE array_test AS SELECT ARRAY [1, 2, 3] AS c", 1);
        assertQuery("SELECT cardinality(c) FROM array_test", "SELECT 3");
        assertUpdate("DROP TABLE array_test");
    }

    @Test
    public void testMapTable()
            throws Exception
    {
        assertUpdate("CREATE TABLE map_test AS SELECT MAP(ARRAY [1, 2, 3], ARRAY ['hi', 'bye', NULL]) AS c", 1);
        assertQuery("SELECT c[1] FROM map_test", "SELECT 'hi'");
        assertQuery("SELECT c[3] FROM map_test", "SELECT NULL");
        assertUpdate("DROP TABLE map_test");
    }

    @Test
    public void testInsertSelectDecimal()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_decimal(short_decimal DECIMAL(5,2), long_decimal DECIMAL(25,20))");
        assertUpdate("INSERT INTO test_decimal VALUES(DECIMAL '123.45', DECIMAL '12345.12345678901234567890')", "VALUES(1)");
        assertUpdate("INSERT INTO test_decimal VALUES(NULL, NULL)", "VALUES(1)");
        assertQuery("SELECT * FROM test_decimal", "VALUES (123.45, 12345.12345678901234567890), (NULL, NULL)");
        assertUpdate("DROP TABLE test_decimal");
    }

    @Test
    public void testShardUuidHiddenColumn()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_shard_uuid AS SELECT orderdate, orderkey FROM orders", "SELECT count(*) FROM orders");

        MaterializedResult actualResults = computeActual("SELECT *, \"$shard_uuid\" FROM test_shard_uuid");
        assertEquals(actualResults.getTypes(), ImmutableList.of(DATE, BIGINT, SHARD_UUID_COLUMN_TYPE));
        UUID arbitraryUuid = null;
        for (MaterializedRow row : actualResults.getMaterializedRows()) {
            Object uuid = row.getField(2);
            assertInstanceOf(uuid, String.class);
            arbitraryUuid = UUID.fromString((String) uuid);
        }
        assertNotNull(arbitraryUuid);

        actualResults = computeActual(format("SELECT * FROM test_shard_uuid where \"$shard_uuid\" = '%s'", arbitraryUuid));
        assertNotEquals(actualResults.getMaterializedRows().size(), 0);
        actualResults = computeActual("SELECT * FROM test_shard_uuid where \"$shard_uuid\" = 'foo'");
        assertEquals(actualResults.getMaterializedRows().size(), 0);
    }

    @Test
    public void testBucketNumberHiddenColumn()
            throws Exception
    {
        assertUpdate("" +
                        "CREATE TABLE test_bucket_number " +
                        "WITH (bucket_count = 50, bucketed_on = ARRAY ['orderkey']) " +
                        "AS SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        MaterializedResult actualResults = computeActual("SELECT DISTINCT \"$bucket_number\" FROM test_bucket_number");
        assertEquals(actualResults.getTypes(), ImmutableList.of(INTEGER));
        Set<Object> actual = actualResults.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .collect(toSet());
        assertEquals(actual, IntStream.range(0, 50).boxed().collect(toSet()));
    }

    @Test
    public void testTableProperties()
            throws Exception
    {
        computeActual("CREATE TABLE test_table_properties_1 (foo BIGINT, bar BIGINT, ds DATE) WITH (ordering=array['foo','bar'], temporal_column='ds')");
        computeActual("CREATE TABLE test_table_properties_2 (foo BIGINT, bar BIGINT, ds DATE) WITH (ORDERING=array['foo','bar'], TEMPORAL_COLUMN='ds')");
    }

    @Test
    public void testShardsSystemTable()
            throws Exception
    {
        assertQuery("" +
                        "SELECT table_schema, table_name, sum(row_count)\n" +
                        "FROM system.shards\n" +
                        "WHERE table_schema = 'tpch'\n" +
                        "  AND table_name IN ('orders', 'lineitem')\n" +
                        "GROUP BY 1, 2",
                "" +
                        "SELECT 'tpch', 'orders', (SELECT count(*) FROM orders)\n" +
                        "UNION ALL\n" +
                        "SELECT 'tpch', 'lineitem', (SELECT count(*) FROM lineitem)");
    }

    @Test
    public void testCreateBucketedTable()
            throws Exception
    {
        assertUpdate("" +
                        "CREATE TABLE orders_bucketed " +
                        "WITH (bucket_count = 50, bucketed_on = ARRAY ['orderkey']) " +
                        "AS SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM orders_bucketed", "SELECT * FROM orders");
        assertQuery("SELECT count(*) FROM orders_bucketed", "SELECT count(*) FROM orders");
        assertQuery("SELECT count(DISTINCT \"$shard_uuid\") FROM orders_bucketed", "SELECT 50");
        assertQuery("SELECT count(DISTINCT \"$bucket_number\") FROM orders_bucketed", "SELECT 50");

        assertUpdate("INSERT INTO orders_bucketed SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM orders_bucketed", "SELECT * FROM orders UNION ALL SELECT * FROM orders");
        assertQuery("SELECT count(*) FROM orders_bucketed", "SELECT count(*) * 2 FROM orders");
        assertQuery("SELECT count(DISTINCT \"$shard_uuid\") FROM orders_bucketed", "SELECT 50 * 2");
        assertQuery("SELECT count(DISTINCT \"$bucket_number\") FROM orders_bucketed", "SELECT 50");

        assertQuery("SELECT count(*) FROM orders_bucketed a JOIN orders_bucketed b USING (orderkey)", "SELECT count(*) * 4 FROM orders");

        assertUpdate("DELETE FROM orders_bucketed WHERE orderkey = 37", 2);
        assertQuery("SELECT count(*) FROM orders_bucketed", "SELECT (count(*) * 2) - 2 FROM orders");
        assertQuery("SELECT count(DISTINCT \"$shard_uuid\") FROM orders_bucketed", "SELECT 50 * 2");
        assertQuery("SELECT count(DISTINCT \"$bucket_number\") FROM orders_bucketed", "SELECT 50");

        assertUpdate("DROP TABLE orders_bucketed");
    }

    @Test
    public void testShowCreateTable()
            throws Exception
    {
        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint,\n" +
                        "   c2 double,\n" +
                        "   \"c 3\" varchar,\n" +
                        "   \"c'4\" array(bigint),\n" +
                        "   c5 map(bigint, varchar)\n" +
                        ")",
                getSession().getCatalog().get(), getSession().getSchema().get(), "test_show_create_table");
        assertUpdate(createTableSql);

        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_show_create_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        actualResult = computeActual("SHOW CREATE TABLE " + getSession().getSchema().get() + ".test_show_create_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        actualResult = computeActual("SHOW CREATE TABLE " + getSession().getCatalog().get() + "." + getSession().getSchema().get() + ".test_show_create_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   \"c\"\"1\" bigint,\n" +
                        "   c2 double,\n" +
                        "   \"c 3\" varchar,\n" +
                        "   \"c'4\" array(bigint),\n" +
                        "   c5 map(bigint, varchar)\n" +
                        ")",
                getSession().getCatalog().get(), getSession().getSchema().get(), "\"test_show_create_table\"\"2\"");
        assertUpdate(createTableSql);

        actualResult = computeActual("SHOW CREATE TABLE \"test_show_create_table\"\"2\"");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);
    }

    @Test
    public void testTablesSystemTable()
    {
        assertUpdate("" +
                "CREATE TABLE system_tables_test0 (c00 timestamp, c01 varchar, c02 double, c03 bigint, c04 bigint)");
        assertUpdate("" +
                "CREATE TABLE system_tables_test1 (c10 timestamp, c11 varchar, c12 double, c13 bigint, c14 bigint) " +
                "WITH (temporal_column = 'c10')");
        assertUpdate("" +
                "CREATE TABLE system_tables_test2 (c20 timestamp, c21 varchar, c22 double, c23 bigint, c24 bigint) " +
                "WITH (temporal_column = 'c20', ordering = ARRAY['c22', 'c21'])");
        assertUpdate("" +
                "CREATE TABLE system_tables_test3 (c30 timestamp, c31 varchar, c32 double, c33 bigint, c34 bigint) " +
                "WITH (temporal_column = 'c30', bucket_count = 40, bucketed_on = ARRAY ['c34', 'c33'])");
        assertUpdate("" +
                "CREATE TABLE system_tables_test4 (c40 timestamp, c41 varchar, c42 double, c43 bigint, c44 bigint) " +
                "WITH (temporal_column = 'c40', ordering = ARRAY['c41', 'c42'], distribution_name = 'test_distribution', bucket_count = 50, bucketed_on = ARRAY ['c43', 'c44'])");

        MaterializedResult actualResults = computeActual("SELECT * FROM system.tables");
        assertEquals(
                actualResults.getTypes(),
                ImmutableList.builder()
                        .add(VARCHAR) // table_schema
                        .add(VARCHAR) // table_name
                        .add(VARCHAR) // temporal_column
                        .add(new ArrayType(VARCHAR)) // ordering_columns
                        .add(VARCHAR) // distribution_name
                        .add(BIGINT) // bucket_count
                        .add(new ArrayType(VARCHAR)) // bucket_columns
                        .build());
        Map<String, MaterializedRow> map = actualResults.getMaterializedRows().stream()
                .filter(row -> ((String) row.getField(1)).startsWith("system_tables_test"))
                .collect(ImmutableCollectors.toImmutableMap(row -> ((String) row.getField(1))));
        assertEquals(map.size(), 5);
        assertEquals(
                map.get("system_tables_test0").getFields(),
                asList("tpch", "system_tables_test0", null, null, null, null, null));
        assertEquals(
                map.get("system_tables_test1").getFields(),
                asList("tpch", "system_tables_test1", "c10", null, null, null, null));
        assertEquals(
                map.get("system_tables_test2").getFields(),
                asList("tpch", "system_tables_test2", "c20", ImmutableList.of("c22", "c21"), null, null, null));
        assertEquals(
                map.get("system_tables_test3").getFields(),
                asList("tpch", "system_tables_test3", "c30", null, null, 40L, ImmutableList.of("c34", "c33")));
        assertEquals(
                map.get("system_tables_test4").getFields(),
                asList("tpch", "system_tables_test4", "c40", ImmutableList.of("c41", "c42"), "test_distribution", 50L, ImmutableList.of("c43", "c44")));

        actualResults = computeActual("SELECT * FROM system.tables WHERE table_schema = 'tpch'");
        long actualRowCount = actualResults.getMaterializedRows().stream()
                .filter(row -> ((String) row.getField(1)).startsWith("system_tables_test"))
                .count();
        assertEquals(actualRowCount, 5);

        actualResults = computeActual("SELECT * FROM system.tables WHERE table_name = 'system_tables_test3'");
        assertEquals(actualResults.getMaterializedRows().size(), 1);

        actualResults = computeActual("SELECT * FROM system.tables WHERE table_schema = 'tpch' and table_name = 'system_tables_test3'");
        assertEquals(actualResults.getMaterializedRows().size(), 1);

        actualResults = computeActual("" +
                "SELECT distribution_name, bucket_count, bucketing_columns, ordering_columns, temporal_column " +
                "FROM system.tables " +
                "WHERE table_schema = 'tpch' and table_name = 'system_tables_test3'");
        assertEquals(actualResults.getTypes(), ImmutableList.of(VARCHAR, BIGINT, new ArrayType(VARCHAR), new ArrayType(VARCHAR), VARCHAR));
        assertEquals(actualResults.getMaterializedRows().size(), 1);

        assertUpdate("DROP TABLE system_tables_test0");
        assertUpdate("DROP TABLE system_tables_test1");
        assertUpdate("DROP TABLE system_tables_test2");
        assertUpdate("DROP TABLE system_tables_test3");
        assertUpdate("DROP TABLE system_tables_test4");
    }
}
