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
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorQueryRunner.createRaptorQueryRunner;
import static com.facebook.presto.raptor.RaptorQueryRunner.createSampledSession;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertEquals;

public class TestRaptorDistributedQueries
        extends AbstractTestDistributedQueries
{
    public TestRaptorDistributedQueries()
            throws Exception
    {
        super(createRaptorQueryRunner(getTables()), createSampledSession());
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
    public void testRefreshMaterializedQueryTable()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_refresh_base AS " +
                "SELECT a, b, c FROM (VALUES (1, 2, 3), (1, 2, 4), (2, 3, 4), (3, 4, 5), (10, 20, 30)) t(a, b, c)", 4);

        // Create two materialized query tables. One without filtering and one with.
        assertUpdate("CREATE TABLE test_refresh_mqt1 AS " +
                "SELECT a, b, SUM(c) as c from raptor.tpch.test_refresh_base " +
                "GROUP BY a, b " +
                "WITH NO DATA " +
                "REFRESH ON DEMAND", 4);
        assertUpdate("CREATE TABLE test_refresh_mqt2 AS " +
                "SELECT a, b, SUM(c) as c from raptor.tpch.test_refresh_base " +
                "WHERE b < 10 " +
                "GROUP BY a, b " +
                "WITH NO DATA " +
                "REFRESH ON DEMAND", 3);

        // Refresh both materialized tables with no predicate
        queryRunner.execute(getSession(), "CALL system.runtime.refresh_materialized_query_table('raptor.tpch.test_refresh_mqt1', '')");
        queryRunner.execute(getSession(), "CALL system.runtime.refresh_materialized_query_table('raptor.tpch.test_refresh_mqt2', '')");

        MaterializedResult materializedRows1 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt1");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), 4L);

        materializedRows1 = computeActual("SELECT a, b, c FROM test_refresh_mqt1 ORDER BY a, b");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(1), 2L);
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(2), 7L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(0), 2L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(1), 3L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(2), 4L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(0), 3L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(1), 4L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(2), 5L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(0), 10L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(1), 20L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(2), 30L);

        MaterializedResult materializedRows2 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt2");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), 3L);

        materializedRows2 = computeActual("SELECT a, b, c FROM test_refresh_mqt2 ORDER BY a, b");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(1), 2L);
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(2), 7L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(0), 2L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(1), 3L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(2), 4L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(0), 3L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(1), 4L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(2), 5L);

        queryRunner.execute(getSession(), "INSERT INTO test_refresh_base values (1, 2, 10)");
        queryRunner.execute(getSession(), "INSERT INTO test_refresh_base values (2, 3, 5)");
        queryRunner.execute(getSession(), "INSERT INTO test_refresh_base values (10, 20, 5)");

        // Refresh both materialized tables with predicate
        queryRunner.execute(getSession(), "CALL system.runtime.refresh_materialized_query_table('raptor.tpch.test_refresh_mqt1', 'a > 1')");
        queryRunner.execute(getSession(), "CALL system.runtime.refresh_materialized_query_table('raptor.tpch.test_refresh_mqt2', 'a > 1')");

        materializedRows1 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt1");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), 4L);

        materializedRows1 = computeActual("SELECT a, b, c FROM test_refresh_mqt1 ORDER BY a, b");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(1), 2L);
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(2), 7L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(0), 2L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(1), 3L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(2), 9L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(0), 3L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(1), 4L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(2), 5L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(0), 10L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(1), 20L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(2), 35L);

        materializedRows2 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt2");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), 3L);

        materializedRows2 = computeActual("SELECT a, b, c FROM test_refresh_mqt2 ORDER BY a, b");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(1), 2L);
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(2), 7L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(0), 2L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(1), 3L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(2), 9L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(0), 3L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(1), 4L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(2), 5L);
    }

    @Test
    public void testShardUuidHiddenColumn()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_shard_uuid AS SELECT orderdate, orderkey FROM orders", "SELECT count(*) FROM orders");
        MaterializedResult actualResults = computeActual("SELECT *, \"$shard_uuid\" FROM test_shard_uuid");
        assertEquals(actualResults.getTypes(), ImmutableList.of(DATE, BIGINT, VARCHAR));
        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        for (MaterializedRow row : actualRows) {
            Object uuid = row.getField(2);
            assertInstanceOf(uuid, String.class);
            // check that the string can be parsed into a UUID
            UUID.fromString((String) uuid);
        }
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
}
