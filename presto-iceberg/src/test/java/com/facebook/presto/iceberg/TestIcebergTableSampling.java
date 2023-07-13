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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static org.testng.Assert.assertEquals;

public class TestIcebergTableSampling
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of(), ImmutableMap.of());
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup()
    {
        try {
            assertUpdate("CALL iceberg.system.delete_sample_table('tpch', 'lineitem')");
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop sample table");
        }
    }

    @Test
    public void testCreateSampleTable()
    {
        assertUpdate("CALL iceberg.system.create_sample_table('tpch', 'lineitem')");
        assertQuerySucceeds("SELECT count(*) FROM \"lineitem$samples\"");
    }

    @Test
    public void testInsertIntoSampleTable()
    {
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('tpch', 'lineitem')");
        assertUpdate("INSERT INTO \"lineitem$samples\" SELECT * FROM tpch.lineitem LIMIT 3", 3);
    }

    @Test
    public void testQuerySampleTable()
    {
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('tpch', 'lineitem')");
        assertUpdate("INSERT INTO \"lineitem$samples\" SELECT * FROM tpch.lineitem LIMIT 3", 3);
        assertQuerySucceeds("SELECT * FROM \"lineitem$samples\"");
        assertQuerySucceeds("SELECT count(*) FROM \"lineitem$samples\"");
    }

    @Test
    public void testGetStatsForSampleExplicit()
    {
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('tpch', 'lineitem')");
        assertUpdate("INSERT INTO \"lineitem$samples\" SELECT * FROM tpch.lineitem LIMIT 3", 3);
        assertQuerySucceeds("SHOW STATS FOR \"lineitem$samples\"");
    }

    @Test
    public void testGetStatisticsFromActual()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("iceberg." + IcebergSessionProperties.USE_SAMPLE_STATISTICS, "false")
                .build();

        assertQuerySucceeds("DROP TABLE IF EXISTS test");
        assertQuerySucceeds("CREATE TABLE test(i int)");
        assertUpdate("INSERT INTO test VALUES(1)", 1);
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('tpch', 'test')");
        assertUpdate("INSERT INTO \"test$samples\" VALUES (2)", 1);
        MaterializedResult r = this.computeActual(session, "SHOW STATS FOR test");
        int max = Integer.parseInt(r.getMaterializedRows().get(0).getField(6).toString());
        int min = Integer.parseInt(r.getMaterializedRows().get(0).getField(5).toString());
        assertEquals(min, 1);
        assertEquals(max, 1);
    }

    @Test
    public void testGetStatisticsFromSample()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("iceberg." + IcebergSessionProperties.USE_SAMPLE_STATISTICS, "true")
                .build();

        assertQuerySucceeds("DROP TABLE IF EXISTS test");
        assertQuerySucceeds("CREATE TABLE test(i int)");
        assertUpdate("INSERT INTO test VALUES(1)", 1);
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('tpch', 'test')");
        assertUpdate("INSERT INTO \"test$samples\" VALUES (2)", 1);
        MaterializedResult r = this.computeActual(session, "SHOW STATS FOR test");
        int max = Integer.parseInt(r.getMaterializedRows().get(0).getField(6).toString());
        int min = Integer.parseInt(r.getMaterializedRows().get(0).getField(5).toString());
        assertEquals(min, 2);
        assertEquals(max, 2);
    }

    @Test
    public void testSnapshotTableStatsUsePrev()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("iceberg." + IcebergSessionProperties.USE_SAMPLE_STATISTICS, "true")
                .build();

        assertQuerySucceeds("DROP TABLE IF EXISTS test");
        assertQuerySucceeds("CREATE TABLE test(i int)");
        assertUpdate("INSERT INTO test VALUES(1)", 1);
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('tpch', 'test')");
        assertUpdate("INSERT INTO \"test$samples\" VALUES (2)", 1);
        assertUpdate("INSERT INTO test VALUES(2)", 1);
        assertUpdate("INSERT INTO test VALUES(3)", 1);
        assertUpdate("INSERT INTO test VALUES(5)", 1);
        assertUpdate("INSERT INTO test (VALUES 5, 6, 7, 8, 9)", 5);
        assertUpdate("INSERT INTO \"test$samples\" VALUES (3)", 1);
        List<Long> actualSnapshots = getSnapshotIds(session, "test");
        List<Long> snapshotsUsingPrev = actualSnapshots.subList(0, actualSnapshots.size() - 1);

        for (Long snapshot : snapshotsUsingPrev) {
            MaterializedResult r = this.computeActual(session, String.format("SHOW STATS FOR \"test@%d\"", snapshot));
            int max = Integer.parseInt(r.getMaterializedRows().get(0).getField(6).toString());
            int min = Integer.parseInt(r.getMaterializedRows().get(0).getField(5).toString());
            assertEquals(min, 2);
            assertEquals(max, 2);
        }
    }

    @Test
    public void testSnapshotTableStatsUseNext()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("iceberg." + IcebergSessionProperties.USE_SAMPLE_STATISTICS, "true")
                .build();
        assertQuerySucceeds("DROP TABLE IF EXISTS test");
        assertQuerySucceeds("CREATE TABLE test(i int)");
        assertUpdate("INSERT INTO test VALUES(1)", 1);
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('tpch', 'test')");
        assertUpdate("INSERT INTO \"test$samples\" VALUES (2)", 1);
        assertUpdate("INSERT INTO test (VALUES (1), (2), (3), (4), (5), (6), (7))", 7);
        assertUpdate("INSERT INTO test VALUES(2)", 1);
        assertUpdate("INSERT INTO test VALUES(3)", 1);
        assertUpdate("INSERT INTO test VALUES(5)", 1);
        assertUpdate("INSERT INTO test VALUES(6)", 1);
        assertUpdate("INSERT INTO \"test$samples\" ( VALUES (3), (1) )", 2);
        List<Long> actualSnapshots = getSnapshotIds(session, "test");
        List<Long> snapshotsUsingNext = actualSnapshots.subList(1, actualSnapshots.size());

        for (Long snapshot : snapshotsUsingNext) {
            MaterializedResult r = this.computeActual(session, String.format("SHOW STATS FOR \"test@%d\"", snapshot));
            int max = Integer.parseInt(r.getMaterializedRows().get(0).getField(6).toString());
            int min = Integer.parseInt(r.getMaterializedRows().get(0).getField(5).toString());
            assertEquals(min, 1);
            assertEquals(max, 3);
        }
    }

    @Test
    public void testGetStatsNoSampleAfterUpdateActual()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("iceberg." + IcebergSessionProperties.USE_SAMPLE_STATISTICS, "true")
                .build();
        assertQuerySucceeds("DROP TABLE IF EXISTS test");
        assertQuerySucceeds("CREATE TABLE test(i int)");
        assertUpdate("INSERT INTO test VALUES(1)", 1);
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('tpch', 'test')");
        assertUpdate("INSERT INTO \"test$samples\" VALUES (2)", 1);
        assertUpdate("INSERT INTO test (VALUES (1), (2), (3), (4), (5), (6), (7))", 7);
        MaterializedResult r = this.computeActual(session, "SHOW STATS FOR test");
        int max = Integer.parseInt(r.getMaterializedRows().get(0).getField(6).toString());
        int min = Integer.parseInt(r.getMaterializedRows().get(0).getField(5).toString());
        assertEquals(min, 2);
        assertEquals(max, 2);
    }

    private List<Long> getSnapshotIds(Session s, String tableName)
    {
        return this.computeActual(s, String.format("SELECT snapshot_id from \"%s$snapshots\"", tableName))
                .getMaterializedRows()
                .stream()
                .map(x -> (Long) x.getField(0))
                .collect(Collectors.toList());
    }
}
