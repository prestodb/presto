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
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public abstract class TestIcebergTableSampling
        extends AbstractTestQueryFramework
{
    private final CatalogType catalogType;
    private final Map<String, String> extraConnectorProperties;

    protected TestIcebergTableSampling(CatalogType catalogType, Map<String, String> extraConnectorProperties)
    {
        this.catalogType = requireNonNull(catalogType, "catalogType is null");
        this.extraConnectorProperties = requireNonNull(extraConnectorProperties, "extraConnectorProperties is null");
    }

    protected TestIcebergTableSampling(CatalogType catalogType)
    {
        this(catalogType, ImmutableMap.of());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), catalogType, extraConnectorProperties);
    }

    @Override
    public Session getSession()
    {
        return Session.builder(super.getSession())
                .setSchema("tpch")
                .build();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup()
    {
        try {
            assertUpdate("CALL iceberg.system.delete_sample_table('lineitem')");
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop sample table");
        }
    }

    @Test
    public void testCreateSampleTable()
    {
        assertQuerySucceeds("CREATE TABLE test_create_sample(i int)");
        assertUpdate(getSession(), "CALL iceberg.system.create_sample_table('test_create_sample')");
        assertQuerySucceeds("SELECT count(*) FROM \"test_create_sample$samples\"");
    }

    @Test
    public void testInsertIntoSampleTable()
    {
        assertQuerySucceeds("CREATE TABLE test_insert_sample(i int)");
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('test_insert_sample')");
        assertUpdate("INSERT INTO \"test_insert_sample$samples\" (VALUES 1, 2, 3)", 3);
    }

    @Test
    public void testQuerySampleTable()
    {
        assertQuerySucceeds("CREATE TABLE test_query_sample_table(i int)");
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('test_query_sample_table')");
        assertUpdate("INSERT INTO \"test_query_sample_table$samples\" (VALUES 3, 4, 5)", 3);
        assertQuerySucceeds("SELECT * FROM \"test_query_sample_table$samples\"");
        assertQuerySucceeds("SELECT count(*) FROM \"test_query_sample_table$samples\"");
    }

    @Test
    public void testGetStatsForSampleExplicit()
    {
        assertQuerySucceeds("CREATE TABLE get_stats_explicit(i int)");
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('get_stats_explicit')");
        assertUpdate("INSERT INTO \"get_stats_explicit$samples\" (VALUES 1, 2, 3)", 3);
        assertQuerySucceeds("SHOW STATS FOR \"get_stats_explicit$samples\"");
    }

    @Test
    public void testGetStatisticsFromActual()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("iceberg." + IcebergSessionProperties.USE_SAMPLE_STATISTICS, "false")
                .build();
        assertQuerySucceeds("CREATE TABLE test_stats_actual(i int)");
        assertUpdate("INSERT INTO test_stats_actual VALUES(1)", 1);
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('test_stats_actual')");
        assertUpdate("INSERT INTO \"test_stats_actual$samples\" VALUES (2)", 1);
        MaterializedResult r = this.computeActual(session, "SHOW STATS FOR test_stats_actual");
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

        assertQuerySucceeds("CREATE TABLE test_stats_sample(i int)");
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('test_stats_sample')");
        assertUpdate("INSERT INTO test_stats_sample VALUES(1)", 1);
        assertUpdate("INSERT INTO \"test_stats_sample$samples\" VALUES (2)", 1);
        MaterializedResult r = this.computeActual(session, "SHOW STATS FOR test_stats_sample");
        int max = Integer.parseInt(r.getMaterializedRows().get(0).getField(6).toString());
        int min = Integer.parseInt(r.getMaterializedRows().get(0).getField(5).toString());
        assertEquals(min, 2);
        assertEquals(max, 2);
    }

    @Test
    public void testSampleTableStatsUsePrev()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("iceberg." + IcebergSessionProperties.USE_SAMPLE_STATISTICS, "true")
                .build();

        assertQuerySucceeds("CREATE TABLE test_stats_sample_prev(i int)");
        assertUpdate("INSERT INTO test_stats_sample_prev VALUES(1)", 1);
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('test_stats_sample_prev')");
        assertUpdate("INSERT INTO \"test_stats_sample_prev$samples\" VALUES (2)", 1);
        assertUpdate("INSERT INTO test_stats_sample_prev VALUES(2)", 1);
        assertUpdate("INSERT INTO test_stats_sample_prev VALUES(3)", 1);
        assertUpdate("INSERT INTO test_stats_sample_prev VALUES(5)", 1);
        assertUpdate("INSERT INTO test_stats_sample_prev (VALUES 5, 6, 7, 8, 9)", 5);
        assertUpdate("INSERT INTO \"test_stats_sample_prev$samples\" VALUES (3)", 1);
        List<Long> actualSnapshots = getSnapshotIds(session, "test_stats_sample_prev");
        List<Long> snapshotsUsingPrev = actualSnapshots.subList(0, actualSnapshots.size() - 1);

        for (Long snapshot : snapshotsUsingPrev) {
            MaterializedResult r = this.computeActual(session, String.format("SHOW STATS FOR \"test_stats_sample_prev@%d\"", snapshot));
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
        assertQuerySucceeds("DROP TABLE IF EXISTS test_stats_sample_next");
        assertQuerySucceeds("CREATE TABLE test_stats_sample_next(i int)");
        assertUpdate("INSERT INTO test_stats_sample_next VALUES(1)", 1);
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('test_stats_sample_next')");
        assertUpdate("INSERT INTO \"test_stats_sample_next$samples\" VALUES (2)", 1);
        assertUpdate("INSERT INTO test_stats_sample_next (VALUES (1), (2), (3), (4), (5), (6), (7))", 7);
        assertUpdate("INSERT INTO test_stats_sample_next VALUES(2)", 1);
        assertUpdate("INSERT INTO test_stats_sample_next VALUES(3)", 1);
        assertUpdate("INSERT INTO test_stats_sample_next VALUES(5)", 1);
        assertUpdate("INSERT INTO test_stats_sample_next VALUES(6)", 1);
        assertUpdate("INSERT INTO \"test_stats_sample_next$samples\" ( VALUES (3), (1) )", 2);
        List<Long> actualSnapshots = getSnapshotIds(session, "test_stats_sample_next");
        List<Long> snapshotsUsingNext = actualSnapshots.subList(1, actualSnapshots.size());

        for (Long snapshot : snapshotsUsingNext) {
            MaterializedResult r = this.computeActual(session, String.format("SHOW STATS FOR \"test_stats_sample_next@%d\"", snapshot));
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

        assertQuerySucceeds("CREATE TABLE test_stats_no_sample_actual(i int)");
        assertUpdate("INSERT INTO test_stats_no_sample_actual VALUES(1)", 1);
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('test_stats_no_sample_actual')");
        assertUpdate("INSERT INTO \"test_stats_no_sample_actual$samples\" VALUES (2)", 1);
        assertUpdate("INSERT INTO test_stats_no_sample_actual (VALUES (1), (2), (3), (4), (5), (6), (7))", 7);
        MaterializedResult r = this.computeActual(session, "SHOW STATS FOR test_stats_no_sample_actual");
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
