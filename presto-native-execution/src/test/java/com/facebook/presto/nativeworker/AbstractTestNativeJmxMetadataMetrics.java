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

import com.facebook.airlift.discovery.client.Announcer;
import com.facebook.presto.connector.jmx.JmxPlugin;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.MetadataManagerStats;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.inject.Key;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.weakref.jmx.MBeanExporter;

import javax.management.MBeanServer;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNationWithFormat;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.server.testing.TestingPrestoServer.updateConnectorIdAnnouncement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestNativeJmxMetadataMetrics
        extends AbstractTestQueryFramework
{
    private final String storageFormat = "PARQUET";
    protected abstract String getCatalogName();
    protected abstract String getSchemaName();
    protected abstract MBeanServer getMBeanServer();
    protected String getTableName(String table)
    {
        return String.format("%s.%s.%s", getCatalogName(), getSchemaName(), table);
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createRegion(queryRunner);
        createNationWithFormat(queryRunner, storageFormat);
    }

    @BeforeClass
    public void setUp()
    {
        try {
            com.facebook.presto.tests.DistributedQueryRunner distributedQueryRunner = (com.facebook.presto.tests.DistributedQueryRunner) getQueryRunner();
            // Install JMX plugin on coordinator only
            distributedQueryRunner.getCoordinator().installPlugin(new JmxPlugin());
            ConnectorId jmxConnectorId = distributedQueryRunner.getCoordinator().createCatalog("jmx", "jmx");

            // Manually update the coordinator's connectorIds announcement
            // This is needed because createCatalog() skips the announcement update for coordinators
            // when node-scheduler.include-coordinator is false (see TestingPrestoServer.createCatalog)
            Announcer announcer = distributedQueryRunner.getCoordinator().getInstance(Key.get(Announcer.class));
            InternalNodeManager nodeManager = distributedQueryRunner.getCoordinator().getNodeManager();
            updateConnectorIdAnnouncement(announcer, jmxConnectorId, nodeManager);
            // Register MetadataManagerStats to the MBeanServer used by JmxPlugin
            // This is needed because MetadataManagerStats is registered to TestingMBeanServer (via ServerMainModule)
            // but JmxPlugin queries a different MBeanServer (platform or custom)
            MetadataManagerStats stats = distributedQueryRunner.getCoordinator().getInstance(Key.get(MetadataManagerStats.class));
            MBeanExporter exporter = new MBeanExporter(getMBeanServer());
            exporter.export("com.facebook.presto.metadata:name=MetadataManagerStats", stats);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to set up JMX connector announcement and register MetadataManagerStats MBean", e);
        }
    }

    @Test
    public void testMetadataManagerStatsExist()
    {
        String jmxQuery = "SELECT * FROM jmx.current.\"com.facebook.presto.metadata:name=MetadataManagerStats\"";
        MaterializedResult result = computeActual(jmxQuery);
        assertTrue(result.getRowCount() > 0);
    }

    @Test
    public void testMetadataMetricsAfterQueries()
    {
        // Query to get metadata metrics
        String metricsQuery = "SELECT getTableMetadataCalls, listTablesCalls, getColumnHandlesCalls " +
                "FROM jmx.current.\"com.facebook.presto.metadata:name=MetadataManagerStats\"";

        MaterializedResult initialResult = computeActual(metricsQuery);
        assertEquals(initialResult.getRowCount(), 1);
        long initialTableMetadataCalls = (long) initialResult.getMaterializedRows().get(0).getField(0);
        long initialColumnHandlesCalls = (long) initialResult.getMaterializedRows().get(0).getField(2);

        assertQuerySucceeds(String.format("SHOW TABLES FROM %s.%s", getCatalogName(), getSchemaName()));
        assertQuerySucceeds(String.format("SELECT * FROM %s", getTableName("nation")));
        assertQuerySucceeds(String.format("DESCRIBE %s", getTableName("nation")));
        assertQuerySucceeds(String.format("SHOW COLUMNS FROM %s", getTableName("region")));
        assertQuerySucceeds(String.format("SELECT n.name, r.name FROM %s n JOIN %s r ON n.regionkey = r.regionkey", getTableName("nation"), getTableName("region")));

        MaterializedResult updatedResult = computeActual(metricsQuery);
        assertEquals(updatedResult.getRowCount(), 1);
        long updatedTableMetadataCalls = (long) updatedResult.getMaterializedRows().get(0).getField(0);
        long updatedColumnHandlesCalls = (long) updatedResult.getMaterializedRows().get(0).getField(2);

        assertTrue(updatedTableMetadataCalls >= initialTableMetadataCalls);
        assertTrue(updatedColumnHandlesCalls >= initialColumnHandlesCalls);
    }

    @Test
    public void testMetadataTimingMetrics()
    {
        assertQuerySucceeds(String.format("SHOW TABLES FROM %s.%s", getCatalogName(), getSchemaName()));
        assertQuerySucceeds(String.format("DESCRIBE %s", getTableName("nation")));
        assertQuerySucceeds(String.format("SELECT * FROM %s LIMIT 10", getTableName("nation")));
        assertQuerySucceeds(String.format("SELECT count(*) FROM %s", getTableName("region")));

        String timingQuery = "SELECT \"gettablemetadatatime.alltime.count\", \"listtablestime.alltime.count\",\"getcolumnhandlestime.alltime.count\" " +
                "FROM jmx.current.\"com.facebook.presto.metadata:name=MetadataManagerStats\"";

        MaterializedResult result = computeActual(timingQuery);
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getFieldCount(), 3);

        double getTableMetadataCount = (double) result.getMaterializedRows().get(0).getField(0);
        double listTablesCount = (double) result.getMaterializedRows().get(0).getField(1);
        double getColumnHandlesCount = (double) result.getMaterializedRows().get(0).getField(2);
        assertTrue(getTableMetadataCount >= 0);
        assertTrue(listTablesCount >= 0);
        assertTrue(getColumnHandlesCount >= 0);
    }

    @Test
    public void testMultipleOperationsIncrementMetrics()
    {
        String countQuery = "SELECT " +
                "getTableMetadataCalls + listTablesCalls + getColumnHandlesCalls as total_calls " +
                "FROM jmx.current.\"com.facebook.presto.metadata:name=MetadataManagerStats\"";

        MaterializedResult initialResult = computeActual(countQuery);
        long initialTotalCalls = (long) initialResult.getMaterializedRows().get(0).getField(0);

        for (int i = 0; i < 5; i++) {
            assertQuerySucceeds(String.format("SHOW TABLES FROM %s.%s", getCatalogName(), getSchemaName()));
            assertQuerySucceeds(String.format("SELECT * FROM %s WHERE nationkey < 10", getTableName("nation")));
            assertQuerySucceeds(String.format("SELECT count(*) FROM %s", getTableName("region")));
        }

        MaterializedResult updatedResult = computeActual(countQuery);
        long updatedTotalCalls = (long) updatedResult.getMaterializedRows().get(0).getField(0);

        assertTrue(updatedTotalCalls > initialTotalCalls);
    }

    @Test
    public void testMetadataStatsWithComplexQueries()
    {
        String metricsQuery = "SELECT getTableMetadataCalls, getColumnHandlesCalls " +
                "FROM jmx.current.\"com.facebook.presto.metadata:name=MetadataManagerStats\"";

        MaterializedResult initialResult = computeActual(metricsQuery);
        long initialTableMetadataCalls = (long) initialResult.getMaterializedRows().get(0).getField(0);
        long initialColumnHandlesCalls = (long) initialResult.getMaterializedRows().get(0).getField(1);

        assertQuerySucceeds(String.format("SELECT n.name, r.name FROM %s n JOIN %s r ON n.regionkey = r.regionkey WHERE n.nationkey < 10",
                getTableName("nation"), getTableName("region")));
        assertQuerySucceeds(String.format("SELECT * FROM %s WHERE nationkey IN (SELECT nationkey FROM %s WHERE regionkey = 1)",
                getTableName("nation"), getTableName("nation")));
        assertQuerySucceeds(String.format("WITH nation_counts AS (SELECT regionkey, count(*) as cnt FROM %s GROUP BY regionkey) " +
                        "SELECT r.name, nc.cnt FROM %s r JOIN nation_counts nc ON r.regionkey = nc.regionkey",
                getTableName("nation"), getTableName("region")));

        MaterializedResult updatedResult = computeActual(metricsQuery);
        long updatedTableMetadataCalls = (long) updatedResult.getMaterializedRows().get(0).getField(0);
        long updatedColumnHandlesCalls = (long) updatedResult.getMaterializedRows().get(0).getField(1);

        assertTrue(updatedTableMetadataCalls >= initialTableMetadataCalls);
        assertTrue(updatedColumnHandlesCalls >= initialColumnHandlesCalls);
    }

    @Test
    public void testMetadataStatsNodeColumn()
    {
        String nodeQuery = "SELECT DISTINCT node FROM jmx.current.\"com.facebook.presto.metadata:name=MetadataManagerStats\"";
        MaterializedResult result = computeActual(nodeQuery);

        assertEquals(result.getRowCount(), 1);

        MaterializedResult coordinatorNodes = computeActual("SELECT node_id FROM system.runtime.nodes WHERE coordinator = true");
        assertEquals(coordinatorNodes.getRowCount(), 1);

        String metadataStatsNode = (String) result.getMaterializedRows().get(0).getField(0);
        String coordinatorNode = (String) coordinatorNodes.getMaterializedRows().get(0).getField(0);
        assertEquals(metadataStatsNode, coordinatorNode);
    }
}
