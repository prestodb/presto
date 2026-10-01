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
package com.facebook.presto.nativetests;

import com.facebook.airlift.discovery.client.Announcer;
import com.facebook.presto.connector.jmx.JmxPlugin;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static com.facebook.presto.server.testing.TestingPrestoServer.updateConnectorIdAnnouncement;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * On a native cluster (Java coordinator + Prestissimo C++ workers), the JMX connector's splits must be
 * routed to the coordinator only, because C++ workers do not expose JMX MBeans. Without that filter
 * the sidecar plan-conversion / worker split execution rejects JMX table scans and the query blocks or
 * fails. See JmxSplitManager and NativePlanChecker.
 */
public class TestPrestoNativeJmxQueries
        extends AbstractTestQueryFramework
{
    private String storageFormat;
    private boolean sidecarEnabled;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));

        // Build the native query runner the same way NativeTestsUtils.createNativeQueryRunner does, but
        // force `exclude-invalid-worker-session-properties=false` on the coordinator. When the sidecar
        // is enabled that flag defaults to true, which skips loading JavaWorkerSessionPropertyProvider
        // (ServerMainModule:892-905). The coordinator still runs JMX table scans locally via
        // LocalExecutionPlanner, and any GROUP BY/DISTINCT aggregation reads
        // `aggregation_operator_unspill_memory_limit` (LocalExecutionPlanner:1489) — an unregistered
        // property in that path throws "Unknown session property". Overriding the flag keeps the Java
        // provider registered on the coordinator; workers are unaffected.
        QueryRunner queryRunner = nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .setExtraProperties(ImmutableMap.of("exclude-invalid-worker-session-properties", "false"))
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }

        // Install the Java JMX plugin on the coordinator only; C++ workers do not host it.
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) queryRunner;
        distributedQueryRunner.getCoordinator().installPlugin(new JmxPlugin());
        ConnectorId jmxConnectorId = distributedQueryRunner.getCoordinator().createCatalog("jmx", "jmx");

        // Announce the jmx connector id on the coordinator; createCatalog() skips the announcement update
        // when node-scheduler.include-coordinator is false.
        Announcer announcer = distributedQueryRunner.getCoordinator().getInstance(Key.get(Announcer.class));
        InternalNodeManager nodeManager = distributedQueryRunner.getCoordinator().getNodeManager();
        updateConnectorIdAnnouncement(announcer, jmxConnectorId, nodeManager);

        return queryRunner;
    }

    @Test
    public void testJmxSplitStaysOnCoordinatorInNativeCluster()
    {
        // With native-execution-enabled=true, JmxSplitManager filters getAllNodes() down to coordinator
        // nodes, so exactly one JmxSplit is produced. Without the fix, a split would be sent to each C++
        // worker as well and the query would fail (workers do not expose JMX MBeans).
        MaterializedResult result = computeActual("SELECT node FROM jmx.current.\"java.lang:type=Runtime\"");
        assertEquals(result.getRowCount(), 1);
    }

    @Test
    public void testJmxQueryWithExpressions()
    {
        // Projection with an arithmetic expression on a JMX column; still one row from the coordinator.
        MaterializedResult result = computeActual(
                "SELECT node, uptime / 1000 AS uptime_seconds FROM jmx.current.\"java.lang:type=Runtime\"");
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getFieldCount(), 2);
        long uptimeSeconds = (long) result.getMaterializedRows().get(0).getField(1);
        assertTrue(uptimeSeconds >= 0);
    }

    @Test
    public void testJmxAggregationWithDistinct()
    {
        // DISTINCT is an aggregation: the planner emits a hash-partitioned shuffle between the source
        // scan and the final aggregate, so the JMX table scan stays in a SOURCE_DISTRIBUTION fragment
        // and does not collide with SystemPartitioningHandle's "source splits not supported" path.
        // With the JmxSplitManager filter the JMX scan yields a single row (the coordinator), so DISTINCT
        // returns exactly one row.
        MaterializedResult result = computeActual(
                "SELECT DISTINCT node FROM jmx.current.\"java.lang:type=Runtime\"");
        assertEquals(result.getRowCount(), 1);
    }

    @Test
    public void testJmxAggregationWithGroupBy()
    {
        // GROUP BY with count/max/min on a JMX table. The AddExchanges optimizer inserts a
        // gathering exchange after the JMX scan on native clusters (same treatment as system table
        // scans), so the JMX source stage stays SOURCE_DISTRIBUTION and the downstream aggregation
        // runs on a native worker via Velox — not on the Java coordinator where SQL-invoked
        // aggregate functions have no accumulator class. With one coordinator row, GROUP BY node
        // produces a single group.
        MaterializedResult result = computeActual(
                "SELECT node, count(uptime), max(uptime), min(uptime) " +
                        "FROM jmx.current.\"java.lang:type=Runtime\" GROUP BY node");
        assertEquals(result.getRowCount(), 1);
        assertEquals((long) result.getMaterializedRows().get(0).getField(1), 1L);
        long maxUptime = (long) result.getMaterializedRows().get(0).getField(2);
        long minUptime = (long) result.getMaterializedRows().get(0).getField(3);
        assertEquals(maxUptime, minUptime);
        assertTrue(maxUptime >= 0);
    }

    @Test
    public void testJmxJoinWithBaseTable()
    {
        // Inner join Runtime and Threading MBeans on the node column. Both are JMX tables that are
        // restricted to the coordinator by the fix, so the join yields one row.
        MaterializedResult result = computeActual(
                "SELECT r.node, r.uptime, t.threadcount " +
                        "FROM jmx.current.\"java.lang:type=Runtime\" r " +
                        "JOIN jmx.current.\"java.lang:type=Threading\" t ON r.node = t.node");
        assertEquals(result.getRowCount(), 1);
        long uptime = (long) result.getMaterializedRows().get(0).getField(1);
        long threadCount = (long) result.getMaterializedRows().get(0).getField(2);
        assertTrue(uptime >= 0);
        assertTrue(threadCount > 0);
    }
}
