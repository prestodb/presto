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
import com.google.inject.Key;
import org.testng.annotations.Test;

import static com.facebook.presto.server.testing.TestingPrestoServer.updateConnectorIdAnnouncement;
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

        QueryRunner queryRunner = NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled);

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

    // Aggregation directly on a JMX table (e.g. SELECT count(node) FROM jmx.current."…") is not covered
    // here: on native clusters the plan fragmenter puts the JMX scan into a system-partitioned stage,
    // and SystemPartitioningHandle rejects source splits with UnsupportedOperationException. That is a
    // pre-existing plan-scheduling limitation unrelated to this fix (before this PR JMX queries failed
    // even earlier on native clusters, so it was never observable). Aggregation coverage on native JMX
    // should be added once that limitation is resolved.

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
