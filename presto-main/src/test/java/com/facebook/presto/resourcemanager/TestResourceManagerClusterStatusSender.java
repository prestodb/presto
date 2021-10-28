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
package com.facebook.presto.resourcemanager;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.MockManagedQueryExecution;
import com.facebook.presto.memory.MemoryInfo;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.spi.ConnectorId;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.OptionalInt;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertTrue;

public class TestResourceManagerClusterStatusSender
{
    public static final ConnectorId CONNECTOR_ID = new ConnectorId("dummy");
    public static final NodeStatus NODE_STATUS = new NodeStatus(
            "nodeId",
            new NodeVersion("1"),
            "environment",
            false,
            new Duration(1, SECONDS),
            "externalAddress",
            "internalAddress",
            new MemoryInfo(new DataSize(1, MEGABYTE), ImmutableMap.of()),
            1,
            1.0,
            2.0,
            1,
            2,
            3);
    private static final int HEARTBEAT_INTERVAL = 100;
    private static final int SLEEP_DURATION = 1000;
    private static final int TARGET_HEARTBEATS = SLEEP_DURATION / HEARTBEAT_INTERVAL;

    private ResourceManagerClusterStatusSender sender;
    private TestingResourceManagerClient resourceManagerClient;

    @BeforeTest
    public void setup()
    {
        resourceManagerClient = new TestingResourceManagerClient();
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(CONNECTOR_ID, new InternalNode("identifier", URI.create("http://localhost:80/identifier"), OptionalInt.of(1), "1", false, true));

        sender = new ResourceManagerClusterStatusSender(
                (addressSelectionContext, headers) -> resourceManagerClient,
                nodeManager,
                () -> NODE_STATUS,
                newSingleThreadScheduledExecutor(),
                new ResourceManagerConfig()
                        .setNodeHeartbeatInterval(new Duration(HEARTBEAT_INTERVAL, MILLISECONDS))
                        .setQueryHeartbeatInterval(new Duration(HEARTBEAT_INTERVAL, MILLISECONDS)));
    }

    @AfterTest
    public void tearDown()
    {
        sender.stop();
    }

    @Test(timeOut = 2_000)
    public void testNodeStatus()
            throws Exception
    {
        sender.init();

        Thread.sleep(SLEEP_DURATION);

        int nodeHeartbeats = resourceManagerClient.getNodeHeartbeats();
        assertTrue(nodeHeartbeats > TARGET_HEARTBEATS * 0.5 && nodeHeartbeats <= TARGET_HEARTBEATS * 1.5,
                format("Expect number of heartbeats to fall within target range (%s), +/- 50%%.  Was: %s", TARGET_HEARTBEATS, nodeHeartbeats));
    }

    @Test(timeOut = 4_000)
    public void testQueryHeartbeat()
            throws Exception
    {
        MockManagedQueryExecution queryExecution = new MockManagedQueryExecution(1);
        sender.registerQuery(queryExecution);

        Thread.sleep(SLEEP_DURATION);

        int queryHeartbeats = resourceManagerClient.getQueryHeartbeats();
        assertTrue(queryHeartbeats > TARGET_HEARTBEATS * 0.5 && queryHeartbeats <= TARGET_HEARTBEATS * 1.5,
                format("Expect number of heartbeats to fall within target range (%s), +/- 50%%.  Was: %s", TARGET_HEARTBEATS, queryHeartbeats));

        // Completing the query stops the heartbeats
        queryExecution.complete();
        queryHeartbeats = resourceManagerClient.getQueryHeartbeats();

        Thread.sleep(SLEEP_DURATION);

        assertTrue(resourceManagerClient.getQueryHeartbeats() <= (queryHeartbeats + 1));
    }
}
