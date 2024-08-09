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

import com.facebook.drift.client.address.SimpleAddressSelector;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static java.lang.Thread.sleep;
import static org.testng.Assert.assertTrue;

public class TestLeaderReElection
{
    private static final int COORDINATOR_COUNT = 2;
    private static final int RESOURCE_MANAGER_COUNT = 2;
    private TestingPrestoServer coordinator1;
    private TestingPrestoServer coordinator2;
    private TestingPrestoServer resourceManager1;
    private TestingPrestoServer resourceManager2;
    private DistributedQueryRunner runner;
    private String groupId;

    @BeforeClass
    public void setup()
            throws Exception
    {
        groupId = "testRaftGroupId1";
        runner = createQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("raft.isEnabled", "true")
                        .put("raft.port", "6000")
                        .put("raft.groupId", groupId)
                        .put("raft.storageDir", "/tmp/raft-server")
                        .put("raft.required-peers-active", "2")
                        .put("raft.peers-timeout", "2ms")
                        .build(),
                ImmutableMap.of(
                        "query.client.timeout", "20s",
                        "resource-manager.query-heartbeat-interval", "100ms",
                        "resource-group-runtimeinfo-refresh-interval", "100ms",
                        "concurrency-threshold-to-enable-resource-group-refresh", "0.1"),
                ImmutableMap.of(
                        "raft.isEnabled", "true",
                        "raft.groupId", groupId,
                        "raft.required-peers-active", "2",
                        "raft.peers-timeout", "2ms"),
                COORDINATOR_COUNT,
                RESOURCE_MANAGER_COUNT);
        coordinator1 = runner.getCoordinator(0);
        coordinator2 = runner.getCoordinator(1);
        this.resourceManager1 = runner.getResourceManager(0);
        this.resourceManager2 = runner.getResourceManager(1);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(coordinator1);
        closeQuietly(coordinator2);
        closeQuietly(resourceManager1);
        closeQuietly(resourceManager2);
        coordinator1 = null;
        coordinator2 = null;
        resourceManager1 = null;
        resourceManager2 = null;
    }

    @Test(timeOut = 60_000)
    public void testRaftServerForResourceManager()
            throws Exception
    {
        RatisClient client = coordinator1.getRatisClient();
        LeaderResourceManagerAddressSelector selector = new LeaderResourceManagerAddressSelector(resourceManager1.getNodeManager(), client);
        Optional<SimpleAddressSelector.SimpleAddress> addressBeforeShutdown = selector.selectAddress(Optional.empty());
        assertTrue(addressBeforeShutdown.isPresent());

        if (client.getLeader().equals(resourceManager1.getNodeManager().getCurrentNode().getNodeIdentifier())) {
            resourceManager1.close();
        }
        else {
            resourceManager2.close();
        }

        client.sendMessage();
        sleep(5000);

        Optional<SimpleAddressSelector.SimpleAddress> addressAfterShutdown = selector.selectAddress(Optional.empty());
        assertTrue(addressAfterShutdown.isPresent());
        assertTrue(!addressBeforeShutdown.get().equals(addressAfterShutdown.get()));
    }
}
