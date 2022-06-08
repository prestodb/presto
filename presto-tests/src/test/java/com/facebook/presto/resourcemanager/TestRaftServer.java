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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static org.testng.Assert.assertTrue;

public class TestRaftServer
{
    private static final String RESOURCE_GROUPS_CONFIG_FILE = "resource_groups_config_simple.json";
    private static final int COORDINATOR_COUNT = 2;
    private static final int RESOURCE_MANAGER_COUNT = 2;
    private static final String RESOURCE_GROUP_GLOBAL = "global";
    private HttpClient client;
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
        client = new JettyHttpClient();
        groupId = "testRaftGroupId1";
        runner = createQueryRunner(
                ImmutableMap.of(
                        "resource-manager.query-expiration-timeout", "4m",
                        "raft.isEnabled", "true",
                        "raft.port", "6000",
                        "raft.groupId", groupId,
                        "raft.storageDir", "/tmp/raft-server"),
                ImmutableMap.of(
                        "query.client.timeout", "20s",
                        "resource-manager.query-heartbeat-interval", "100ms",
                        "resource-group-runtimeinfo-refresh-interval", "100ms",
                        "concurrency-threshold-to-enable-resource-group-refresh", "0.1"),
                ImmutableMap.of(),
                COORDINATOR_COUNT,
                RESOURCE_MANAGER_COUNT);
        coordinator1 = runner.getCoordinator(0);
        coordinator2 = runner.getCoordinator(1);
        this.resourceManager1 = runner.getResourceManager(0);
        this.resourceManager2 = runner.getResourceManager(1);
        runner.getCoordinators().stream().forEach(coordinator -> {
            coordinator.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
            coordinator.getResourceGroupManager().get()
                    .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath(RESOURCE_GROUPS_CONFIG_FILE)));
        });
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(coordinator1);
        closeQuietly(coordinator2);
        closeQuietly(resourceManager1);
        closeQuietly(resourceManager2);
        closeQuietly(client);
        coordinator1 = null;
        coordinator2 = null;
        resourceManager1 = null;
        resourceManager2 = null;
        client = null;
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    @Test(timeOut = 120_000)
    public void testRaftServerForResourceManager()
            throws Exception
    {
        //Making Raft Peers for resource managers
        Set<InternalNode> resourceManagers = resourceManager1.getNodeManager().getResourceManagers();
        RaftPeer[] peers = resourceManagers.stream().map(resourceManager -> {
            RaftPeer.Builder builder = RaftPeer.newBuilder();
            builder.setId(resourceManager.getNodeIdentifier())
                    .setAddress(resourceManager.getHost() + ":" + resourceManager.getRaftPort().getAsInt());
            return builder.build();
        }).toArray(RaftPeer[]::new);

        //Building Raft Client in order to check who the leader is
        RaftClient.Builder builder = RaftClient.newBuilder()
                .setProperties(new RaftProperties())
                .setRaftGroup(RaftGroup.valueOf(RaftGroupId.valueOf(UUID.nameUUIDFromBytes(groupId.getBytes())), peers));
        RaftClient client = builder.build();

        //Checking resource manager leader is chosen
        List<String> resourceManagerIdentifiers = new ArrayList<String>();
        for (InternalNode resourceManager : resourceManager1.getNodeManager().getResourceManagers()) {
            resourceManagerIdentifiers.add(resourceManager.getNodeIdentifier());
        }
        assertTrue(resourceManagerIdentifiers.contains(client.getLeaderId().toString()));
    }
}
