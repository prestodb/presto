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
package com.facebook.presto.metadata;

import com.facebook.airlift.discovery.client.ServiceDescriptor;
import com.facebook.airlift.discovery.client.ServiceSelector;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.http.client.testing.TestingResponse;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.failureDetector.NoOpFailureDetector;
import com.facebook.presto.operator.TestingDriftClient;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.ServerConfig;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.facebook.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static com.facebook.airlift.discovery.client.ServiceSelectorConfig.DEFAULT_POOL;
import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.server.ServerConfig.POOL_TYPE;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.INACTIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

public class TestDiscoveryNodeManagerWithSidecar
{
    private final NodeInfo workerNodeInfo = new NodeInfo("test");
    private final NodeInfo coordinatorSidecarNodeInfo = new NodeInfo("test");

    private final InternalCommunicationConfig internalCommunicationConfig = new InternalCommunicationConfig();
    private NodeVersion expectedVersion;
    private Set<InternalNode> activeNodes;
    private Set<InternalNode> workerNodes;
    private Set<InternalNode> inactiveNodes;
    private InternalNode coordinatorSidecar;
    private InternalNode inActiveCoordinatorSidecar;
    private InternalNode workerNode1;
    private final PrestoNodeServiceSelector selector = new PrestoNodeServiceSelector();
    private HttpClient testHttpClient;
    private InternalNode workerNode2;
    private InternalNode workerNode3;
    private InternalNode inActiveWorkerNode1;
    private InternalNode inActiveWorkerNode2;
    private final ServerConfig serverConfig = new ServerConfig().setNativeExecutionEnabled(true);

    @BeforeMethod
    public void setup()
    {
        testHttpClient = new TestingHttpClient(input -> new TestingResponse(OK, ArrayListMultimap.create(), ACTIVE.name().getBytes()));

        expectedVersion = new NodeVersion("1");
        coordinatorSidecar = new InternalNode(
                coordinatorSidecarNodeInfo.getNodeId(),
                URI.create("https://192.0.3.1"),
                expectedVersion,
                false,
                false,
                false,
                true);
        workerNode1 = new InternalNode(workerNodeInfo.getNodeId(), URI.create("http://192.0.1.1"), expectedVersion, false);
        workerNode2 = new InternalNode(UUID.randomUUID().toString(), URI.create("http://192.0.2.1:8080"), expectedVersion, false);
        workerNode3 = new InternalNode(UUID.randomUUID().toString(), URI.create("http://192.0.2.3"), expectedVersion, false);
        inActiveCoordinatorSidecar = new InternalNode(
                coordinatorSidecarNodeInfo.getNodeId(),
                URI.create("https://192.0.3.2"),
                new NodeVersion("2"),
                false,
                false,
                false,
                true);
        inActiveWorkerNode1 = new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.3.9"), NodeVersion.UNKNOWN, false);
        inActiveWorkerNode2 = new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.4.9"), new NodeVersion("2"), false);

        workerNodes = ImmutableSet.of(workerNode1, workerNode2, workerNode3);
        activeNodes = ImmutableSet.<InternalNode>builder()
                .addAll(workerNodes)
                .add(coordinatorSidecar)
                .build();

        inactiveNodes = ImmutableSet.of(
                inActiveCoordinatorSidecar,
                inActiveWorkerNode1,
                inActiveWorkerNode2);

        selector.announceNodes(activeNodes, inactiveNodes);
    }

    @Test
    public void testGetAllNodesForWorkerNode()
    {
        DiscoveryNodeManager manager = new DiscoveryNodeManager(selector, workerNodeInfo, new NoOpFailureDetector(), Optional.empty(), expectedVersion, testHttpClient, new TestingDriftClient<>(), internalCommunicationConfig, serverConfig);
        try {
            AllNodes allNodes = manager.getAllNodes();

            Set<InternalNode> activeNodes = allNodes.getActiveNodes();
            assertEqualsIgnoreOrder(activeNodes, ImmutableSet.of(coordinatorSidecar));

            for (InternalNode actual : activeNodes) {
                for (InternalNode expected : this.activeNodes) {
                    assertNotSame(actual, expected);
                }
            }

            assertEqualsIgnoreOrder(activeNodes, manager.getNodes(ACTIVE));

            Set<InternalNode> inactiveNodes = allNodes.getInactiveNodes();
            assertEqualsIgnoreOrder(inactiveNodes, ImmutableSet.of(inActiveCoordinatorSidecar));

            for (InternalNode actual : inactiveNodes) {
                for (InternalNode expected : this.inactiveNodes) {
                    assertNotSame(actual, expected);
                }
            }

            assertEqualsIgnoreOrder(inactiveNodes, manager.getNodes(INACTIVE));
        }
        finally {
            manager.stop();
        }
    }

    @Test
    public void testGetAllNodesForCoordinatorSidecar()
    {
        DiscoveryNodeManager manager = new DiscoveryNodeManager(selector, coordinatorSidecarNodeInfo, new NoOpFailureDetector(), Optional.empty(), expectedVersion, testHttpClient, new TestingDriftClient<>(), internalCommunicationConfig, serverConfig);
        try {
            AllNodes allNodes = manager.getAllNodes();

            Set<InternalNode> activeNodes = allNodes.getActiveNodes();
            assertEqualsIgnoreOrder(activeNodes, this.activeNodes);

            for (InternalNode actual : activeNodes) {
                for (InternalNode expected : this.activeNodes) {
                    assertNotSame(actual, expected);
                }
            }

            assertEqualsIgnoreOrder(activeNodes, manager.getNodes(ACTIVE));

            Set<InternalNode> inactiveNodes = allNodes.getInactiveNodes();
            assertEqualsIgnoreOrder(inactiveNodes, this.inactiveNodes);

            for (InternalNode actual : inactiveNodes) {
                for (InternalNode expected : this.inactiveNodes) {
                    assertNotSame(actual, expected);
                }
            }

            assertEqualsIgnoreOrder(inactiveNodes, manager.getNodes(INACTIVE));
        }
        finally {
            manager.stop();
        }
    }

    @Test
    public void testGetCoordinatorSidecar()
    {
        DiscoveryNodeManager manager = new DiscoveryNodeManager(selector, coordinatorSidecarNodeInfo, new NoOpFailureDetector(), Optional.of(host -> false), expectedVersion, testHttpClient, new TestingDriftClient<>(), internalCommunicationConfig, serverConfig);
        try {
            assertEquals(manager.getCoordinatorSidecars(), ImmutableSet.of(coordinatorSidecar));
        }
        finally {
            manager.stop();
        }
    }

    public static class PrestoNodeServiceSelector
            implements ServiceSelector
    {
        @GuardedBy("this")
        private List<ServiceDescriptor> descriptors = ImmutableList.of();

        private synchronized void announceNodes(Set<InternalNode> activeNodes, Set<InternalNode> inactiveNodes)
        {
            ImmutableList.Builder<ServiceDescriptor> descriptors = ImmutableList.builder();
            for (InternalNode node : Iterables.concat(activeNodes, inactiveNodes)) {
                descriptors.add(serviceDescriptor("presto")
                        .setNodeId(node.getNodeIdentifier())
                        .addProperty("http", node.getInternalUri().toString())
                        .addProperty("node_version", node.getNodeVersion().toString())
                        .addProperty("sidecar", String.valueOf(node.isCoordinatorSidecar()))
                        .addProperty(POOL_TYPE, node.getPoolType().name())
                        .build());
            }

            this.descriptors = descriptors.build();
        }

        @Override
        public String getType()
        {
            return "presto";
        }

        @Override
        public String getPool()
        {
            return DEFAULT_POOL;
        }

        @Override
        public synchronized List<ServiceDescriptor> selectAllServices()
        {
            return descriptors;
        }

        @Override
        public ListenableFuture<List<ServiceDescriptor>> refresh()
        {
            throw new UnsupportedOperationException();
        }
    }
}
