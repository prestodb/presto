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
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static com.facebook.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static com.facebook.airlift.discovery.client.ServiceSelectorConfig.DEFAULT_POOL;
import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestInternalNodeSupply
{
    private final NodeInfo nodeInfo = new NodeInfo("test");
    private final InternalCommunicationConfig internalCommunicationConfig = new InternalCommunicationConfig();
    private NodeVersion expectedVersion;
    private Set<InternalNode> activeNodes;
    private Set<InternalNode> inactiveNodes;
    private InternalNode coordinator;
    private InternalNode currentNode;
    private final PrestoNodeServiceSelector selector = new PrestoNodeServiceSelector();
    private HttpClient testHttpClient;
    private InternalNodeSupply internalNodeSupply1;
    private InternalNodeSupply internalNodeSupply2;

    @BeforeMethod
    public void setup()
    {
        testHttpClient = new TestingHttpClient(input -> new TestingResponse(OK, ArrayListMultimap.create(), ACTIVE.name().getBytes()));
        internalNodeSupply1 = new InternalNodeSupply(OptionalLong.of(1_000_000_000L), OptionalInt.of(18));
        internalNodeSupply2 = new InternalNodeSupply(OptionalLong.of(1_000_000_000L), OptionalInt.of(24));

        expectedVersion = new NodeVersion("1");
        coordinator = new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.2.8"), OptionalInt.empty(), expectedVersion, true, Optional.of(internalNodeSupply1));
        currentNode = new InternalNode(nodeInfo.getNodeId(), URI.create("http://192.0.1.1"), OptionalInt.empty(), expectedVersion, false, Optional.of(internalNodeSupply1));

        activeNodes = ImmutableSet.of(
                currentNode,
                new InternalNode(UUID.randomUUID().toString(), URI.create("http://192.0.2.1:8080"), OptionalInt.empty(), expectedVersion, false, Optional.of(internalNodeSupply1)),
                new InternalNode(UUID.randomUUID().toString(), URI.create("http://192.0.2.3"), OptionalInt.empty(), expectedVersion, false, Optional.of(internalNodeSupply1)),
                coordinator);
        inactiveNodes = ImmutableSet.of(
                new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.3.9"), OptionalInt.empty(), NodeVersion.UNKNOWN, false, Optional.of(internalNodeSupply2)),
                new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.4.9"), OptionalInt.empty(), new NodeVersion("2"), false, Optional.of(internalNodeSupply2)));

        selector.announceNodes(activeNodes, inactiveNodes);
    }

    @Test
    public void testInternalNodeSupply()
    {
        DiscoveryNodeManager manager = new DiscoveryNodeManager(selector, nodeInfo, new NoOpFailureDetector(), Optional.empty(), expectedVersion, testHttpClient, new TestingDriftClient<>(), internalCommunicationConfig);
        try {
            AllNodes allNodes = manager.getAllNodes();
            Set<InternalNode> activeNodes = allNodes.getActiveNodes();

            for (InternalNode actual : activeNodes) {
                assertTrue(actual.getInternalNodeResource().isPresent());
                assertEquals(actual.getInternalNodeResource().get(), internalNodeSupply1);
            }

            Set<InternalNode> inactiveNodes = allNodes.getInactiveNodes();

            for (InternalNode actual : inactiveNodes) {
                assertTrue(actual.getInternalNodeResource().isPresent());
                assertEquals(actual.getInternalNodeResource().get(), internalNodeSupply2);
            }
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
                        .addProperty("coordinator", String.valueOf(node.isCoordinator()))
                        .addProperty("generalPoolCapacityInBytes", String.valueOf(node.getInternalNodeResource().get().getGeneralPoolCapacityInBytes().getAsLong()))
                        .addProperty("numberOfCpuCores", String.valueOf(node.getInternalNodeResource().get().getNumberOfCpuCores().getAsInt()))
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
