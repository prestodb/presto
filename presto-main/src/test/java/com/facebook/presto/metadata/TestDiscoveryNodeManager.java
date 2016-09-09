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

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.server.NoOpFailureDetector;
import com.facebook.presto.spi.Node;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.testing.StaticServiceSelector;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.INACTIVE;
import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

@Test(singleThreaded = true)
public class TestDiscoveryNodeManager
{
    private final NodeInfo nodeInfo = new NodeInfo("test");
    private NodeVersion expectedVersion;
    private List<PrestoNode> activeNodes;
    private List<PrestoNode> inactiveNodes;
    private PrestoNode coordinator;
    private ServiceSelector selector;
    private HttpClient testHttpClient;

    @BeforeMethod
    public void setup()
    {
        testHttpClient = new TestingHttpClient(input -> new TestingResponse(OK, ArrayListMultimap.create(), ACTIVE.name().getBytes()));

        expectedVersion = new NodeVersion("1");
        coordinator = new PrestoNode(UUID.randomUUID().toString(), URI.create("https://192.0.2.8"), expectedVersion, false);
        activeNodes = ImmutableList.of(
                new PrestoNode(nodeInfo.getNodeId(), URI.create("http://192.0.1.1"), expectedVersion, false),
                new PrestoNode(UUID.randomUUID().toString(), URI.create("http://192.0.2.1:8080"), expectedVersion, false),
                new PrestoNode(UUID.randomUUID().toString(), URI.create("http://192.0.2.3"), expectedVersion, false),
                coordinator);
        inactiveNodes = ImmutableList.of(
                new PrestoNode(UUID.randomUUID().toString(), URI.create("https://192.0.3.9"), NodeVersion.UNKNOWN, false),
                new PrestoNode(UUID.randomUUID().toString(), URI.create("https://192.0.4.9"), new NodeVersion("2"), false)
        );

        List<ServiceDescriptor> descriptors = new ArrayList<>();
        for (PrestoNode node : Iterables.concat(activeNodes, inactiveNodes)) {
            descriptors.add(serviceDescriptor("presto")
                    .setNodeId(node.getNodeIdentifier())
                    .addProperty("http", node.getHttpUri().toString())
                    .addProperty("node_version", node.getNodeVersion().toString())
                    .addProperty("coordinator", String.valueOf(node.equals(coordinator)))
                    .build());
        }

        selector = new StaticServiceSelector(descriptors);
    }

    @Test
    public void testGetAllNodes()
            throws Exception
    {
        DiscoveryNodeManager manager = new DiscoveryNodeManager(selector, nodeInfo, new NoOpFailureDetector(), expectedVersion, testHttpClient);
        AllNodes allNodes = manager.getAllNodes();

        Set<Node> activeNodes = allNodes.getActiveNodes();
        assertEqualsIgnoreOrder(activeNodes, this.activeNodes);

        for (Node actual : activeNodes) {
            for (Node expected : this.activeNodes) {
                assertNotSame(actual, expected);
            }
        }

        assertEqualsIgnoreOrder(activeNodes, manager.getNodes(ACTIVE));

        Set<Node> inactiveNodes = allNodes.getInactiveNodes();
        assertEqualsIgnoreOrder(inactiveNodes, this.inactiveNodes);

        for (Node actual : inactiveNodes) {
            for (Node expected : this.inactiveNodes) {
                assertNotSame(actual, expected);
            }
        }

        assertEqualsIgnoreOrder(inactiveNodes, manager.getNodes(INACTIVE));
    }

    @Test
    public void testGetCurrentNode()
    {
        Node expected = activeNodes.get(0);

        NodeInfo nodeInfo = new NodeInfo(new NodeConfig()
                .setEnvironment("test")
                .setNodeId(expected.getNodeIdentifier()));

        DiscoveryNodeManager manager = new DiscoveryNodeManager(selector, nodeInfo, new NoOpFailureDetector(), expectedVersion, testHttpClient);

        assertEquals(manager.getCurrentNode(), expected);
    }

    @Test
    public void testGetCoordinators()
            throws Exception
    {
        InternalNodeManager manager = new DiscoveryNodeManager(selector, nodeInfo, new NoOpFailureDetector(), expectedVersion, testHttpClient);
        assertEquals(manager.getCoordinators(), ImmutableSet.of(coordinator));
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".* current node not returned .*")
    public void testGetCurrentNodeRequired()
    {
        new DiscoveryNodeManager(selector, new NodeInfo("test"), new NoOpFailureDetector(), expectedVersion, testHttpClient);
    }
}
