package com.facebook.presto.metadata;

import com.facebook.presto.server.NoOpFailureDetector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.testing.StaticServiceSelector;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;

public class TestDiscoveryNodeManager
{
    private NodeVersion expectedVersion;
    private List<Node> activeNodes;
    private List<Node> inactiveNodes;
    private ServiceSelector selector;

    @BeforeMethod
    public void setup()
    {
        expectedVersion = new NodeVersion("1");
        activeNodes = ImmutableList.of(
                new Node(UUID.randomUUID().toString(), URI.create("http://192.0.2.1:8080"), expectedVersion),
                new Node(UUID.randomUUID().toString(), URI.create("http://192.0.2.3"), expectedVersion),
                new Node(UUID.randomUUID().toString(), URI.create("https://192.0.2.8"), expectedVersion));
        inactiveNodes = ImmutableList.of(
                new Node(UUID.randomUUID().toString(), URI.create("https://192.0.3.9"), NodeVersion.UNKNOWN),
                new Node(UUID.randomUUID().toString(), URI.create("https://192.0.4.9"), new NodeVersion("2"))
        );


        List<ServiceDescriptor> descriptors = new ArrayList<>();
        for (Node node : Iterables.concat(activeNodes, inactiveNodes)) {
            descriptors.add(serviceDescriptor("presto")
                    .setNodeId(node.getNodeIdentifier())
                    .addProperty("http", node.getHttpUri().toString())
                    .addProperty("node_version", node.getNodeVersion().toString())
                    .build());
        }

        selector = new StaticServiceSelector(descriptors);
    }

    @Test
    public void testGetAllNodes()
            throws Exception
    {
        DiscoveryNodeManager manager = new DiscoveryNodeManager(selector, new NodeInfo("test"), new NoOpFailureDetector(), expectedVersion);
        AllNodes allNodes = manager.getAllNodes();

        Set<Node> activeNodes = allNodes.getActiveNodes();
        assertEqualsIgnoreOrder(activeNodes, this.activeNodes);

        for (Node actual : activeNodes) {
            for (Node expected : this.activeNodes) {
                assertNotSame(actual, expected);
            }
        }

        Set<Node> inactiveNodes = allNodes.getInactiveNodes();
        assertEqualsIgnoreOrder(inactiveNodes, this.inactiveNodes);

        for (Node actual : inactiveNodes) {
            for (Node expected : this.inactiveNodes) {
                assertNotSame(actual, expected);
            }
        }
    }

    @Test
    public void testGetCurrentNode()
    {
        Node expected = activeNodes.get(0);

        NodeInfo nodeInfo = new NodeInfo(new NodeConfig()
                .setEnvironment("test")
                .setNodeId(expected.getNodeIdentifier()));

        DiscoveryNodeManager manager = new DiscoveryNodeManager(selector, nodeInfo, new NoOpFailureDetector(), expectedVersion);

        assertEquals(manager.getCurrentNode().get(), expected);
    }

    @Test
    public void testGetCurrentNodeNotActive()
    {
        DiscoveryNodeManager manager = new DiscoveryNodeManager(selector, new NodeInfo("test"), new NoOpFailureDetector(), expectedVersion);
        assertFalse(manager.getCurrentNode().isPresent());
    }
}
