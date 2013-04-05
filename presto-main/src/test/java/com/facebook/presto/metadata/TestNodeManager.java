package com.facebook.presto.metadata;

import com.facebook.presto.server.FailureDetector;
import com.facebook.presto.server.NoOpFailureDetector;
import com.google.common.collect.ImmutableList;
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

public class TestNodeManager
{
    private List<Node> nodes;
    private ServiceSelector selector;

    @BeforeMethod
    public void setup()
    {
        nodes = ImmutableList.of(
                new Node(UUID.randomUUID().toString(), URI.create("http://192.0.2.1:8080")),
                new Node(UUID.randomUUID().toString(), URI.create("http://192.0.2.3")),
                new Node(UUID.randomUUID().toString(), URI.create("https://192.0.2.8")));

        List<ServiceDescriptor> descriptors = new ArrayList<>();
        for (Node node : nodes) {
            descriptors.add(serviceDescriptor("presto")
                    .setNodeId(node.getNodeIdentifier())
                    .addProperty("http", node.getHttpUri().toString())
                    .build());
        }

        selector = new StaticServiceSelector(descriptors);
    }

    @Test
    public void testGetActiveNodes()
            throws Exception
    {
        NodeManager manager = new NodeManager(selector, new NodeInfo("test"), new NoOpFailureDetector());
        Set<Node> activeNodes = manager.getActiveNodes();

        assertEqualsIgnoreOrder(activeNodes, nodes);

        for (Node actual : activeNodes) {
            for (Node expected : nodes) {
                assertNotSame(actual, expected);
            }
        }
    }

    @Test
    public void testGetCurrentNode()
    {
        Node expected = nodes.get(0);

        NodeInfo nodeInfo = new NodeInfo(new NodeConfig()
                .setEnvironment("test")
                .setNodeId(expected.getNodeIdentifier()));

        NodeManager manager = new NodeManager(selector, nodeInfo, new NoOpFailureDetector());

        assertEquals(manager.getCurrentNode().get(), expected);
    }

    public void testGetCurrentNodeNotActive()
    {
        NodeManager manager = new NodeManager(selector, new NodeInfo("test"), new NoOpFailureDetector());
        assertFalse(manager.getCurrentNode().isPresent());
    }
}
