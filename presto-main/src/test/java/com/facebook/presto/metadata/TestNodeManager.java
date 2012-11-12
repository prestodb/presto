package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableList;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.testing.StaticServiceSelector;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertNotSame;

public class TestNodeManager
{
    @Test
    public void testGetActiveNodes()
            throws Exception
    {
        List<Node> nodes = ImmutableList.of(
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

        NodeManager manager = new NodeManager(new StaticServiceSelector(descriptors));
        Set<Node> activeNodes = manager.getActiveNodes();

        assertEqualsIgnoreOrder(activeNodes, nodes);

        for (Node actual : activeNodes) {
            for (Node expected : nodes) {
                assertNotSame(actual, expected);
            }
        }
    }
}
