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
package com.facebook.presto.split;

import com.facebook.presto.connector.jmx.JmxColumnHandle;
import com.facebook.presto.connector.jmx.JmxConnectorId;
import com.facebook.presto.connector.jmx.JmxSplitManager;
import com.facebook.presto.connector.jmx.JmxTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestJmxSplitManager
{
    private static final String CONNECTOR_ID = "test_connector";

    private final Node localNode = new TestingNode("host1");
    private final Set<Node> nodes = ImmutableSet.of(localNode, new TestingNode("host2"), new TestingNode("host3"));

    private final JmxColumnHandle columnHandle = new JmxColumnHandle(CONNECTOR_ID, "node", VARCHAR, 0);
    private final JmxTableHandle tableHandle = new JmxTableHandle(CONNECTOR_ID, "objectName", ImmutableList.of(columnHandle));
    private final JmxSplitManager splitManager = new JmxSplitManager(new JmxConnectorId(CONNECTOR_ID), new TestingNodeManager());

    @Test
    public void testPredicatePushdown()
            throws Exception
    {
        for (Node node : nodes) {
            String nodeIdentifier = node.getNodeIdentifier();
            TupleDomain<ColumnHandle> nodeTupleDomain = TupleDomain.withFixedValues(ImmutableMap.of(columnHandle, Slices.utf8Slice(nodeIdentifier)));

            ConnectorPartitionResult connectorPartitionResult = splitManager.getPartitions(tableHandle, nodeTupleDomain);
            ConnectorSplitSource splitSource = splitManager.getPartitionSplits(tableHandle, connectorPartitionResult.getPartitions());
            List<ConnectorSplit> allSplits = getAllSplits(splitSource);

            assertEquals(allSplits.size(), 1);
            assertEquals(allSplits.get(0).getAddresses().size(), 1);
            assertEquals(allSplits.get(0).getAddresses().get(0).getHostText(), nodeIdentifier);
        }
    }

    @Test
    public void testNoPredicate()
            throws Exception
    {
        ConnectorPartitionResult connectorPartitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        ConnectorSplitSource splitSource = splitManager.getPartitionSplits(tableHandle, connectorPartitionResult.getPartitions());
        List<ConnectorSplit> allSplits = getAllSplits(splitSource);
        assertEquals(allSplits.size(), nodes.size());

        Set<String> actualNodes = nodes.stream().map(Node::getNodeIdentifier).collect(toImmutableSet());
        Set<String> expectedNodes = new HashSet<>();
        for (ConnectorSplit split : allSplits) {
            List<HostAddress> addresses = split.getAddresses();
            assertEquals(addresses.size(), 1);
            expectedNodes.add(addresses.get(0).getHostText());
        }
        assertEquals(actualNodes, expectedNodes);
    }

    private static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> batch = getFutureValue(splitSource.getNextBatch(1000));
            splits.addAll(batch);
        }
        return splits.build();
    }

    private class TestingNodeManager
            implements NodeManager
    {
        @Override
        public Set<Node> getActiveNodes()
        {
            return nodes;
        }

        @Override
        public Set<Node> getActiveDatasourceNodes(String datasourceName)
        {
            return nodes;
        }

        @Override
        public Node getCurrentNode()
        {
            return localNode;
        }

        @Override
        public Set<Node> getCoordinators()
        {
            return ImmutableSet.of(localNode);
        }
    }

    private static class TestingNode
            implements Node
    {
        private final String hostname;

        public TestingNode(String hostname)
        {
            this.hostname = hostname;
        }

        @Override
        public HostAddress getHostAndPort()
        {
            return HostAddress.fromParts(hostname, 8080);
        }

        @Override
        public URI getHttpUri()
        {
            return URI.create(format("http://%s:8080", hostname));
        }

        @Override
        public String getNodeIdentifier()
        {
            return hostname;
        }
    }
}
