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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;

public class TestJmxSplitManager
{
    private final Node localNode = new TestingNode("host1");
    private final Set<Node> nodes = ImmutableSet.of(localNode, new TestingNode("host2"), new TestingNode("host3"));

    private final JmxColumnHandle columnHandle = new JmxColumnHandle("test", "node", createUnboundedVarcharType());
    private final JmxTableHandle tableHandle = new JmxTableHandle("test", "objectName", ImmutableList.of(columnHandle));
    private final JmxSplitManager splitManager = new JmxSplitManager("test", new TestingNodeManager());
    private final JmxMetadata metadata = new JmxMetadata("test", getPlatformMBeanServer());
    private final JmxRecordSetProvider recordSetProvider = new JmxRecordSetProvider(getPlatformMBeanServer(), localNode.getNodeIdentifier());

    @Test
    public void testPredicatePushdown()
            throws Exception
    {
        for (Node node : nodes) {
            String nodeIdentifier = node.getNodeIdentifier();
            TupleDomain<ColumnHandle> nodeTupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(columnHandle, NullableValue.of(createUnboundedVarcharType(), utf8Slice(nodeIdentifier))));
            ConnectorTableLayoutHandle layout = new JmxTableLayoutHandle(tableHandle, nodeTupleDomain);

            ConnectorSplitSource splitSource = splitManager.getSplits(JmxTransactionHandle.INSTANCE, SESSION, layout);
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
        ConnectorTableLayoutHandle layout = new JmxTableLayoutHandle(tableHandle, TupleDomain.all());
        ConnectorSplitSource splitSource = splitManager.getSplits(JmxTransactionHandle.INSTANCE, SESSION, layout);
        List<ConnectorSplit> allSplits = getAllSplits(splitSource);
        assertEquals(allSplits.size(), nodes.size());

        Set<String> actualNodes = nodes.stream().map(Node::getNodeIdentifier).collect(toSet());
        Set<String> expectedNodes = new HashSet<>();
        for (ConnectorSplit split : allSplits) {
            List<HostAddress> addresses = split.getAddresses();
            assertEquals(addresses.size(), 1);
            expectedNodes.add(addresses.get(0).getHostText());
        }
        assertEquals(actualNodes, expectedNodes);
    }

    @Test
    public void testRecordSetProvider()
            throws Exception
    {
        for (SchemaTableName schemaTableName : metadata.listTables(SESSION, "jmx")) {
            JmxTableHandle tableHandle = metadata.getTableHandle(SESSION, schemaTableName);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

            ConnectorTableLayoutHandle layout = new JmxTableLayoutHandle(tableHandle, TupleDomain.all());
            ConnectorSplitSource splitSource = splitManager.getSplits(JmxTransactionHandle.INSTANCE, SESSION, layout);
            List<ConnectorSplit> allSplits = getAllSplits(splitSource);
            assertEquals(allSplits.size(), nodes.size());
            ConnectorSplit split = allSplits.get(0);

            RecordSet recordSet = recordSetProvider.getRecordSet(JmxTransactionHandle.INSTANCE, SESSION, split, columnHandles);
            try (RecordCursor cursor = recordSet.cursor()) {
                while (cursor.advanceNextPosition()) {
                    for (int i = 0; i < recordSet.getColumnTypes().size(); i++) {
                        cursor.isNull(i);
                    }
                }
            }
        }
    }

    private static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException, ExecutionException
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> batch = splitSource.getNextBatch(1000).get();
            splits.addAll(batch);
        }
        return splits.build();
    }

    private class TestingNodeManager
            implements NodeManager
    {
        @Override
        public Set<Node> getNodes(NodeState state)
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
