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
package com.facebook.presto.spi.hashing;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.math.Quantiles.percentiles;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestConsistentHashSelector
{
    private ConsistentHashSelector<TestNode> hashSelector;

    @BeforeMethod
    public void setUp()
    {
        ImmutableList.Builder<TestNode> nodeBuilder = ImmutableList.builder();
        for (int i = 1; i < 10; i++) {
            nodeBuilder.add(new TestNode("node" + i));
        }
        List<TestNode> nodes = nodeBuilder.build();
        hashSelector = new ConsistentHashSelector<>(nodes, 10, TestNode::getNodeIdentifier);
    }

    @Test
    public void testInsertNodes()
    {
        assertEquals(hashSelector.getN("key", 3).size(), 3);
        Map<String, TestNode> mapping = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            String key = "key" + i;
            mapping.put(key, hashSelector.get(key));
        }
        hashSelector.add(new TestNode("newNode"));
        int count = 0;
        for (int i = 0; i < 100; i++) {
            String key = "key" + i;
            TestNode previousNode = mapping.get(key);
            TestNode currentNode = hashSelector.get(key);
            if (previousNode != currentNode) {
                count++;
            }
        }
        assertTrue(count <= 10);
    }

    @Test
    public void testRemoveNodes()
    {
        Map<String, TestNode> mapping = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            String key = "key" + i;
            mapping.put(key, hashSelector.get(key));
        }
        TestNode selectedNode = hashSelector.get("key1");
        hashSelector.remove(selectedNode);
        for (int i = 0; i < 100; i++) {
            String key = "key" + i;
            TestNode previousNode = mapping.get(key);
            TestNode currentNode = hashSelector.get(key);
            if (previousNode == selectedNode) {
                assertNotEquals(currentNode, previousNode);
            }
            else {
                assertEquals(currentNode, previousNode);
            }
        }
    }

    @Test
    public void testStress()
    {
        Map<Integer, Double> result10 = runStressWith(400, 100_000, 10);
        Map<Integer, Double> result30 = runStressWith(400, 100_000, 30);
        Map<Integer, Double> result60 = runStressWith(400, 100_000, 60);
        assertTrue(Math.abs(result60.get(50) - 250) <= Math.abs(result30.get(50) - 250));
        assertTrue(Math.abs(result30.get(50) - 250) <= Math.abs(result10.get(50) - 250));
    }

    private Map<Integer, Double> runStressWith(int nodeNumber, int splits, int replicas)
    {
        ImmutableList.Builder<TestNode> nodeBuilder = ImmutableList.builder();
        for (int i = 1; i < nodeNumber; i++) {
            nodeBuilder.add(new TestNode("node" + i));
        }
        List<TestNode> nodes = nodeBuilder.build();
        hashSelector = new ConsistentHashSelector<>(nodes, replicas, TestNode::getNodeIdentifier);

        Map<TestNode, Integer> mapping = new HashMap<>();
        for (int i = 0; i < splits; i++) {
            String key = "key" + i;
            TestNode node = hashSelector.get(key);
            mapping.put(node, mapping.getOrDefault(node, 0) + 1);
        }
        Map<Integer, Double> myPercentiles =
                percentiles().indexes(50, 90, 99).compute(mapping.values());
        return myPercentiles;
    }

    private static class TestNode
            implements Node
    {
        private String id;

        public TestNode(String id)
        {
            this.id = id;
        }

        @Override
        public String getHost()
        {
            return null;
        }

        @Override
        public HostAddress getHostAndPort()
        {
            return null;
        }

        @Override
        public URI getHttpUri()
        {
            return null;
        }

        @Override
        public String getNodeIdentifier()
        {
            return id;
        }

        @Override
        public String getVersion()
        {
            return "UNKNOWN";
        }

        @Override
        public boolean isCoordinator()
        {
            return false;
        }

        @Override
        public boolean isResourceManager()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return id;
        }
    }
}
