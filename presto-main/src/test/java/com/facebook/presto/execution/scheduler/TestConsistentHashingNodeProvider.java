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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestConsistentHashingNodeProvider
{
    @Test
    public void testDistribution()
    {
        List<InternalNode> nodes = IntStream.range(0, 10).mapToObj(i -> new InternalNode(format("other%d", i), URI.create(format("http://127.0.0.%d:100", i)), NodeVersion.UNKNOWN, false)).collect(toImmutableList());
        ConsistentHashingNodeProvider nodeProvider = ConsistentHashingNodeProvider.create(nodes, 100);
        Random random = new Random();
        Map<HostAddress, Integer> result = new HashMap<>();
        for (int i = 0; i < 1_000_000; i++) {
            List<HostAddress> candidates = nodeProvider.get(format("split%d", random.nextInt()), 2);
            assertNotEquals(candidates.get(1), candidates.get(0));
            HostAddress hostAddress = candidates.get(0);
            int count = result.getOrDefault(hostAddress, 0);
            result.put(hostAddress, count + 1);
        }
        assertTrue(result.values().stream().allMatch(count -> count >= 80000 && count <= 120000));
    }

    @Test
    public void testMultipleCandidates()
    {
        List<InternalNode> nodes = IntStream.range(0, 10).mapToObj(i -> new InternalNode(format("other%d", i), URI.create(format("http://127.0.0.%d:100", i)), NodeVersion.UNKNOWN, false)).collect(toImmutableList());
        ConsistentHashingNodeProvider nodeProvider = ConsistentHashingNodeProvider.create(nodes, 1);
        assertEquals(ImmutableSet.copyOf(nodeProvider.get("split1", 10)), nodes.stream().map(InternalNode::getHostAndPort).collect(toImmutableSet()));
        assertEquals(ImmutableSet.copyOf(nodeProvider.get("split1", 11)), nodes.stream().map(InternalNode::getHostAndPort).collect(toImmutableSet()));

        ConsistentHashingNodeProvider nodeProviderWithWeight = ConsistentHashingNodeProvider.create(nodes, 100);
        assertEquals(ImmutableSet.copyOf(nodeProvider.get("split1", 10)), nodes.stream().map(InternalNode::getHostAndPort).collect(toImmutableSet()));
    }
}
