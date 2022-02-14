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

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.facebook.presto.common.type.encoding.StringUtils.UTF_8;
import static com.google.common.hash.Hashing.murmur3_32;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConsistentHashingNodeProvider
        implements NodeProvider
{
    private static final HashFunction HASH_FUNCTION = murmur3_32();
    private final NavigableMap<Integer, InternalNode> candidates;
    private final int nodeCount;

    static ConsistentHashingNodeProvider create(Collection<InternalNode> nodes, int weight)
    {
        NavigableMap<Integer, InternalNode> activeNodesByConsistentHashing = new TreeMap<>();
        for (InternalNode node : nodes) {
            for (int i = 0; i < weight; i++) {
                activeNodesByConsistentHashing.put(murmur3_32().hashString(format("%s%d", node.getNodeIdentifier(), i), UTF_8).asInt(), node);
            }
        }
        return new ConsistentHashingNodeProvider(activeNodesByConsistentHashing, nodes.size());
    }

    private ConsistentHashingNodeProvider(NavigableMap<Integer, InternalNode> candidates, int nodeCount)
    {
        this.candidates = requireNonNull(candidates, "candidates is null");
        this.nodeCount = nodeCount;
    }

    @Override
    public List<HostAddress> get(String key, int count)
    {
        if (count > nodeCount) {
            count = nodeCount;
        }
        ImmutableList.Builder<HostAddress> nodes = ImmutableList.builder();
        Set<HostAddress> unique = new HashSet<>();
        int hashKey = HASH_FUNCTION.hashString(format("%s", key), UTF_8).asInt();
        Map.Entry<Integer, InternalNode> entry = candidates.ceilingEntry(hashKey);
        HostAddress candidate;
        SortedMap<Integer, InternalNode> nextEntries;
        if (entry != null) {
            candidate = entry.getValue().getHostAndPort();
            nextEntries = candidates.tailMap(entry.getKey(), false);
        }
        else {
            candidate = candidates.firstEntry().getValue().getHostAndPort();
            nextEntries = candidates.tailMap(candidates.firstKey(), false);
        }
        unique.add(candidate);
        nodes.add(candidate);
        while (unique.size() < count) {
            for (Map.Entry<Integer, InternalNode> next : nextEntries.entrySet()) {
                candidate = next.getValue().getHostAndPort();
                if (!unique.contains(candidate)) {
                    unique.add(candidate);
                    nodes.add(candidate);
                    if (unique.size() == count) {
                        break;
                    }
                }
            }
            nextEntries = candidates;
        }
        return nodes.build();
    }
}
