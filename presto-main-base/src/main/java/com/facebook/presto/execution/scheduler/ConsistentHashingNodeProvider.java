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
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.hash.Hashing.murmur3_32;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class ConsistentHashingNodeProvider
{
    private static final HashFunction HASH_FUNCTION = murmur3_32();
    private final NavigableMap<Integer, InternalNode> candidates;
    private final int nodeCount;

    static ConsistentHashingNodeProvider create(Collection<InternalNode> nodes, int weight)
    {
        NavigableMap<Integer, InternalNode> activeNodesByConsistentHashing = new TreeMap<>();
        for (InternalNode node : nodes) {
            for (int i = 0; i < weight; i++) {
                activeNodesByConsistentHashing.put(HASH_FUNCTION.hashString(format("%s%d", node.getNodeIdentifier(), i), UTF_8).asInt(), node);
            }
        }
        return new ConsistentHashingNodeProvider(activeNodesByConsistentHashing, nodes.size());
    }

    private ConsistentHashingNodeProvider(NavigableMap<Integer, InternalNode> candidates, int nodeCount)
    {
        this.candidates = requireNonNull(candidates, "candidates is null");
        this.nodeCount = nodeCount;
    }

    public List<HostAddress> get(String key, int count)
    {
        if (count > nodeCount) {
            count = nodeCount;
        }

        Set<HostAddress> uniqueNodes = new LinkedHashSet<>();
        int hashKey = HASH_FUNCTION.hashString(key, UTF_8).asInt();

        SortedMap<Integer, InternalNode> tailMap = candidates.tailMap(hashKey);
        //Start reading from tail
        for (Map.Entry<Integer, InternalNode> entry : tailMap.entrySet()) {
            uniqueNodes.add(entry.getValue().getHostAndPort());
            if (uniqueNodes.size() == count) {
                break;
            }
        }

        if (uniqueNodes.size() < count) {
            //Start reading from the head as we have exhausted tail
            SortedMap<Integer, InternalNode> headMap = candidates.headMap(hashKey);
            for (Map.Entry<Integer, InternalNode> entry : headMap.entrySet()) {
                uniqueNodes.add(entry.getValue().getHostAndPort());
                if (uniqueNodes.size() == count) {
                    break;
                }
            }
        }

        return ImmutableList.copyOf(uniqueNodes);
    }
}
