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
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

public class NodePartitionMap
{
    private final Map<Integer, Node> partitionToNode;
    private final int[] bucketToPartition;
    private final ToIntFunction<Split> splitToBucket;

    public NodePartitionMap(Map<Integer, Node> partitionToNode, ToIntFunction<Split> splitToBucket)
    {
        this.partitionToNode = ImmutableMap.copyOf(requireNonNull(partitionToNode, "partitionToNode is null"));

        this.bucketToPartition = IntStream.range(0, partitionToNode.size()).toArray();
        this.splitToBucket = requireNonNull(splitToBucket, "splitToBucket is null");
    }

    public NodePartitionMap(Map<Integer, Node> partitionToNode, int[] bucketToPartition, ToIntFunction<Split> splitToBucket)
    {
        this.bucketToPartition = requireNonNull(bucketToPartition, "bucketToPartition is null");
        this.partitionToNode = ImmutableMap.copyOf(requireNonNull(partitionToNode, "partitionToNode is null"));
        this.splitToBucket = requireNonNull(splitToBucket, "splitToBucket is null");
    }

    public Map<Integer, Node> getPartitionToNode()
    {
        return partitionToNode;
    }

    public int[] getBucketToPartition()
    {
        return bucketToPartition;
    }

    public Node getNode(Split split)
    {
        int bucket = splitToBucket.applyAsInt(split);
        int partition = bucketToPartition[bucket];
        return requireNonNull(partitionToNode.get(partition));
    }
}
