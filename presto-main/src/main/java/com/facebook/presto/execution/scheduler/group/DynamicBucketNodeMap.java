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
package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.checkerframework.checker.nullness.Opt;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DynamicBucketNodeMap
        extends BucketNodeMap
{
    private final int bucketCount;
    private final Int2ObjectMap<InternalNode> bucketToNode = new Int2ObjectOpenHashMap<>();
    private final boolean hasInitialMap;

    public DynamicBucketNodeMap(ToIntFunction<Split> splitToBucket, int bucketCount)
    {
        super(splitToBucket);
        checkArgument(bucketCount > 0, "bucketCount must be positive");
        this.bucketCount = bucketCount;
        hasInitialMap = false;
    }

    public DynamicBucketNodeMap(ToIntFunction<Split> splitToBucket, int bucketCount, List<InternalNode> bucketToPreferredNode)
    {
        super(splitToBucket);
        checkArgument(bucketCount > 0, "bucketCount must be positive");
        checkArgument(bucketToPreferredNode.size() == bucketCount, "bucketToPreferredNode size must be equal to bucketCount");
        for (int bucketNumber = 0; bucketNumber < bucketCount; bucketNumber++) {
            bucketToNode.put(bucketNumber, bucketToPreferredNode.get(bucketNumber));
        }
        this.bucketCount = bucketCount;
        this.hasInitialMap = true;
    }

    @Override
    public Optional<InternalNode> getAssignedNode(int bucketedId)
    {
        return Optional.ofNullable(bucketToNode.get(bucketedId));
    }

    @Override
    public int getBucketCount()
    {
        return bucketCount;
    }

    @Override
    public void assignOrUpdateBucketToNode(int bucketedId, InternalNode node)
    {
        checkArgument(bucketedId >= 0 && bucketedId < bucketCount);
        requireNonNull(node, "node is null");
        bucketToNode.put(bucketedId, node);
    }

    @Override
    public boolean isDynamic()
    {
        return true;
    }

    @Override
    public boolean hasInitialMap()
    {
        return hasInitialMap;
    }

    @Override
    public Optional<List<InternalNode>> getBucketToNode()
    {
        if (bucketToNode.size() == 0) {
            return Optional.empty();
        }
        return Optional.of(ImmutableList.copyOf(bucketToNode.values()));
    }
}
