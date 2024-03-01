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
import com.facebook.presto.execution.scheduler.InternalNodeInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DynamicBucketNodeMap
        extends BucketNodeMap
{
    private final int bucketCount;
    private final Int2ObjectMap<InternalNodeInfo> bucketToNodeInfo = new Int2ObjectOpenHashMap<>();
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
            bucketToNodeInfo.put(bucketNumber, new InternalNodeInfo(bucketToPreferredNode.get(bucketNumber), true));
        }
        this.bucketCount = bucketCount;
        this.hasInitialMap = true;
    }

    @Override
    public Optional<InternalNode> getAssignedNode(int bucketedId)
    {
        if (!bucketToNodeInfo.containsKey(bucketedId)) {
            return Optional.empty();
        }
        return Optional.of(bucketToNodeInfo.get(bucketedId).getInternalNode());
    }

    @Override
    public boolean isBucketCacheable(int bucketedId)
    {
        if (!bucketToNodeInfo.containsKey(bucketedId)) {
            return false;
        }
        return bucketToNodeInfo.get(bucketedId).isCacheable();
    }

    @Override
    public int getBucketCount()
    {
        return bucketCount;
    }

    @Override
    public void assignOrUpdateBucketToNode(int bucketedId, InternalNode node, boolean cacheable)
    {
        checkArgument(bucketedId >= 0 && bucketedId < bucketCount);
        requireNonNull(node, "node is null");
        bucketToNodeInfo.put(bucketedId, new InternalNodeInfo(node, cacheable));
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
        if (bucketToNodeInfo.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(bucketToNodeInfo.values().stream().map(InternalNodeInfo::getInternalNode).collect(Collectors.toList()));
    }
}
