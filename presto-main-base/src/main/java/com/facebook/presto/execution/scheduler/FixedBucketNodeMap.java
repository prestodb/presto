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
import com.facebook.presto.metadata.Split;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static java.util.Objects.requireNonNull;

// the bucket to node mapping is fixed and pre-assigned
public class FixedBucketNodeMap
        extends BucketNodeMap
{
    private final List<InternalNode> bucketToNode;
    private final boolean cacheable;

    public FixedBucketNodeMap(ToIntFunction<Split> splitToBucket, List<InternalNode> bucketToNode, boolean cacheable)
    {
        super(splitToBucket);
        requireNonNull(bucketToNode, "bucketToNode is null");
        this.bucketToNode = ImmutableList.copyOf(requireNonNull(bucketToNode, "bucketToNode is null"));
        this.cacheable = cacheable;
    }

    @Override
    public Optional<InternalNode> getAssignedNode(int bucketedId)
    {
        return Optional.of(bucketToNode.get(bucketedId));
    }

    @Override
    public boolean isBucketCacheable(int bucketedId)
    {
        return cacheable;
    }

    @Override
    public int getBucketCount()
    {
        return bucketToNode.size();
    }

    @Override
    public void assignOrUpdateBucketToNode(int bucketedId, InternalNode node, boolean cacheable)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDynamic()
    {
        return false;
    }

    @Override
    public boolean hasInitialMap()
    {
        return true;
    }

    @Override
    public Optional<List<InternalNode>> getBucketToNode()
    {
        return Optional.of(bucketToNode);
    }
}
