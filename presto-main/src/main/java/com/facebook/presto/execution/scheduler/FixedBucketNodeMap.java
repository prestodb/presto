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

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

// the bucket to node mapping is fixed and pre-assigned
public class FixedBucketNodeMap
        extends BucketNodeMap
{
    private final Map<Integer, Node> bucketToNode;
    private final int bucketCount;

    public FixedBucketNodeMap(ToIntFunction<Split> splitToBucket, Map<Integer, Node> bucketToNode)
    {
        super(splitToBucket);
        requireNonNull(bucketToNode, "bucketToNode is null");
        this.bucketToNode = ImmutableMap.copyOf(bucketToNode);
        bucketCount = bucketToNode.keySet().stream()
                .mapToInt(Integer::intValue)
                .max()
                .getAsInt() + 1;
    }

    @Override
    public Optional<Node> getAssignedNode(int bucketedId)
    {
        checkArgument(bucketedId >= 0 && bucketedId < bucketCount);
        Node node = bucketToNode.get(bucketedId);
        verify(node != null);
        return Optional.of(node);
    }

    @Override
    public int getBucketCount()
    {
        return bucketCount;
    }
}
