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
package io.prestosql.execution.scheduler.group;

import io.prestosql.execution.scheduler.BucketNodeMap;
import io.prestosql.metadata.Split;
import io.prestosql.spi.Node;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.Optional;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DynamicBucketNodeMap
        extends BucketNodeMap
{
    private final int bucketCount;
    private final Int2ObjectMap<Node> bucketToNode = new Int2ObjectOpenHashMap<>();

    public DynamicBucketNodeMap(ToIntFunction<Split> splitToBucket, int bucketCount)
    {
        super(splitToBucket);
        checkArgument(bucketCount > 0, "bucketCount must be positive");
        this.bucketCount = bucketCount;
    }

    @Override
    public Optional<Node> getAssignedNode(int bucketedId)
    {
        return Optional.ofNullable(bucketToNode.get(bucketedId));
    }

    @Override
    public int getBucketCount()
    {
        return bucketCount;
    }

    @Override
    public void assignBucketToNode(int bucketedId, Node node)
    {
        checkArgument(bucketedId >= 0 && bucketedId < bucketCount);
        requireNonNull(node, "node is null");
        checkState(!bucketToNode.containsKey(bucketedId), "bucket already assigned");
        bucketToNode.put(bucketedId, node);
    }

    @Override
    public boolean isDynamic()
    {
        return true;
    }
}
