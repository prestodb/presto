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
package com.facebook.presto.raptor.metadata;

import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShardNodes
{
    private final Set<UUID> shardUuids;
    private final OptionalInt bucketNumber;
    private final Set<String> nodeIdentifiers;

    public ShardNodes(UUID shardUuid, Set<String> nodeIdentifiers)
    {
        this(ImmutableSet.of(shardUuid), OptionalInt.empty(), nodeIdentifiers);
    }

    public ShardNodes(Set<UUID> shardUuids, OptionalInt bucketNumber, Set<String> nodeIdentifiers)
    {
        this.shardUuids = ImmutableSet.copyOf(requireNonNull(shardUuids, "shardUuids is null"));
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.nodeIdentifiers = ImmutableSet.copyOf(requireNonNull(nodeIdentifiers, "nodeIdentifiers is null"));
    }

    public Set<UUID> getShardUuids()
    {
        return shardUuids;
    }

    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    public Set<String> getNodeIdentifiers()
    {
        return nodeIdentifiers;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ShardNodes other = (ShardNodes) obj;
        return Objects.equals(this.shardUuids, other.shardUuids) &&
                Objects.equals(this.bucketNumber, other.bucketNumber) &&
                Objects.equals(this.nodeIdentifiers, other.nodeIdentifiers);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(shardUuids, bucketNumber, nodeIdentifiers);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("shardUuids", shardUuids)
                .add("bucketNumber", bucketNumber.isPresent() ? bucketNumber.getAsInt() : null)
                .add("nodeIdentifiers", nodeIdentifiers)
                .omitNullValues()
                .toString();
    }
}
