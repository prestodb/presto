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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BucketShards
{
    private final OptionalInt bucketNumber;
    private final Set<ShardNodes> shards;

    public BucketShards(OptionalInt bucketNumber, Set<ShardNodes> shards)
    {
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.shards = ImmutableSet.copyOf(requireNonNull(shards, "shards is null"));
    }

    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    public Set<ShardNodes> getShards()
    {
        return shards;
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
        BucketShards other = (BucketShards) obj;
        return Objects.equals(this.bucketNumber, other.bucketNumber) &&
                Objects.equals(this.shards, other.shards);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketNumber, shards);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketNumber", bucketNumber.isPresent() ? bucketNumber.getAsInt() : null)
                .add("shards", shards)
                .omitNullValues()
                .toString();
    }
}
