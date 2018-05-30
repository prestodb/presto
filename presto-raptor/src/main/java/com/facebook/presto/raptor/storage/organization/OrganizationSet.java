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
package com.facebook.presto.raptor.storage.organization;

import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class OrganizationSet
{
    private final long tableId;
    private final Set<UUID> shards;
    private final OptionalInt bucketNumber;

    public OrganizationSet(long tableId, Set<UUID> shards, OptionalInt bucketNumber)
    {
        this.tableId = tableId;
        this.shards = requireNonNull(shards, "shards is null");
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public Set<UUID> getShards()
    {
        return shards;
    }

    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrganizationSet that = (OrganizationSet) o;
        return tableId == that.tableId &&
                Objects.equals(shards, that.shards) &&
                Objects.equals(bucketNumber, that.bucketNumber);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableId, shards, bucketNumber);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("shards", shards)
                .add("bucketNumber", bucketNumber.isPresent() ? bucketNumber.getAsInt() : null)
                .omitNullValues()
                .toString();
    }
}
