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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class OrganizationSet
{
    private final long tableId;
    private final boolean tableSupportsDeltaDelete;
    private final Map<UUID, Optional<UUID>> shardsMap;
    private final OptionalInt bucketNumber;
    private final int priority;

    public OrganizationSet(long tableId, boolean tableSupportsDeltaDelete, Map<UUID, Optional<UUID>> shardsMap, OptionalInt bucketNumber, int priority)
    {
        this.tableId = tableId;
        this.tableSupportsDeltaDelete = tableSupportsDeltaDelete;
        this.shardsMap = requireNonNull(shardsMap, "shards is null");
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.priority = priority;
    }

    public long getTableId()
    {
        return tableId;
    }

    public boolean isTableSupportsDeltaDelete()
    {
        return tableSupportsDeltaDelete;
    }

    public Map<UUID, Optional<UUID>> getShardsMap()
    {
        return shardsMap;
    }

    public Set<UUID> getShards()
    {
        return shardsMap.keySet();
    }

    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    public int getPriority()
    {
        return priority;
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
                tableSupportsDeltaDelete == that.tableSupportsDeltaDelete &&
                Objects.equals(shardsMap, that.shardsMap) &&
                Objects.equals(bucketNumber, that.bucketNumber);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableId, tableSupportsDeltaDelete, shardsMap, bucketNumber);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("tableSupportsDeltaDelete", tableSupportsDeltaDelete)
                .add("shardSize", shardsMap.size())
                .add("deltaSize", shardsMap.values().stream().filter(Optional::isPresent).collect(toSet()).size())
                .add("priority", priority)
                .add("bucketNumber", bucketNumber.isPresent() ? bucketNumber.getAsInt() : null)
                .omitNullValues()
                .toString();
    }
}
