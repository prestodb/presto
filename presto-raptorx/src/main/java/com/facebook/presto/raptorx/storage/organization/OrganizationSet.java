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
package com.facebook.presto.raptorx.storage.organization;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class OrganizationSet
{
    private final long tableId;
    private final Set<Long> chunkIds;
    private final int bucketNumber;

    public OrganizationSet(long tableId, Set<Long> chunkIds, int bucketNumber)
    {
        this.tableId = tableId;
        this.chunkIds = requireNonNull(chunkIds, "ChunkIds is null");
        this.bucketNumber = bucketNumber;
    }

    public long getTableId()
    {
        return tableId;
    }

    public Set<Long> getChunkIds()
    {
        return chunkIds;
    }

    public int getBucketNumber()
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
                Objects.equals(chunkIds, that.chunkIds) &&
                Objects.equals(bucketNumber, that.bucketNumber);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableId, chunkIds, bucketNumber);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("chunkIds", chunkIds)
                .add("bucketNumber", bucketNumber)
                .omitNullValues()
                .toString();
    }
}
