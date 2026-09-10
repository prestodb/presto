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
package com.facebook.presto.ducklake.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A row of {@code ducklake_partition_column} for the active partition spec (the row of {@code
 * ducklake_partition_info} visible at the snapshot). {@code transform} is the raw string as
 * stored (for example {@code identity}, {@code bucket(16)}, {@code year}, {@code month}, {@code
 * day}, {@code hour}); a later task (Task 3.2) parses it.
 */
public class DuckLakePartitionField
{
    private final int partitionKeyIndex;
    private final long columnId;
    private final String transform;

    @JsonCreator
    public DuckLakePartitionField(
            @JsonProperty("partitionKeyIndex") int partitionKeyIndex,
            @JsonProperty("columnId") long columnId,
            @JsonProperty("transform") String transform)
    {
        this.partitionKeyIndex = partitionKeyIndex;
        this.columnId = columnId;
        this.transform = requireNonNull(transform, "transform is null");
    }

    @JsonProperty
    public int getPartitionKeyIndex()
    {
        return partitionKeyIndex;
    }

    @JsonProperty
    public long getColumnId()
    {
        return columnId;
    }

    @JsonProperty
    public String getTransform()
    {
        return transform;
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
        DuckLakePartitionField that = (DuckLakePartitionField) o;
        return partitionKeyIndex == that.partitionKeyIndex &&
                columnId == that.columnId &&
                transform.equals(that.transform);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionKeyIndex, columnId, transform);
    }

    @Override
    public String toString()
    {
        return "DuckLakePartitionField{" +
                "partitionKeyIndex=" + partitionKeyIndex +
                ", columnId=" + columnId +
                ", transform='" + transform + '\'' +
                '}';
    }
}
