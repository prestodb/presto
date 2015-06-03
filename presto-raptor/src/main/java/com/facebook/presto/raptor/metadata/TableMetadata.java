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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public final class TableMetadata
{
    private final long tableId;
    private final List<ColumnInfo> columns;
    private final List<Long> sortColumnIds;

    public TableMetadata(long tableId, List<ColumnInfo> columns, List<Long> sortColumnIds)
    {
        this.tableId = tableId;
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        this.sortColumnIds = ImmutableList.copyOf(checkNotNull(sortColumnIds, "sortColumnIds is null"));
    }

    public long getTableId()
    {
        return tableId;
    }

    public List<ColumnInfo> getColumns()
    {
        return columns;
    }

    public List<Long> getSortColumnIds()
    {
        return sortColumnIds;
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
        TableMetadata that = (TableMetadata) o;
        return Objects.equal(tableId, that.tableId) &&
                Objects.equal(columns, that.columns) &&
                Objects.equal(sortColumnIds, that.sortColumnIds);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableId, columns, sortColumnIds);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("columns", columns)
                .add("sortColumnIds", sortColumnIds)
                .toString();
    }
}
