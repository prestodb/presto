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
package io.prestosql.plugin.raptor.legacy.metadata;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class TableMetadata
{
    private final long tableId;
    private final List<ColumnInfo> columns;
    private final List<Long> sortColumnIds;

    public TableMetadata(long tableId, List<ColumnInfo> columns, List<Long> sortColumnIds)
    {
        this.tableId = tableId;
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.sortColumnIds = ImmutableList.copyOf(requireNonNull(sortColumnIds, "sortColumnIds is null"));
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
        return Objects.equals(tableId, that.tableId) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(sortColumnIds, that.sortColumnIds);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableId, columns, sortColumnIds);
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
