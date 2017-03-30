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
package com.facebook.presto.hdfs;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSTable
{
    private final HDFSTableHandle table;
    private final HDFSTableLayoutHandle tableLayout;
    private final List<HDFSColumnHandle> columns;
    private final List<ColumnMetadata> columnMetadatas;

    @JsonCreator
    public HDFSTable(
            @JsonProperty("table") HDFSTableHandle table,
            @JsonProperty("tableLayout") HDFSTableLayoutHandle tableLayout,
            @JsonProperty("columns") List<HDFSColumnHandle> columns,
            @JsonProperty("columnMetadatas") List<ColumnMetadata> columnMetadatas)
    {
        this.table = requireNonNull(table, "table is null");
        this.tableLayout = requireNonNull(tableLayout, "tableLayout is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.columnMetadatas = requireNonNull(columnMetadatas, "columnMetadas is null");
    }

    @JsonProperty
    public HDFSTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public HDFSTableLayoutHandle getTableLayout()
    {
        return tableLayout;
    }

    @JsonProperty
    public List<HDFSColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<ColumnMetadata> getColumnMetadatas()
    {
        return columnMetadatas;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, tableLayout, columns, columnMetadatas);
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

        HDFSTable other = (HDFSTable) obj;
        return Objects.equals(table, other.table) &&
                Objects.equals(tableLayout, other.tableLayout) &&
                Objects.equals(columns, other.columns) &&
                Objects.equals(columnMetadatas, other.columnMetadatas);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("table layout", tableLayout)
                .add("columns", columns)
                .add("column metadatas", columnMetadatas)
                .toString();
    }
}
