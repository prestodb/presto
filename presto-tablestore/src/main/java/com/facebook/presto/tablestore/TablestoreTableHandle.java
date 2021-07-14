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
package com.facebook.presto.tablestore;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TablestoreTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final String printableStn;
    private final List<TablestoreColumnHandle> orderedColumnHandles;
    private final List<TablestoreColumnHandle> orderedPrimaryKeyColumns;
    private final Map<String, ColumnHandle> columnHandleMap;

    @JsonCreator
    public TablestoreTableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("orderedColumnHandles") List<TablestoreColumnHandle> orderedColumnHandles)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.printableStn = schemaTableName.toString();
        this.orderedColumnHandles = requireNonNull(orderedColumnHandles, "orderedColumnHandles is null");

        ImmutableList.Builder<TablestoreColumnHandle> pks = ImmutableList.builder();
        Builder<String, ColumnHandle> cBuilder = ImmutableMap.builder();
        for (TablestoreColumnHandle och : orderedColumnHandles) {
            String columnName = och.getColumnName();
            if (och.isPrimaryKey()) {
                pks.add(och);
            }
            cBuilder.put(columnName, och);
        }
        this.orderedPrimaryKeyColumns = pks.build().stream()
                .sorted(Comparator.comparing(TablestoreColumnHandle::getPkPosition))
                .collect(Collectors.toList());
        this.columnHandleMap = cBuilder.build();
    }

    @JsonProperty
    public Map<String, ColumnHandle> getColumnHandleMap()
    {
        return columnHandleMap;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public String getPrintableStn()
    {
        return printableStn;
    }

    @JsonProperty
    public String getTableName()
    {
        return schemaTableName.getTableName();
    }

    @JsonProperty
    public List<TablestoreColumnHandle> getOrderedColumnHandles()
    {
        return orderedColumnHandles;
    }

    @JsonProperty
    public List<TablestoreColumnHandle> getOrderedPrimaryKeyColumns()
    {
        return orderedPrimaryKeyColumns;
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
        TablestoreTableHandle that = (TablestoreTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName)
                && Objects.equals(printableStn, that.printableStn)
                && Objects.equals(orderedColumnHandles, that.orderedColumnHandles)
                && Objects.equals(orderedPrimaryKeyColumns, that.orderedPrimaryKeyColumns)
                && Objects.equals(columnHandleMap, that.columnHandleMap);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, printableStn, orderedColumnHandles, orderedPrimaryKeyColumns, columnHandleMap);
    }

    @Override
    public String toString()
    {
        return "table=" + schemaTableName;
    }
}
