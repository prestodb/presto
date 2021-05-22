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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TablestoreInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final SchemaTableName schemaTableName;
    private final List<TablestoreColumnHandle> columnHandleList;

    @JsonCreator
    public TablestoreInsertTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("columnHandleList") List<TablestoreColumnHandle> columnHandleList)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columnHandleList = requireNonNull(columnHandleList, "columnHandleList is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public List<TablestoreColumnHandle> getColumnHandleList()
    {
        return columnHandleList;
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
        TablestoreInsertTableHandle that = (TablestoreInsertTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName)
                && Objects.equals(columnHandleList, that.columnHandleList);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, columnHandleList);
    }

    @Override
    public String toString()
    {
        return format("tablestore:%s->%s", schemaTableName);
    }
}
