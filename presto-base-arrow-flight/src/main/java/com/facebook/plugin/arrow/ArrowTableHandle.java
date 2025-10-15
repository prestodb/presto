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
package com.facebook.plugin.arrow;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ArrowTableHandle
        implements ConnectorTableHandle
{
    private final String schema;
    private final String table;
    private final Optional<List<ArrowColumnHandle>> columns;

    /***
     * Create instance of ArrowTableHandle
     * @param schema schema name
     * @param table table name
     * @param columns If present, this list should be the list of columns in the table,
     * which can be a larger list than the list of output columns. This list should match the list of vectors
     * returned by Arrow Flight stream for this table. This value needs to be set only
     * in certain scenarios like when using TVF. In other common scenarios, this can be empty.
     */
    @JsonCreator
    public ArrowTableHandle(
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("columns") Optional<List<ArrowColumnHandle>> columns)
    {
        this.schema = schema;
        this.table = table;
        this.columns = columns;
    }

    @JsonProperty("schema")
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty("table")
    public String getTable()
    {
        return table;
    }

    @JsonProperty("columns")
    public Optional<List<ArrowColumnHandle>> getColumns()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        return schema + ":" + table + ":" + columns;
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
        ArrowTableHandle that = (ArrowTableHandle) o;
        return Objects.equals(schema, that.schema) && Objects.equals(table, that.table) && Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, table);
    }
}
