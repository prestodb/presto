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
package com.facebook.presto.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public final class Input
{
    private final String connectorId;
    private final String schema;
    private final String table;
    private final List<Column> columns;
    private final Optional<Object> connectorInfo;

    @JsonCreator
    public Input(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("connectorInfo") Optional<Object> connectorInfo,
            @JsonProperty("columns") List<Column> columns)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
        requireNonNull(connectorInfo, "connectorInfo is null");
        requireNonNull(columns, "columns is null");

        this.connectorId = connectorId;
        this.schema = schema;
        this.table = table;
        this.connectorInfo = connectorInfo;
        this.columns = ImmutableList.copyOf(columns);
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<Object> getConnectorInfo()
    {
        return connectorInfo;
    }

    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
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
        Input input = (Input) o;
        return Objects.equals(connectorId, input.connectorId) &&
                Objects.equals(schema, input.schema) &&
                Objects.equals(table, input.table) &&
                Objects.equals(columns, input.columns) &&
                Objects.equals(connectorInfo, input.connectorInfo);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schema, table, columns, connectorInfo);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(connectorId)
                .addValue(schema)
                .addValue(table)
                .addValue(columns)
                .toString();
    }
}
