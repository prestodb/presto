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

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.connector.ConnectorCommitHandle;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public final class Output
{
    private final ConnectorId connectorId;
    private final String schema;
    private final String table;
    private final Optional<List<OutputColumnMetadata>> columns;
    private final Optional<ConnectorCommitHandle> commitHandle;

    @JsonCreator
    public Output(
            @JsonProperty("connectorId") ConnectorId connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("columns") Optional<List<OutputColumnMetadata>> columns,
            @JsonProperty("commitHandle") Optional<ConnectorCommitHandle> commitHandle)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = columns.map(ImmutableList::copyOf);
        this.commitHandle = requireNonNull(commitHandle, "commitHandle is null");
    }

    @JsonProperty
    public ConnectorId getConnectorId()
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
    public Optional<List<OutputColumnMetadata>> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<ConnectorCommitHandle> getCommitHandle()
    {
        return commitHandle;
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
        Output output = (Output) o;
        return Objects.equals(connectorId, output.connectorId) &&
                Objects.equals(schema, output.schema) &&
                Objects.equals(table, output.table) &&
                Objects.equals(columns, output.columns) &&
                Objects.equals(commitHandle, output.commitHandle);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schema, table, columns, commitHandle);
    }
}
