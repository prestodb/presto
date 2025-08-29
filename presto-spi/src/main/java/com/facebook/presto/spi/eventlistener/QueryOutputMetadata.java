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
package com.facebook.presto.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryOutputMetadata
{
    private final String catalogName;
    private final String schema;
    private final String table;

    private final Optional<String> connectorOutputMetadata;
    private final Optional<Boolean> jsonLengthLimitExceeded;

    private final String serializedCommitOutput;
    private final Optional<List<OutputColumnMetadata>> columns;

    public QueryOutputMetadata(
            String catalogName,
            String schema,
            String table,
            Optional<String> connectorOutputMetadata,
            Optional<Boolean> jsonLengthLimitExceeded,
            String serializedCommitOutput,
            Optional<List<OutputColumnMetadata>> columns)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.connectorOutputMetadata = requireNonNull(connectorOutputMetadata, "connectorOutputMetadata is null");
        this.jsonLengthLimitExceeded = requireNonNull(jsonLengthLimitExceeded, "jsonLengthLimitExceeded is null");
        this.serializedCommitOutput = requireNonNull(serializedCommitOutput, "connectorCommitHandle is null");
        this.columns = requireNonNull(columns, "columns is null");
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
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
    public Optional<String> getConnectorOutputMetadata()
    {
        return connectorOutputMetadata;
    }

    @JsonProperty
    public Optional<Boolean> getJsonLengthLimitExceeded()
    {
        return jsonLengthLimitExceeded;
    }

    @JsonProperty
    public String getSerializedCommitOutput()
    {
        return serializedCommitOutput;
    }

    @JsonProperty
    public Optional<List<OutputColumnMetadata>> getColumns()
    {
        return columns;
    }
}
