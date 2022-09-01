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

import com.facebook.presto.spi.statistics.TableStatistics;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryInputMetadata
{
    private final String catalogName;
    private final String schema;
    private final String table;
    private final List<String> columns;
    private final Optional<Object> connectorInfo;
    private final Optional<TableStatistics> statistics;
    private final String serializedCommitOutput;

    public QueryInputMetadata(String catalogName, String schema, String table, List<String> columns, Optional<Object> connectorInfo, Optional<TableStatistics> statistics, String serializedCommitOutput)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.connectorInfo = requireNonNull(connectorInfo, "connectorInfo is null");
        this.statistics = requireNonNull(statistics, "table statistics is null");
        this.serializedCommitOutput = requireNonNull(serializedCommitOutput, "serializedCommitOutput is null");
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
    public List<String> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<Object> getConnectorInfo()
    {
        return connectorInfo;
    }

    @JsonProperty
    public Optional<TableStatistics> getStatistics()
    {
        return statistics;
    }

    @JsonProperty
    public String getSerializedCommitOutput()
    {
        return serializedCommitOutput;
    }
}
