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

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorCommitHandle;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class QueryInputMetadata
{
    private final String catalogName;
    private final String schema;
    private final String table;
    private final List<Column> columnObjects;
    private final Optional<Object> connectorInfo;
    private final Optional<TableStatistics> statistics;
    private final Optional<ConnectorCommitHandle> commitHandle;

    public QueryInputMetadata(String catalogName, String schema, String table, List<Column> columnObjects, Optional<Object> connectorInfo, Optional<TableStatistics> statistics, Optional<ConnectorCommitHandle> commitHandle)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.columnObjects = requireNonNull(columnObjects, "columns is null");
        this.connectorInfo = requireNonNull(connectorInfo, "connectorInfo is null");
        this.statistics = requireNonNull(statistics, "table statistics is null");
        this.commitHandle = requireNonNull(commitHandle, "commitHandle is null");
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
        return Collections.unmodifiableList(columnObjects.stream().map(Column::getName).collect(Collectors.toList()));
    }

    @JsonProperty
    public List<Column> getColumnObjects()
    {
        return columnObjects;
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

    public String getSerializedCommitOutput()
    {
        return commitHandle.map(x -> x.getSerializedCommitOutputForRead(new SchemaTableName(schema, table))).orElse("");
    }

    @JsonProperty
    public Optional<ConnectorCommitHandle> getCommitHandle()
    {
        return commitHandle;
    }
}
