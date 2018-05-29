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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class ElasticsearchTableDescription
{
    private final String tableName;
    private final String schemaName;
    private final String hostAddress;
    private final int port;
    private final String clusterName;
    private final String index;
    private final boolean indexExactMatch;
    private final String type;

    private List<ElasticsearchColumn> columns;
    private List<ElasticsearchColumnMetadata> columnsMetadata;

    @JsonCreator
    public ElasticsearchTableDescription(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("hostAddress") String hostAddress,
            @JsonProperty("port") int port,
            @JsonProperty("clusterName") String clusterName,
            @JsonProperty("index") String index,
            @JsonProperty("indexExactMatch") boolean indexExactMatch,
            @JsonProperty("type") String type,
            @JsonProperty("columns") List<ElasticsearchColumn> columns)
    {
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        checkArgument(!isNullOrEmpty(hostAddress), "hostAddress is null or is empty");
        checkArgument(!isNullOrEmpty(clusterName), "clusterName is null or is empty");
        checkArgument(!isNullOrEmpty(type), "type is null or is empty");
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.hostAddress = hostAddress;
        this.port = port;
        this.clusterName = clusterName;
        this.index = index;
        this.indexExactMatch = indexExactMatch;
        this.type = type;
        if (columns == null) {
            this.columns = ImmutableList.of();
        }
        else {
            this.columns = ImmutableList.copyOf(columns);
        }
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getHostAddress()
    {
        return hostAddress;
    }

    @JsonProperty
    public int getPort()
    {
        return port;
    }

    @JsonProperty
    public String getClusterName()
    {
        return clusterName;
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public boolean getIndexExactMatch()
    {
        return indexExactMatch;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public List<ElasticsearchColumn> getColumns()
    {
        return columns;
    }

    public void setColumns(List<ElasticsearchColumn> columns)
    {
        this.columns = ImmutableList.copyOf(columns);
    }

    public boolean isMetadataSet()
    {
        return columnsMetadata != null;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return ImmutableList.copyOf(columnsMetadata);
    }

    public void setColumnsMetadata(List<ElasticsearchColumnMetadata> columnsMetadata)
    {
        this.columnsMetadata = columnsMetadata;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("schemaName", schemaName)
                .add("hostAddress", hostAddress)
                .add("port", port)
                .add("clusterName", clusterName)
                .add("index", index)
                .add("indexExactMatch", indexExactMatch)
                .add("type", type)
                .add("columns", columns)
                .toString();
    }
}
