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
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class Input
{
    private final String connectorId;
    private final String schema;
    private final String table;
    private final List<String> columns;

    @JsonCreator
    public Input(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("columns") List<String> columns)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(schema, "schema is null");
        checkNotNull(table, "table is null");
        checkNotNull(columns, "columns is null");

        this.connectorId = connectorId;
        this.schema = schema;
        this.table = table;
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
    public List<String> getColumns()
    {
        return columns;
    }
}
