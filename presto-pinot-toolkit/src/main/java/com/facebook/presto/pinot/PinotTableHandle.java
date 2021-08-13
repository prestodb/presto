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
package com.facebook.presto.pinot;

import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class PinotTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final Optional<Boolean> isQueryShort;
    private final Optional<PinotQueryGenerator.GeneratedPinotQuery> pinotQuery;
    private final Optional<List<PinotColumnHandle>> expectedColumnHandles;

    public PinotTableHandle(
            String connectorId,
            String schemaName,
            String tableName)
    {
        this(connectorId, schemaName, tableName, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @JsonCreator
    public PinotTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("isQueryShort") Optional<Boolean> isQueryShort,
            @JsonProperty("expectedColumnHandles") Optional<List<PinotColumnHandle>> expectedColumnHandles,
            @JsonProperty("pinotQuery") Optional<PinotQueryGenerator.GeneratedPinotQuery> pinotQuery)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.isQueryShort = requireNonNull(isQueryShort, "safe to execute is null");
        this.pinotQuery = requireNonNull(pinotQuery, "broker pinotQuery is null");
        this.expectedColumnHandles = requireNonNull(expectedColumnHandles, "expected column handles is null");
    }

    @JsonProperty
    public Optional<PinotQueryGenerator.GeneratedPinotQuery> getPinotQuery()
    {
        return pinotQuery;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<Boolean> getIsQueryShort()
    {
        return isQueryShort;
    }

    @JsonProperty
    public Optional<List<PinotColumnHandle>> getExpectedColumnHandles()
    {
        return expectedColumnHandles;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
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
        PinotTableHandle that = (PinotTableHandle) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(isQueryShort, that.isQueryShort) &&
                Objects.equals(expectedColumnHandles, that.expectedColumnHandles) &&
                Objects.equals(pinotQuery, that.pinotQuery);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName, isQueryShort, expectedColumnHandles, pinotQuery);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("isQueryShort", isQueryShort)
                .add("expectedColumnHandles", expectedColumnHandles)
                .add("pinotQuery", pinotQuery)
                .toString();
    }
}
