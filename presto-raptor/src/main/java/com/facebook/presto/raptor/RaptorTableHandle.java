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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.facebook.presto.raptor.util.MetadataUtil.checkSchemaName;
import static com.facebook.presto.raptor.util.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class RaptorTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final long tableId;
    private final OptionalLong distributionId;
    private final Optional<String> distributionName;
    private final OptionalInt bucketCount;
    private final boolean organized;
    private final OptionalLong transactionId;
    private final boolean delete;

    @JsonCreator
    public RaptorTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("distributionId") OptionalLong distributionId,
            @JsonProperty("distributionName") Optional<String> distributionName,
            @JsonProperty("bucketCount") OptionalInt bucketCount,
            @JsonProperty("organized") boolean organized,
            @JsonProperty("transactionId") OptionalLong transactionId,
            @JsonProperty("delete") boolean delete)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);

        checkArgument(tableId > 0, "tableId must be greater than zero");
        this.tableId = tableId;

        this.distributionName = requireNonNull(distributionName, "distributionName is null");
        this.distributionId = requireNonNull(distributionId, "distributionId is null");
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
        this.organized = organized;
        this.transactionId = requireNonNull(transactionId, "transactionId is null");

        this.delete = delete;
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
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public OptionalLong getDistributionId()
    {
        return distributionId;
    }

    @JsonProperty
    public Optional<String> getDistributionName()
    {
        return distributionName;
    }

    @JsonProperty
    public OptionalInt getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public boolean isOrganized()
    {
        return organized;
    }

    @JsonProperty
    public OptionalLong getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public boolean isDelete()
    {
        return delete;
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + schemaName + ":" + tableName + ":" + tableId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RaptorTableHandle other = (RaptorTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.tableId, other.tableId);
    }
}
