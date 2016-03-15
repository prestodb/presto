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
package com.facebook.presto.cassandra;

import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CassandraColumnHandle
        implements ColumnHandle
{
    public static final String SAMPLE_WEIGHT_COLUMN_NAME = "presto_sample_weight";

    private final String connectorId;
    private final String name;
    private final int ordinalPosition;
    private final CassandraType cassandraType;
    private final List<CassandraType> typeArguments;
    private final boolean partitionKey;
    private final boolean clusteringKey;
    private final boolean indexed;
    private final boolean hidden;

    @JsonCreator
    public CassandraColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("name") String name,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("cassandraType") CassandraType cassandraType,
            @Nullable @JsonProperty("typeArguments") List<CassandraType> typeArguments,
            @JsonProperty("partitionKey") boolean partitionKey,
            @JsonProperty("clusteringKey") boolean clusteringKey,
            @JsonProperty("indexed") boolean indexed,
            @JsonProperty("hidden") boolean hidden)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.name = requireNonNull(name, "name is null");
        checkArgument(ordinalPosition >= 0, "ordinalPosition is negative");
        this.ordinalPosition = ordinalPosition;
        this.cassandraType = requireNonNull(cassandraType, "cassandraType is null");
        int typeArgsSize = cassandraType.getTypeArgumentSize();
        if (typeArgsSize > 0) {
            this.typeArguments = requireNonNull(typeArguments, "typeArguments is null");
            checkArgument(typeArguments.size() == typeArgsSize, cassandraType
                    + " must provide " + typeArgsSize + " type arguments");
        }
        else {
            this.typeArguments = null;
        }
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
        this.indexed = indexed;
        this.hidden = hidden;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public CassandraType getCassandraType()
    {
        return cassandraType;
    }

    @JsonProperty
    public List<CassandraType> getTypeArguments()
    {
        return typeArguments;
    }

    @JsonProperty
    public boolean isPartitionKey()
    {
        return partitionKey;
    }

    @JsonProperty
    public boolean isClusteringKey()
    {
        return clusteringKey;
    }

    @JsonProperty
    public boolean isIndexed()
    {
        return indexed;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(CassandraCqlUtils.cqlNameToSqlName(name), cassandraType.getNativeType(), null, hidden);
    }

    public Type getType()
    {
        return cassandraType.getNativeType();
    }

    public FullCassandraType getFullType()
    {
        if (cassandraType.getTypeArgumentSize() == 0) {
            return cassandraType;
        }
        return new CassandraTypeWithTypeArguments(cassandraType, typeArguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                connectorId,
                name,
                ordinalPosition,
                cassandraType,
                typeArguments,
                partitionKey,
                clusteringKey,
                indexed,
                hidden);
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
        CassandraColumnHandle other = (CassandraColumnHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equals(this.cassandraType, other.cassandraType) &&
                Objects.equals(this.typeArguments, other.typeArguments) &&
                Objects.equals(this.partitionKey, other.partitionKey) &&
                Objects.equals(this.clusteringKey, other.clusteringKey) &&
                Objects.equals(this.indexed, other.indexed) &&
                Objects.equals(this.hidden, other.hidden);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this)
                .add("connectorId", connectorId)
                .add("name", name)
                .add("ordinalPosition", ordinalPosition)
                .add("cassandraType", cassandraType);

        if (typeArguments != null && !typeArguments.isEmpty()) {
            helper.add("typeArguments", typeArguments);
        }

        helper.add("partitionKey", partitionKey)
                .add("clusteringKey", clusteringKey)
                .add("indexed", indexed)
                .add("hidden", hidden);

        return helper.toString();
    }
}
