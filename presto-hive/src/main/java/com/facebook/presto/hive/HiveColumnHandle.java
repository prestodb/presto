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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.facebook.presto.hive.util.Types.checkType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveColumnHandle
        implements ConnectorColumnHandle
{
    public static final String SAMPLE_WEIGHT_COLUMN_NAME = "__presto__sample_weight__";

    private final String clientId;
    private final String name;
    private final int ordinalPosition;
    private final HiveType hiveType;
    private final TypeSignature typeName;
    private final int hiveColumnIndex;
    private final boolean partitionKey;

    @JsonCreator
    public HiveColumnHandle(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("name") String name,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("hiveType") HiveType hiveType,
            @JsonProperty("typeSignature") TypeSignature typeSignature,
            @JsonProperty("hiveColumnIndex") int hiveColumnIndex,
            @JsonProperty("partitionKey") boolean partitionKey)
    {
        this.clientId = checkNotNull(clientId, "clientId is null");
        this.name = checkNotNull(name, "name is null");
        checkArgument(ordinalPosition >= 0, "ordinalPosition is negative");
        this.ordinalPosition = ordinalPosition;
        checkArgument(hiveColumnIndex >= 0 || partitionKey, "hiveColumnIndex is negative");
        this.hiveColumnIndex = hiveColumnIndex;
        this.hiveType = checkNotNull(hiveType, "hiveType is null");
        this.typeName = checkNotNull(typeSignature, "type is null");
        this.partitionKey = partitionKey;
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
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
    public HiveType getHiveType()
    {
        return hiveType;
    }

    @JsonProperty
    public int getHiveColumnIndex()
    {
        return hiveColumnIndex;
    }

    @JsonProperty
    public boolean isPartitionKey()
    {
        return partitionKey;
    }

    public ColumnMetadata getColumnMetadata(TypeManager typeManager)
    {
        return new ColumnMetadata(name, typeManager.getType(typeName), ordinalPosition, partitionKey);
    }

    @JsonProperty
    public TypeSignature getTypeSignature()
    {
        return typeName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(clientId, name, hiveColumnIndex, hiveType, partitionKey);
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
        HiveColumnHandle other = (HiveColumnHandle) obj;
        return Objects.equals(this.clientId, other.clientId) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.hiveColumnIndex, other.hiveColumnIndex) &&
                Objects.equals(this.hiveType, other.hiveType) &&
                Objects.equals(this.partitionKey, other.partitionKey);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("clientId", clientId)
                .add("name", name)
                .add("ordinalPosition", ordinalPosition)
                .add("hiveType", hiveType)
                .add("hiveColumnIndex", hiveColumnIndex)
                .add("partitionKey", partitionKey)
                .toString();
    }

    public static HiveColumnHandle toHiveColumnHandle(ConnectorColumnHandle columnHandle)
    {
        return checkType(columnHandle, HiveColumnHandle.class, "columnHandle");
    }
}
