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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveColumnHandle
        implements ColumnHandle
{
    private final String clientId;
    private final String name;
    private final int ordinalPosition;
    private final HiveType hiveType;
    private final int hiveColumnIndex;
    private final boolean partitionKey;

    @JsonCreator
    public HiveColumnHandle(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("name") String name,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("hiveType") HiveType hiveType,
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

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(name, hiveType.getNativeType(), ordinalPosition, partitionKey);
    }

    public ColumnType getType()
    {
        return hiveType.getNativeType();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(clientId, name, hiveColumnIndex, hiveType, partitionKey);
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
        return Objects.equal(this.clientId, other.clientId) &&
                Objects.equal(this.name, other.name) &&
                Objects.equal(this.hiveColumnIndex, other.hiveColumnIndex) &&
                Objects.equal(this.hiveType, other.hiveType) &&
                Objects.equal(this.partitionKey, other.partitionKey);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("clientId", clientId)
                .add("name", name)
                .add("ordinalPosition", ordinalPosition)
                .add("hiveType", hiveType)
                .add("hiveColumnIndex", hiveColumnIndex)
                .add("partitionKey", partitionKey)
                .toString();
    }

    public static Function<ColumnHandle, HiveColumnHandle> hiveColumnHandle()
    {
        return new Function<ColumnHandle, HiveColumnHandle>()
        {
            @Override
            public HiveColumnHandle apply(ColumnHandle columnHandle)
            {
                checkNotNull(columnHandle, "columnHandle is null");
                checkArgument(columnHandle instanceof HiveColumnHandle, "columnHandle is not an instance of HiveColumnHandle");
                return (HiveColumnHandle) columnHandle;
            }
        };
    }

    public static Function<HiveColumnHandle, Integer> hiveColumnIndexGetter()
    {
        return new Function<HiveColumnHandle, Integer>()
        {
            @Override
            public Integer apply(HiveColumnHandle input)
            {
                return input.getHiveColumnIndex();
            }
        };
    }

    public static Function<HiveColumnHandle, ColumnMetadata> columnMetadataGetter()
    {
        return new Function<HiveColumnHandle, ColumnMetadata>()
        {
            @Override
            public ColumnMetadata apply(HiveColumnHandle input)
            {
                return input.getColumnMetadata();
            }
        };
    }

    public static Function<HiveColumnHandle, ColumnType> nativeTypeGetter()
    {
        return new Function<HiveColumnHandle, ColumnType>()
        {
            @Override
            public ColumnType apply(HiveColumnHandle input)
            {
                return input.getType();
            }
        };
    }

    public static Predicate<HiveColumnHandle> isPartitionKeyPredicate()
    {
        return new Predicate<HiveColumnHandle>()
        {
            @Override
            public boolean apply(HiveColumnHandle input)
            {
                return input.isPartitionKey();
            }
        };
    }
}
