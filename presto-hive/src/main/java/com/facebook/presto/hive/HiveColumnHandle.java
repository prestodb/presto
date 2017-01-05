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
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.HIDDEN;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class HiveColumnHandle
        implements ColumnHandle
{
    public static final int PATH_COLUMN_INDEX = -11;
    public static final String PATH_COLUMN_NAME = "$path";
    public static final HiveType PATH_HIVE_TYPE = HIVE_STRING;
    public static final TypeSignature PATH_TYPE_SIGNATURE = PATH_HIVE_TYPE.getTypeSignature();

    public static final int BUCKET_COLUMN_INDEX = -12;
    public static final String BUCKET_COLUMN_NAME = "$bucket";
    public static final HiveType BUCKET_HIVE_TYPE = HIVE_INT;
    public static final TypeSignature BUCKET_TYPE_SIGNATURE = BUCKET_HIVE_TYPE.getTypeSignature();

    private static final String UPDATE_ROW_ID_COLUMN_NAME = "$shard_row_id";

    public enum ColumnType
    {
        PARTITION_KEY,
        REGULAR,
        HIDDEN
    }

    private final String clientId;
    private final String name;
    private final HiveType hiveType;
    private final TypeSignature typeName;
    private final int hiveColumnIndex;
    private final ColumnType columnType;
    private final Optional<String> comment;

    @JsonCreator
    public HiveColumnHandle(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("name") String name,
            @JsonProperty("hiveType") HiveType hiveType,
            @JsonProperty("typeSignature") TypeSignature typeSignature,
            @JsonProperty("hiveColumnIndex") int hiveColumnIndex,
            @JsonProperty("columnType") ColumnType columnType,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.clientId = requireNonNull(clientId, "clientId is null");
        this.name = requireNonNull(name, "name is null");
        checkArgument(hiveColumnIndex >= 0 || columnType == PARTITION_KEY || columnType == HIDDEN, "hiveColumnIndex is negative");
        this.hiveColumnIndex = hiveColumnIndex;
        this.hiveType = requireNonNull(hiveType, "hiveType is null");
        this.typeName = requireNonNull(typeSignature, "type is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.comment = requireNonNull(comment, "comment is null");
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
    public HiveType getHiveType()
    {
        return hiveType;
    }

    @JsonProperty
    public int getHiveColumnIndex()
    {
        return hiveColumnIndex;
    }

    public boolean isPartitionKey()
    {
        return columnType == PARTITION_KEY;
    }

    public boolean isHidden()
    {
        return columnType == HIDDEN;
    }

    public ColumnMetadata getColumnMetadata(TypeManager typeManager)
    {
        return new ColumnMetadata(name, typeManager.getType(typeName), null, isHidden());
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public TypeSignature getTypeSignature()
    {
        return typeName;
    }

    @JsonProperty
    public ColumnType getColumnType()
    {
        return columnType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(clientId, name, hiveColumnIndex, hiveType, columnType, comment);
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
                Objects.equals(this.columnType, other.columnType) &&
                Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("clientId", clientId)
                .add("name", name)
                .add("hiveType", hiveType)
                .add("hiveColumnIndex", hiveColumnIndex)
                .add("columnType", columnType)
                .add("comment", comment)
                .toString();
    }

    public static HiveColumnHandle toHiveColumnHandle(ColumnHandle columnHandle)
    {
        return checkType(columnHandle, HiveColumnHandle.class, "columnHandle");
    }

    public static HiveColumnHandle updateRowIdHandle(String connectorId)
    {
        // Hive connector only supports metadata delete. It does not support generic row-by-row deletion.
        // Metadata delete is implemented in Presto by generating a plan for row-by-row delete first,
        // and then optimize it into metadata delete. As a result, Hive connector must provide partial
        // plan-time support for row-by-row delete so that planning doesn't fail. This is why we need
        // rowid handle. Note that in Hive connector, rowid handle is not implemented beyond plan-time.

        return new HiveColumnHandle(connectorId, UPDATE_ROW_ID_COLUMN_NAME, HIVE_LONG, BIGINT.getTypeSignature(), -1, HIDDEN, Optional.empty());
    }

    public static HiveColumnHandle pathColumnHandle(String connectorId)
    {
        return new HiveColumnHandle(connectorId, PATH_COLUMN_NAME, PATH_HIVE_TYPE, PATH_TYPE_SIGNATURE, PATH_COLUMN_INDEX, HIDDEN, Optional.empty());
    }

    public static HiveColumnHandle bucketColumnHandle(String connectorId)
    {
        return new HiveColumnHandle(connectorId, BUCKET_COLUMN_NAME, BUCKET_HIVE_TYPE, BUCKET_TYPE_SIGNATURE, BUCKET_COLUMN_INDEX, HIDDEN, Optional.empty());
    }

    public static boolean isPathColumnHandle(HiveColumnHandle column)
    {
        return column.getHiveColumnIndex() == PATH_COLUMN_INDEX;
    }

    public static boolean isBucketColumnHandle(HiveColumnHandle column)
    {
        return column.getHiveColumnIndex() == BUCKET_COLUMN_INDEX;
    }
}
