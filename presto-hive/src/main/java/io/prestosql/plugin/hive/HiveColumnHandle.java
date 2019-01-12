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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.spi.type.BigintType.BIGINT;
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
        SYNTHESIZED,
    }

    private final String name;
    private final HiveType hiveType;
    private final TypeSignature typeName;
    private final int hiveColumnIndex;
    private final ColumnType columnType;
    private final Optional<String> comment;

    @JsonCreator
    public HiveColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("hiveType") HiveType hiveType,
            @JsonProperty("typeSignature") TypeSignature typeSignature,
            @JsonProperty("hiveColumnIndex") int hiveColumnIndex,
            @JsonProperty("columnType") ColumnType columnType,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.name = requireNonNull(name, "name is null");
        checkArgument(hiveColumnIndex >= 0 || columnType == PARTITION_KEY || columnType == SYNTHESIZED, "hiveColumnIndex is negative");
        this.hiveColumnIndex = hiveColumnIndex;
        this.hiveType = requireNonNull(hiveType, "hiveType is null");
        this.typeName = requireNonNull(typeSignature, "type is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.comment = requireNonNull(comment, "comment is null");
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
        return columnType == SYNTHESIZED;
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
        return Objects.hash(name, hiveColumnIndex, hiveType, columnType, comment);
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
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.hiveColumnIndex, other.hiveColumnIndex) &&
                Objects.equals(this.hiveType, other.hiveType) &&
                Objects.equals(this.columnType, other.columnType) &&
                Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString()
    {
        return name + ":" + hiveType + ":" + hiveColumnIndex + ":" + columnType;
    }

    public static HiveColumnHandle updateRowIdHandle()
    {
        // Hive connector only supports metadata delete. It does not support generic row-by-row deletion.
        // Metadata delete is implemented in Presto by generating a plan for row-by-row delete first,
        // and then optimize it into metadata delete. As a result, Hive connector must provide partial
        // plan-time support for row-by-row delete so that planning doesn't fail. This is why we need
        // rowid handle. Note that in Hive connector, rowid handle is not implemented beyond plan-time.

        return new HiveColumnHandle(UPDATE_ROW_ID_COLUMN_NAME, HIVE_LONG, BIGINT.getTypeSignature(), -1, SYNTHESIZED, Optional.empty());
    }

    public static HiveColumnHandle pathColumnHandle()
    {
        return new HiveColumnHandle(PATH_COLUMN_NAME, PATH_HIVE_TYPE, PATH_TYPE_SIGNATURE, PATH_COLUMN_INDEX, SYNTHESIZED, Optional.empty());
    }

    /**
     * The column indicating the bucket id.
     * When table bucketing differs from partition bucketing, this column indicates
     * what bucket the row will fall in under the table bucketing scheme.
     */
    public static HiveColumnHandle bucketColumnHandle()
    {
        return new HiveColumnHandle(BUCKET_COLUMN_NAME, BUCKET_HIVE_TYPE, BUCKET_TYPE_SIGNATURE, BUCKET_COLUMN_INDEX, SYNTHESIZED, Optional.empty());
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
