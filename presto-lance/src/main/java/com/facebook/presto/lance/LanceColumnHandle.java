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
package com.facebook.presto.lance;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LanceColumnHandle
        implements ColumnHandle
{
    /**
     * Synthetic column used by row-level UPDATE to carry the row id plus all
     * unmodified columns, mirroring Iceberg's {@code $row_id} struct column.
     */
    public static final String UPDATE_ROW_ID_COLUMN_NAME = "$row_id";

    public enum ColumnHandleType
    {
        /** A regular table column read from Lance storage. */
        REGULAR,
        /** The {@code _rowaddr} metadata column used as the DELETE row id. */
        ROW_ADDRESS,
        /** The synthetic struct row id used by UPDATE (row address + unmodified columns). */
        UPDATE_ROW_ID,
    }

    private final String columnName;
    private final Type columnType;
    private final boolean nullable;
    private final ColumnHandleType columnHandleType;

    @JsonCreator
    public LanceColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("nullable") boolean nullable,
            @JsonProperty("columnHandleType") ColumnHandleType columnHandleType)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.nullable = nullable;
        this.columnHandleType = columnHandleType == null ? ColumnHandleType.REGULAR : columnHandleType;
    }

    public LanceColumnHandle(String columnName, Type columnType, boolean nullable)
    {
        this(columnName, columnType, nullable, ColumnHandleType.REGULAR);
    }

    public LanceColumnHandle(String columnName, Type columnType)
    {
        this(columnName, columnType, true, ColumnHandleType.REGULAR);
    }

    /**
     * The {@code _rowaddr} metadata column used as the row id for DELETE.
     */
    public static LanceColumnHandle rowAddressColumn()
    {
        return new LanceColumnHandle(RowAddress.ROW_ADDRESS_COLUMN_NAME, BigintType.BIGINT, false, ColumnHandleType.ROW_ADDRESS);
    }

    /**
     * The synthetic {@code $row_id} struct column used by UPDATE. Its first field is the
     * row address, followed by every column that is not being updated, so the connector
     * can re-emit the full row when rewriting it.
     */
    public static LanceColumnHandle updateRowIdColumn(List<LanceColumnHandle> unmodifiedColumns)
    {
        List<RowType.Field> fields = new ArrayList<>();
        fields.add(new RowType.Field(Optional.of(RowAddress.ROW_ADDRESS_COLUMN_NAME), BigintType.BIGINT));
        for (LanceColumnHandle column : unmodifiedColumns) {
            fields.add(new RowType.Field(Optional.of(column.getColumnName()), column.getColumnType()));
        }
        return new LanceColumnHandle(UPDATE_ROW_ID_COLUMN_NAME, RowType.from(fields), false, ColumnHandleType.UPDATE_ROW_ID);
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public boolean isNullable()
    {
        return nullable;
    }

    @JsonProperty
    public ColumnHandleType getColumnHandleType()
    {
        return columnHandleType;
    }

    public boolean isRowAddress()
    {
        return columnHandleType == ColumnHandleType.ROW_ADDRESS;
    }

    public boolean isUpdateRowId()
    {
        return columnHandleType == ColumnHandleType.UPDATE_ROW_ID;
    }

    public boolean isSynthetic()
    {
        return columnHandleType != ColumnHandleType.REGULAR;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnType)
                .setNullable(nullable)
                .build();
    }

    public static Type toPrestoType(Field field)
    {
        ArrowType type = field.getType();

        if (type instanceof ArrowType.FixedSizeList || type instanceof ArrowType.List) {
            Type elementType = RealType.REAL;
            if (field.getChildren() != null && !field.getChildren().isEmpty()) {
                elementType = toPrestoType(field.getChildren().get(0));
            }
            return new ArrayType(elementType);
        }

        if (type instanceof ArrowType.Bool) {
            return BooleanType.BOOLEAN;
        }
        else if (type instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) type;
            switch (intType.getBitWidth()) {
                case 8:
                    return TinyintType.TINYINT;
                case 16:
                    return SmallintType.SMALLINT;
                case 32:
                    return IntegerType.INTEGER;
                case 64:
                    return BigintType.BIGINT;
            }
        }
        else if (type instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) type;
            if (fpType.getPrecision() == FloatingPointPrecision.HALF
                    || fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
                return RealType.REAL;
            }
            return DoubleType.DOUBLE;
        }
        else if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
            return VarcharType.VARCHAR;
        }
        else if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) {
            return VarbinaryType.VARBINARY;
        }
        else if (type instanceof ArrowType.Date) {
            return DateType.DATE;
        }
        else if (type instanceof ArrowType.Timestamp) {
            return TimestampType.TIMESTAMP;
        }
        throw new UnsupportedOperationException("Unsupported Arrow type: " + type);
    }

    public static ArrowType toArrowType(Type prestoType)
    {
        if (prestoType.equals(BooleanType.BOOLEAN)) {
            return ArrowType.Bool.INSTANCE;
        }
        else if (prestoType.equals(TinyintType.TINYINT)) {
            return new ArrowType.Int(8, true);
        }
        else if (prestoType.equals(SmallintType.SMALLINT)) {
            return new ArrowType.Int(16, true);
        }
        else if (prestoType.equals(IntegerType.INTEGER)) {
            return new ArrowType.Int(32, true);
        }
        else if (prestoType.equals(BigintType.BIGINT)) {
            return new ArrowType.Int(64, true);
        }
        else if (prestoType.equals(RealType.REAL)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }
        else if (prestoType.equals(DoubleType.DOUBLE)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        else if (prestoType instanceof VarcharType) {
            return ArrowType.Utf8.INSTANCE;
        }
        else if (prestoType instanceof VarbinaryType) {
            return ArrowType.Binary.INSTANCE;
        }
        else if (prestoType instanceof DateType) {
            return new ArrowType.Date(DateUnit.DAY);
        }
        else if (prestoType instanceof TimestampType) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        }
        else if (prestoType instanceof ArrayType) {
            return ArrowType.List.INSTANCE;
        }
        else if (prestoType instanceof RowType) {
            return ArrowType.Struct.INSTANCE;
        }
        throw new UnsupportedOperationException("Unsupported Presto type: " + prestoType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnType, columnHandleType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        LanceColumnHandle other = (LanceColumnHandle) obj;
        return Objects.equals(this.columnName, other.columnName) &&
                Objects.equals(this.columnType, other.columnType) &&
                this.columnHandleType == other.columnHandleType;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("nullable", nullable)
                .add("columnHandleType", columnHandleType)
                .toString();
    }
}
