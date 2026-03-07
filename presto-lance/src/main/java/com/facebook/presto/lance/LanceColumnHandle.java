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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LanceColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;
    private final boolean nullable;

    @JsonCreator
    public LanceColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("nullable") boolean nullable)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.nullable = nullable;
    }

    public LanceColumnHandle(String columnName, Type columnType)
    {
        this(columnName, columnType, true);
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

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnType)
                .setNullable(nullable)
                .build();
    }

    public static Type toPrestoType(ArrowType type)
    {
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
            if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
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
        else if (type instanceof ArrowType.List || type instanceof ArrowType.FixedSizeList) {
            return new ArrayType(RealType.REAL);
        }
        throw new UnsupportedOperationException("Unsupported Arrow type: " + type);
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

        return toPrestoType(type);
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
        return Objects.hash(columnName, columnType);
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
                Objects.equals(this.columnType, other.columnType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("nullable", nullable)
                .toString();
    }
}
