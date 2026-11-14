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
package com.facebook.presto.parquet;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.UuidType.UUID;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Inverse of {@link com.facebook.presto.parquet.writer.ParquetSchemaConverter}: derives a Presto type
 * tree from a Parquet schema (used by {@code read_files} which has no metastore type to resolve against).
 *
 * <p>{@code INT96} → {@code TIMESTAMP}. {@code TIMESTAMP}/{@code TIME} with {@code isAdjustedToUTC=true}
 * → the {@code WITH TIME ZONE} variant. {@code TIMESTAMP_NANOS} and {@code TIME_NANOS} are rejected —
 * the reader has no nanosecond-aware decoder and would silently emit values 10^6× too large.
 */
public final class ParquetToPrestoTypeConverter
{
    private ParquetToPrestoTypeConverter() {}

    public static Type fromParquet(org.apache.parquet.schema.Type parquetType, TypeManager typeManager)
    {
        requireNonNull(parquetType, "parquetType is null");
        requireNonNull(typeManager, "typeManager is null");

        if (parquetType.isPrimitive()) {
            return fromPrimitive(parquetType.asPrimitiveType());
        }
        return fromGroup(parquetType.asGroupType(), typeManager);
    }

    private static Type fromGroup(GroupType group, TypeManager typeManager)
    {
        LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
        if (annotation instanceof ListLogicalTypeAnnotation) {
            return fromList(group, typeManager);
        }
        if (annotation instanceof MapLogicalTypeAnnotation) {
            return fromMap(group, typeManager);
        }
        return fromStruct(group, typeManager);
    }

    private static Type fromList(GroupType list, TypeManager typeManager)
    {
        // Accept both 3-level (list -> repeated group -> element) and legacy 2-level encodings.
        if (list.getFieldCount() != 1) {
            throw new IllegalArgumentException(format("Invalid Parquet LIST type, expected 1 field, got %s: %s", list.getFieldCount(), list));
        }
        org.apache.parquet.schema.Type repeated = list.getType(0);
        Type elementType;
        if (repeated.isPrimitive() || repeated.asGroupType().getFieldCount() != 1) {
            elementType = fromParquet(repeated, typeManager);
        }
        else {
            elementType = fromParquet(repeated.asGroupType().getType(0), typeManager);
        }
        return new ArrayType(elementType);
    }

    private static Type fromMap(GroupType map, TypeManager typeManager)
    {
        if (map.getFieldCount() != 1) {
            throw new IllegalArgumentException(format("Invalid Parquet MAP type, expected 1 repeated field, got %s: %s", map.getFieldCount(), map));
        }
        GroupType keyValue = map.getType(0).asGroupType();
        if (keyValue.getFieldCount() != 2) {
            throw new IllegalArgumentException(format("Invalid Parquet MAP key_value, expected 2 fields, got %s: %s", keyValue.getFieldCount(), keyValue));
        }
        Type keyType = fromParquet(keyValue.getType(0), typeManager);
        Type valueType = fromParquet(keyValue.getType(1), typeManager);
        return typeManager.getParameterizedType(
                MAP,
                ImmutableList.of(
                        TypeSignatureParameter.of(keyType.getTypeSignature()),
                        TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    private static Type fromStruct(GroupType struct, TypeManager typeManager)
    {
        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        for (org.apache.parquet.schema.Type field : struct.getFields()) {
            fields.add(new RowType.Field(java.util.Optional.of(field.getName()), fromParquet(field, typeManager)));
        }
        return RowType.from(fields.build());
    }

    private static Type fromPrimitive(PrimitiveType primitive)
    {
        LogicalTypeAnnotation annotation = primitive.getLogicalTypeAnnotation();
        PrimitiveTypeName name = primitive.getPrimitiveTypeName();

        if (annotation instanceof DecimalLogicalTypeAnnotation) {
            DecimalLogicalTypeAnnotation decimal = (DecimalLogicalTypeAnnotation) annotation;
            return DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale());
        }
        if (annotation instanceof DateLogicalTypeAnnotation) {
            return DATE;
        }
        if (annotation instanceof TimestampLogicalTypeAnnotation) {
            TimestampLogicalTypeAnnotation timestamp = (TimestampLogicalTypeAnnotation) annotation;
            if (timestamp.getUnit() == TimeUnit.NANOS) {
                throw new IllegalArgumentException(format("Unsupported Parquet TIMESTAMP unit NANOS: %s", primitive));
            }
            return timestamp.isAdjustedToUTC() ? TIMESTAMP_WITH_TIME_ZONE : TIMESTAMP;
        }
        if (annotation instanceof TimeLogicalTypeAnnotation) {
            TimeLogicalTypeAnnotation time = (TimeLogicalTypeAnnotation) annotation;
            if (time.getUnit() == TimeUnit.NANOS) {
                throw new IllegalArgumentException(format("Unsupported Parquet TIME unit NANOS: %s", primitive));
            }
            return time.isAdjustedToUTC() ? TIME_WITH_TIME_ZONE : TIME;
        }
        if (annotation instanceof StringLogicalTypeAnnotation
                || annotation instanceof EnumLogicalTypeAnnotation
                || annotation instanceof JsonLogicalTypeAnnotation) {
            return VARCHAR;
        }
        if (annotation instanceof UUIDLogicalTypeAnnotation) {
            return UUID;
        }
        if (annotation instanceof IntLogicalTypeAnnotation) {
            IntLogicalTypeAnnotation intAnnotation = (IntLogicalTypeAnnotation) annotation;
            return fromIntAnnotation(intAnnotation.getBitWidth(), intAnnotation.isSigned());
        }

        switch (name) {
            case BOOLEAN:
                return BOOLEAN;
            case INT32:
                return INTEGER;
            case INT64:
                return BIGINT;
            case INT96:
                return TIMESTAMP;
            case FLOAT:
                return REAL;
            case DOUBLE:
                return DOUBLE;
            case BINARY:
                return VARBINARY;
            case FIXED_LEN_BYTE_ARRAY:
                return VARBINARY;
            default:
                throw new IllegalArgumentException(format("Unsupported Parquet primitive type: %s (name=%s)", primitive, name));
        }
    }

    private static Type fromIntAnnotation(int bitWidth, boolean signed)
    {
        if (signed) {
            switch (bitWidth) {
                case 8:
                    return TINYINT;
                case 16:
                    return SMALLINT;
                case 32:
                    return INTEGER;
                case 64:
                    return BIGINT;
                default:
                    throw new IllegalArgumentException(format("Unsupported signed INT bit width: %s", bitWidth));
            }
        }
        // Unsigned: widen to the next signed type. UINT_64 has no safe widening.
        switch (bitWidth) {
            case 8:
                return SMALLINT;
            case 16:
                return INTEGER;
            case 32:
                return BIGINT;
            case 64:
                throw new IllegalArgumentException("Unsigned 64-bit integer is not supported (no signed Presto type can represent the full range)");
            default:
                throw new IllegalArgumentException(format("Unsupported unsigned INT bit width: %s", bitWidth));
        }
    }
}
