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
package com.facebook.presto.delta;

import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.delta.standalone.types.ArrayType;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.MapType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.isLongDecimal;
import static com.facebook.presto.common.type.Decimals.isShortDecimal;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.delta.DeltaErrorCode.DELTA_INVALID_PARTITION_VALUE;
import static com.facebook.presto.delta.DeltaErrorCode.DELTA_UNSUPPORTED_COLUMN_TYPE;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Long.parseLong;
import static java.lang.String.format;

/**
 * Contains utility methods to convert Delta data types (and data values) to Presto data types (and data values)
 */
public class DeltaTypeUtils
{
    private DeltaTypeUtils()
    {
    }

    /**
     * Convert given Delta data type to Presto data type signature.
     *
     * @param tableName  Used in error messages when an unsupported data type is encountered.
     * @param columnName Used in error messages when an unsupported data type is encountered.
     * @param deltaType  Data type to convert
     * @return
     */
    public static TypeSignature convertDeltaDataTypePrestoDataType(SchemaTableName tableName, String columnName, DataType deltaType)
    {
        checkArgument(deltaType != null);

        if (deltaType instanceof StructType) {
            StructType deltaStructType = (StructType) deltaType;
            ImmutableList.Builder<TypeSignatureParameter> typeSignatureBuilder = ImmutableList.builder();
            Arrays.stream(deltaStructType.getFields())
                    .forEach(field -> {
                        String rowFieldName = field.getName().toLowerCase(Locale.US);
                        TypeSignature rowFieldType = convertDeltaDataTypePrestoDataType(
                                tableName,
                                columnName + "." + field.getName(),
                                field.getDataType());
                        typeSignatureBuilder.add(TypeSignatureParameter.of(new NamedTypeSignature(
                                Optional.of(new RowFieldName(rowFieldName, false)),
                                rowFieldType)));
                    });
            return new TypeSignature(StandardTypes.ROW, typeSignatureBuilder.build());
        }
        else if (deltaType instanceof ArrayType) {
            ArrayType deltaArrayType = (ArrayType) deltaType;
            TypeSignature elementType = convertDeltaDataTypePrestoDataType(tableName, columnName, deltaArrayType.getElementType());
            return new TypeSignature(ARRAY, ImmutableList.of(TypeSignatureParameter.of(elementType)));
        }
        else if (deltaType instanceof MapType) {
            MapType deltaMapType = (MapType) deltaType;
            TypeSignature keyType = convertDeltaDataTypePrestoDataType(tableName, columnName, deltaMapType.getKeyType());
            TypeSignature valueType = convertDeltaDataTypePrestoDataType(tableName, columnName, deltaMapType.getValueType());
            return new TypeSignature(MAP,
                    ImmutableList.of(TypeSignatureParameter.of(keyType), TypeSignatureParameter.of(valueType)));
        }

        return convertDeltaPrimitiveTypeToPrestoPrimitiveType(tableName, columnName, deltaType).getTypeSignature();
    }

    public static Object convertPartitionValue(
            String columnName,
            String valueString,
            Type type)
    {
        if (valueString == null) {
            return null;
        }

        try {
            if (type.equals(BOOLEAN)) {
                checkArgument(valueString.equalsIgnoreCase("true") || valueString.equalsIgnoreCase("false"));
                return Boolean.valueOf(valueString);
            }
            if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
                return parseLong(valueString);
            }
            if (type.equals(REAL)) {
                return (long) floatToRawIntBits(parseFloat(valueString));
            }
            if (type.equals(DOUBLE)) {
                return parseDouble(valueString);
            }
            if (type instanceof VarcharType) {
                Slice value = utf8Slice(valueString);
                VarcharType varcharType = (VarcharType) type;
                if (!varcharType.isUnbounded() && SliceUtf8.countCodePoints(value) > varcharType.getLengthSafe()) {
                    throw new IllegalArgumentException();
                }
                return value;
            }
            if (type.equals(VARBINARY)) {
                return utf8Slice(valueString);
            }
            if (isShortDecimal(type) || isLongDecimal(type)) {
                com.facebook.presto.common.type.DecimalType decimalType = (com.facebook.presto.common.type.DecimalType) type;
                BigDecimal decimal = new BigDecimal(valueString);
                decimal = decimal.setScale(decimalType.getScale(), BigDecimal.ROUND_UNNECESSARY);
                if (decimal.precision() > decimalType.getPrecision()) {
                    throw new IllegalArgumentException();
                }
                BigInteger unscaledValue = decimal.unscaledValue();
                return isShortDecimal(type) ? unscaledValue.longValue() : Decimals.encodeUnscaledValue(unscaledValue);
            }
            if (type.equals(DATE)) {
                return LocalDate.parse(valueString, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay();
            }
            if (type.equals(TIMESTAMP)) {
                // Delta partition serialized value contains up to the second precision
                return Timestamp.valueOf(valueString).toLocalDateTime().toEpochSecond(ZoneOffset.UTC) * 1_000;
            }
            throw new PrestoException(DELTA_UNSUPPORTED_COLUMN_TYPE,
                    format("Unsupported data type '%s' for partition column %s", type, columnName));
        }
        catch (IllegalArgumentException | DateTimeParseException e) {
            throw new PrestoException(
                    DELTA_INVALID_PARTITION_VALUE,
                    format("Can not parse partition value '%s' of type '%s' for partition column '%s'", valueString, type, columnName),
                    e);
        }
    }

    private static Type convertDeltaPrimitiveTypeToPrestoPrimitiveType(SchemaTableName tableName, String columnName, DataType deltaType)
    {
        if (deltaType instanceof BinaryType) {
            return VARBINARY;
        }
        else if (deltaType instanceof BooleanType) {
            return BOOLEAN;
        }
        else if (deltaType instanceof ByteType) {
            return TINYINT;
        }
        else if (deltaType instanceof DateType) {
            return DATE;
        }
        else if (deltaType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) deltaType;
            return createDecimalType(decimalType.getPrecision(), decimalType.getScale());
        }
        else if (deltaType instanceof DoubleType) {
            return DOUBLE;
        }
        else if (deltaType instanceof FloatType) {
            return REAL;
        }
        else if (deltaType instanceof IntegerType) {
            return INTEGER;
        }
        else if (deltaType instanceof LongType) {
            return BIGINT;
        }
        else if (deltaType instanceof ShortType) {
            return SMALLINT;
        }
        else if (deltaType instanceof StringType) {
            return createUnboundedVarcharType();
        }
        else if (deltaType instanceof TimestampType) {
            return TIMESTAMP;
        }

        throw new PrestoException(DELTA_UNSUPPORTED_COLUMN_TYPE,
                format("Column '%s' in Delta table %s contains unsupported data type: %s", columnName, tableName, deltaType.getCatalogString()));
    }
}
