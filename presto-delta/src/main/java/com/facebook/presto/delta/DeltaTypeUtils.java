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

import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
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

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
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
import static com.facebook.presto.delta.DeltaErrorCode.DELTA_UNSUPPORTED_COLUMN_TYPE;
import static com.google.common.base.Preconditions.checkArgument;
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
