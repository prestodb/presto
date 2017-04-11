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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import static com.facebook.presto.hive.HiveType.HIVE_BINARY;
import static com.facebook.presto.hive.HiveType.HIVE_BOOLEAN;
import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveType.HIVE_TIMESTAMP;
import static com.facebook.presto.hive.HiveUtil.isArrayType;
import static com.facebook.presto.hive.HiveUtil.isMapType;
import static com.facebook.presto.hive.HiveUtil.isRowType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getListTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getVarcharTypeInfo;

public class HiveTypeTranslator
        implements TypeTranslator
{
    @Override
    public TypeInfo translate(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return HIVE_BOOLEAN.getTypeInfo();
        }
        if (BIGINT.equals(type)) {
            return HIVE_LONG.getTypeInfo();
        }
        if (INTEGER.equals(type)) {
            return HIVE_INT.getTypeInfo();
        }
        if (SMALLINT.equals(type)) {
            return HIVE_SHORT.getTypeInfo();
        }
        if (TINYINT.equals(type)) {
            return HIVE_BYTE.getTypeInfo();
        }
        if (REAL.equals(type)) {
            return HIVE_FLOAT.getTypeInfo();
        }
        if (DOUBLE.equals(type)) {
            return HIVE_DOUBLE.getTypeInfo();
        }
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            int varcharLength = varcharType.getLength();
            if (varcharLength <= HiveVarchar.MAX_VARCHAR_LENGTH) {
                return getVarcharTypeInfo(varcharLength);
            }
            else if (varcharLength == VarcharType.UNBOUNDED_LENGTH) {
                return HIVE_STRING.getTypeInfo();
            }
            else {
                throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s. Supported VARCHAR types: VARCHAR(<=%d), VARCHAR.",
                        type, HiveVarchar.MAX_VARCHAR_LENGTH));
            }
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            int charLength = charType.getLength();
            if (charLength <= HiveChar.MAX_CHAR_LENGTH) {
                return getCharTypeInfo(charLength);
            }
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s. Supported CHAR types: CHAR(<=%d).",
                    type, HiveChar.MAX_CHAR_LENGTH));
        }
        if (VARBINARY.equals(type)) {
            return HIVE_BINARY.getTypeInfo();
        }
        if (DATE.equals(type)) {
            return HIVE_DATE.getTypeInfo();
        }
        if (TIMESTAMP.equals(type)) {
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
        }
        if (isArrayType(type)) {
            TypeInfo elementType = translate(type.getTypeParameters().get(0));
            return getListTypeInfo(elementType);
        }
        if (isMapType(type)) {
            TypeInfo keyType = translate(type.getTypeParameters().get(0));
            TypeInfo valueType = translate(type.getTypeParameters().get(1));
            return getMapTypeInfo(keyType, valueType);
        }
        if (isRowType(type)) {
            ImmutableList.Builder<String> fieldNames = ImmutableList.builder();
            for (TypeSignatureParameter parameter : type.getTypeSignature().getParameters()) {
                if (!parameter.isNamedTypeSignature()) {
                    throw new IllegalArgumentException(format("Expected all parameters to be named type, but got %s", parameter));
                }
                NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                fieldNames.add(namedTypeSignature.getName());
            }
            return getStructTypeInfo(
                    fieldNames.build(),
                    type.getTypeParameters().stream()
                            .map(this::translate)
                            .collect(toList()));
        }
        throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s", type));
    }
}
