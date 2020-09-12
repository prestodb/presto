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

import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
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
import static com.facebook.presto.hive.metastore.MetastoreUtil.isArrayType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isMapType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isRowType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
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
    public TypeInfo translate(Type type, Optional<HiveType> defaultHiveType)
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
            TypeInfo elementType = translate(type.getTypeParameters().get(0), defaultHiveType);
            return getListTypeInfo(elementType);
        }
        if (isMapType(type)) {
            TypeInfo keyType = translate(type.getTypeParameters().get(0), defaultHiveType);
            TypeInfo valueType = translate(type.getTypeParameters().get(1), defaultHiveType);
            return getMapTypeInfo(keyType, valueType);
        }
        if (isRowType(type)) {
            ImmutableList.Builder<String> fieldNames = ImmutableList.builder();
            for (TypeSignatureParameter parameter : type.getTypeSignature().getParameters()) {
                if (!parameter.isNamedTypeSignature()) {
                    throw new IllegalArgumentException(format("Expected all parameters to be named type, but got %s", parameter));
                }
                NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                if (!namedTypeSignature.getName().isPresent()) {
                    throw new PrestoException(NOT_SUPPORTED, format("Anonymous row type is not supported in Hive. Please give each field a name: %s", type));
                }
                fieldNames.add(namedTypeSignature.getName().get());
            }
            return getStructTypeInfo(
                    fieldNames.build(),
                    type.getTypeParameters().stream()
                            .map(t -> translate(t, defaultHiveType))
                            .collect(toList()));
        }
        return defaultHiveType
                .orElseThrow(() -> new PrestoException(NOT_SUPPORTED, format("No default Hive type provided for unsupported Hive type: %s", type)))
                .getTypeInfo();
    }

    public static boolean isSupportedHiveType(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return true;
        }
        if (BIGINT.equals(type)) {
            return true;
        }
        if (INTEGER.equals(type)) {
            return true;
        }
        if (SMALLINT.equals(type)) {
            return true;
        }
        if (TINYINT.equals(type)) {
            return true;
        }
        if (REAL.equals(type)) {
            return true;
        }
        if (DOUBLE.equals(type)) {
            return true;
        }
        if (type instanceof VarcharType) {
            return true;
        }
        if (type instanceof CharType) {
            return true;
        }
        if (VARBINARY.equals(type)) {
            return true;
        }
        if (DATE.equals(type)) {
            return true;
        }
        if (TIMESTAMP.equals(type)) {
            return true;
        }
        if (type instanceof DecimalType) {
            return true;
        }
        if (isArrayType(type)) {
            return isSupportedHiveType(type.getTypeParameters().get(0));
        }
        if (isMapType(type)) {
            return isSupportedHiveType(type.getTypeParameters().get(0)) &&
                    isSupportedHiveType(type.getTypeParameters().get(1));
        }
        if (isRowType(type)) {
            return type.getTypeParameters().stream()
                    .allMatch(HiveTypeTranslator::isSupportedHiveType);
        }
        return false;
    }
}
