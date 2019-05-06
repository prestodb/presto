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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.raptorx.util.Types.getElementType;
import static com.facebook.presto.raptorx.util.Types.getKeyType;
import static com.facebook.presto.raptorx.util.Types.getValueType;
import static com.facebook.presto.raptorx.util.Types.isArrayType;
import static com.facebook.presto.raptorx.util.Types.isMapType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.isLongDecimal;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.util.Objects.requireNonNull;

public class StorageTypeConverter
{
    private final TypeManager typeManager;

    public StorageTypeConverter(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public Type toStorageType(Type type)
    {
        if (type.equals(BOOLEAN) ||
                type.equals(BIGINT) ||
                type.equals(INTEGER) ||
                type.equals(SMALLINT) ||
                type.equals(TINYINT) ||
                type.equals(DOUBLE) ||
                type.equals(REAL) ||
                isCharType(type) ||
                isVarcharType(type) ||
                isLongDecimal(type) ||
                isShortDecimal(type) ||
                type.equals(VARBINARY)) {
            return type;
        }
        if (type.equals(DATE) || type.equals(TIME)) {
            return INTEGER;
        }
        if (type.equals(TIMESTAMP)) {
            return BIGINT;
        }
        if (isArrayType(type)) {
            return new ArrayType(toStorageType(getElementType(type)));
        }
        if (isMapType(type)) {
            return mapType(toStorageType(getKeyType(type)), toStorageType(getValueType(type)));
        }
        throw new PrestoException(NOT_SUPPORTED, "Type not supported: " + type);
    }

    private Type mapType(Type keyType, Type valueType)
    {
        return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }
}
