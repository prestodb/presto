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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

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
import static com.google.common.base.Preconditions.checkState;
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
        Type storageType;
        if (type.equals(BOOLEAN) ||
                type.equals(BIGINT) ||
                type.equals(INTEGER) ||
                type.equals(SMALLINT) ||
                type.equals(TINYINT) ||
                type.equals(DOUBLE) ||
                type.equals(REAL) ||
                type.equals(DATE) ||
                isCharType(type) ||
                isVarcharType(type) ||
                isLongDecimal(type) ||
                isShortDecimal(type) ||
                type.equals(VARBINARY)) {
            storageType = type;
        }
        // Raptor does not handle TIME or TIMESTAMP timezone conversion.
        // The timezone used by ORC reader/writer is a placeholder that is not used.
        // Check OrcType::toOrcType() for reference.
        else if (type.equals(TIME) || type.equals(TIMESTAMP)) {
            storageType = BIGINT;
        }
        else if (type instanceof ArrayType) {
            storageType = new ArrayType(toStorageType(((ArrayType) type).getElementType()));
        }
        else if (type instanceof MapType) {
            storageType = mapType(toStorageType(((MapType) type).getKeyType()), toStorageType(((MapType) type).getValueType()));
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Type not supported: " + type);
        }

        // We cannot write different java types because when Raptor calculates stats, it uses the column types rather than storage types.
        // Need to make sure min/max are compliant with both storage and column types.
        checkState(storageType.getJavaType().equals(type.getJavaType()));
        return storageType;
    }

    private Type mapType(Type keyType, Type valueType)
    {
        return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }
}
