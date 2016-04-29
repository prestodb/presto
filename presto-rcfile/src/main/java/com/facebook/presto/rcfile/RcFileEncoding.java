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
package com.facebook.presto.rcfile;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static java.util.stream.Collectors.toList;

public interface RcFileEncoding
{
    ColumnEncoding booleanEncoding(Type type);

    ColumnEncoding byteEncoding(Type type);

    ColumnEncoding shortEncoding(Type type);

    ColumnEncoding intEncoding(Type type);

    ColumnEncoding longEncoding(Type type);

    ColumnEncoding decimalEncoding(Type type);

    ColumnEncoding floatEncoding(Type type);

    ColumnEncoding doubleEncoding(Type type);

    ColumnEncoding stringEncoding(Type type);

    ColumnEncoding binaryEncoding(Type type);

    ColumnEncoding dateEncoding(Type type);

    ColumnEncoding timestampEncoding(Type type);

    ColumnEncoding listEncoding(Type type, ColumnEncoding elementEncoding);

    ColumnEncoding mapEncoding(Type type, ColumnEncoding keyEncoding, ColumnEncoding valueEncoding);

    ColumnEncoding structEncoding(Type type, List<ColumnEncoding> fieldEncodings);

    default ColumnEncoding getEncoding(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return booleanEncoding(type);
        }
        if (TINYINT.equals(type)) {
            return byteEncoding(type);
        }
        if (SMALLINT.equals(type)) {
            return shortEncoding(type);
        }
        if (INTEGER.equals(type)) {
            return intEncoding(type);
        }
        if (BIGINT.equals(type)) {
            return longEncoding(type);
        }
        if (type instanceof DecimalType) {
            return decimalEncoding(type);
        }
        if (REAL.equals(type)) {
            return floatEncoding(type);
        }
        if (DOUBLE.equals(type)) {
            return doubleEncoding(type);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return stringEncoding(type);
        }
        if (VARBINARY.equals(type)) {
            return binaryEncoding(type);
        }
        if (DATE.equals(type)) {
            return dateEncoding(type);
        }
        if (TIMESTAMP.equals(type)) {
            return timestampEncoding(type);
        }
        String baseType = type.getTypeSignature().getBase();
        if (ARRAY.equals(baseType)) {
            ColumnEncoding elementType = getEncoding(type.getTypeParameters().get(0));
            return listEncoding(type, elementType);
        }
        if (MAP.equals(baseType)) {
            ColumnEncoding keyType = getEncoding(type.getTypeParameters().get(0));
            ColumnEncoding valueType = getEncoding(type.getTypeParameters().get(1));
            return mapEncoding(type, keyType, valueType);
        }
        if (ROW.equals(baseType)) {
            return structEncoding(
                    type,
                    type.getTypeParameters().stream()
                            .map(this::getEncoding)
                            .collect(toList()));
        }
        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }
}
