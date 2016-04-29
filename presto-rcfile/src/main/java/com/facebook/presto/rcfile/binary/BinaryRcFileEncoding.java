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
package com.facebook.presto.rcfile.binary;

import com.facebook.presto.rcfile.ColumnEncoding;
import com.facebook.presto.rcfile.RcFileEncoding;
import com.facebook.presto.spi.type.Type;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.rcfile.RcFileDecoderUtils.checkType;
import static com.facebook.presto.spi.type.Decimals.isLongDecimal;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;

public class BinaryRcFileEncoding
        implements RcFileEncoding
{
    @Override
    public ColumnEncoding booleanEncoding(Type type)
    {
        return new BooleanEncoding(type);
    }

    @Override
    public ColumnEncoding byteEncoding(Type type)
    {
        return new ByteEncoding(type);
    }

    @Override
    public ColumnEncoding shortEncoding(Type type)
    {
        return new ShortEncoding(type);
    }

    @Override
    public ColumnEncoding intEncoding(Type type)
    {
        return longEncoding(type);
    }

    @Override
    public ColumnEncoding longEncoding(Type type)
    {
        return new LongEncoding(type);
    }

    @Override
    public ColumnEncoding decimalEncoding(Type type)
    {
        if (isShortDecimal(type)) {
            return new DecimalEncoding(type);
        }
        else if (isLongDecimal(type)) {
            return new DecimalEncoding(type);
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    @Override
    public ColumnEncoding floatEncoding(Type type)
    {
        return new FloatEncoding(type);
    }

    @Override
    public ColumnEncoding doubleEncoding(Type type)
    {
        return new DoubleEncoding(type);
    }

    @Override
    public ColumnEncoding stringEncoding(Type type)
    {
        return new StringEncoding(type);
    }

    @Override
    public ColumnEncoding binaryEncoding(Type type)
    {
        return new BinaryEncoding(type);
    }

    @Override
    public ColumnEncoding dateEncoding(Type type)
    {
        return new DateEncoding(type);
    }

    @Override
    public ColumnEncoding timestampEncoding(Type type)
    {
        return new TimestampEncoding(type);
    }

    @Override
    public ColumnEncoding listEncoding(Type type, ColumnEncoding elementEncoding)
    {
        return new ListEncoding(type, checkType(elementEncoding, BinaryColumnEncoding.class, "elementEncoding"));
    }

    @Override
    public ColumnEncoding mapEncoding(Type type, ColumnEncoding keyEncoding, ColumnEncoding valueEncoding)
    {
        return new MapEncoding(
                type,
                checkType(keyEncoding, BinaryColumnEncoding.class, "keyEncoding"),
                checkType(valueEncoding, BinaryColumnEncoding.class, "valueEncoding"));
    }

    @Override
    public ColumnEncoding structEncoding(Type type, List<ColumnEncoding> fieldEncodings)
    {
        return new StructEncoding(
                type,
                fieldEncodings.stream()
                        .map(field -> checkType(field, BinaryColumnEncoding.class, "fieldEncoding"))
                        .collect(Collectors.toList()));
    }
}
