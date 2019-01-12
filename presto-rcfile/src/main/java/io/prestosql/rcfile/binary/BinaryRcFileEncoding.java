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
package io.prestosql.rcfile.binary;

import io.prestosql.rcfile.ColumnEncoding;
import io.prestosql.rcfile.RcFileEncoding;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.stream.Collectors;

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
        return new DecimalEncoding(type);
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
        return new ListEncoding(type, (BinaryColumnEncoding) elementEncoding);
    }

    @Override
    public ColumnEncoding mapEncoding(Type type, ColumnEncoding keyEncoding, ColumnEncoding valueEncoding)
    {
        return new MapEncoding(
                type,
                (BinaryColumnEncoding) keyEncoding,
                (BinaryColumnEncoding) valueEncoding);
    }

    @Override
    public ColumnEncoding structEncoding(Type type, List<ColumnEncoding> fieldEncodings)
    {
        return new StructEncoding(
                type,
                fieldEncodings.stream()
                        .map(field -> (BinaryColumnEncoding) field)
                        .collect(Collectors.toList()));
    }
}
