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
package com.facebook.presto.teradata.functions;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.LiteralParameters;
import com.facebook.presto.type.SqlType;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static java.nio.charset.StandardCharsets.UTF_16BE;

public final class TeradataStringFunctions
{
    private TeradataStringFunctions()
    {
    }

    @Description("Returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("index")
    @SqlType(StandardTypes.BIGINT)
    public static long index(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice substring)
    {
        return StringFunctions.stringPosition(string, substring);
    }

    @Description("suffix starting at given index")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice substring(@SqlType("varchar(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start)
    {
        return StringFunctions.substr(utf8, start);
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice substring(@SqlType("varchar(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length)
    {
        return StringFunctions.substr(utf8, start, length);
    }

    @Description("Returns the hexadecimal representation of the UTF-16BE encoding of the argument")
    @ScalarFunction("char2hexint")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice char2HexInt(@SqlType(StandardTypes.VARCHAR) Slice string)
    {
        Slice utf16 = Slices.wrappedBuffer(UTF_16BE.encode(string.toStringUtf8()));
        String encoded = BaseEncoding.base16().encode(utf16.getBytes());
        return Slices.utf8Slice(encoded);
    }
}
