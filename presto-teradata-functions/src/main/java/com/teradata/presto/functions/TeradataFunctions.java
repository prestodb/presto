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
package com.teradata.presto.functions;

import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import static java.nio.charset.StandardCharsets.UTF_16BE;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class TeradataFunctions
{
    private TeradataFunctions()
    {
    }

    @Description("Teradata extension to the ANSI SQL-2003 standard. Returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("index")
    @SqlType(StandardTypes.BIGINT)
    public static long index(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice substring)
    {
        return StringFunctions.stringPosition(string, substring);
    }

    @Description("Teradata extension to the ANSI SQL-2003 standard. Returns string with HEX representation of UTF16BE encoding of the argument")
    @ScalarFunction("char2hexint")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice char2HexInt(@SqlType(StandardTypes.VARCHAR) Slice string)
    {
        ByteBuffer utf8Buffer = string.toByteBuffer();
        ByteBuffer utf16Buffer = UTF_16BE.encode(UTF_8.decode(utf8Buffer));
        try {
            ByteBuffer hexBuffer = new HexEncoder().encode(utf16Buffer.asCharBuffer());
            return Slices.wrappedBuffer(hexBuffer);
        }
        catch (CharacterCodingException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
        }
    }
}
