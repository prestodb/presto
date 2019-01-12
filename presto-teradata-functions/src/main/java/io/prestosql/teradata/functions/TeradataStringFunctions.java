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
package io.prestosql.teradata.functions;

import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.FunctionDependency;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_16BE;

public final class TeradataStringFunctions
{
    private TeradataStringFunctions()
    {
    }

    @Description("Returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("index")
    @SqlType(StandardTypes.BIGINT)
    public static long index(
            @FunctionDependency(
                    name = "strpos",
                    returnType = StandardTypes.BIGINT,
                    argumentTypes = {StandardTypes.VARCHAR, StandardTypes.VARCHAR})
                    MethodHandle method,
            @SqlType(StandardTypes.VARCHAR) Slice string,
            @SqlType(StandardTypes.VARCHAR) Slice substring)
    {
        try {
            return (long) method.invokeExact(string, substring);
        }
        catch (Throwable t) {
            throwIfInstanceOf(t, Error.class);
            throwIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @Description("suffix starting at given index")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice substring(
            @FunctionDependency(
                    name = "substr",
                    returnType = "varchar(x)",
                    argumentTypes = {"varchar(x)", StandardTypes.BIGINT})
                    MethodHandle method,
            @SqlType("varchar(x)") Slice utf8,
            @SqlType(StandardTypes.BIGINT) long start)
    {
        try {
            return (Slice) method.invokeExact(utf8, start);
        }
        catch (Throwable t) {
            throwIfInstanceOf(t, Error.class);
            throwIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice substring(
            @FunctionDependency(
                    name = "substr",
                    returnType = "varchar(x)",
                    argumentTypes = {"varchar(x)", StandardTypes.BIGINT, StandardTypes.BIGINT})
                    MethodHandle method,
            @SqlType("varchar(x)") Slice utf8,
            @SqlType(StandardTypes.BIGINT) long start,
            @SqlType(StandardTypes.BIGINT) long length)
    {
        try {
            return (Slice) method.invokeExact(utf8, start, length);
        }
        catch (Throwable t) {
            throwIfInstanceOf(t, Error.class);
            throwIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
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
