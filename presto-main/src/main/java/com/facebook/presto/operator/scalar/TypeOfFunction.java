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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;

@Description("textual representation of expression type")
@ScalarFunction("typeof")
public final class TypeOfFunction
{
    private TypeOfFunction() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice typeof(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Object value)
    {
        return Slices.wrappedBuffer(ByteBuffer.wrap(type.getDisplayName().getBytes()));
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice typeof(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Long value)
    {
        return typeof(type, (Object) value);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice typeof(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Double value)
    {
        return typeof(type, (Object) value);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice typeof(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Boolean value)
    {
        return typeof(type, (Object) value);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice typeof(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Void value)
    {
        return typeof(type, (Object) value);
    }
}
