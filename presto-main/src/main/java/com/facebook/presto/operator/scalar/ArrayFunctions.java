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

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

public final class ArrayFunctions
{
    private ArrayFunctions()
    {
    }

    @ScalarFunction(hidden = true)
    @SqlType("array<unknown>")
    public static Slice arrayConstructor()
    {
        return Slices.utf8Slice("[]");
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean contains(@SqlType("array<bigint>") Slice slice, @SqlType(StandardTypes.BIGINT) long value)
    {
        return JsonFunctions.jsonArrayContains(slice, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean contains(@SqlType("array<boolean>") Slice slice, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return JsonFunctions.jsonArrayContains(slice, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean contains(@SqlType("array<double>") Slice slice, @SqlType(StandardTypes.DOUBLE) double value)
    {
        return JsonFunctions.jsonArrayContains(slice, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean contains(@SqlType("array<varchar>") Slice slice, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        return JsonFunctions.jsonArrayContains(slice, value);
    }
}
