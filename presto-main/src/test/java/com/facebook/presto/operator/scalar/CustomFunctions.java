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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

public final class CustomFunctions
{
    private CustomFunctions() {}

    @ScalarFunction
    @SqlType(BigintType.class)
    public static long customAdd(@SqlType(BigintType.class) long x, @SqlType(BigintType.class) long y)
    {
        return x + y;
    }

    @ScalarFunction("custom_is_null")
    @SqlType(BooleanType.class)
    public static boolean customIsNullVarchar(@Nullable @SqlType(VarcharType.class) Slice slice)
    {
        return slice == null;
    }

    @ScalarFunction("custom_is_null")
    @SqlType(BooleanType.class)
    public static boolean customIsNullBigint(@Nullable @SqlType(BigintType.class) Long value)
    {
        return value == null;
    }
}
