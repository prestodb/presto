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
package com.facebook.presto.functions;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

public final class TestConnectorSystemFunctions
{
    private TestConnectorSystemFunctions()
    {}

    @Description("Returns modulo of value by numberOfBuckets")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long modulo(
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.BIGINT) long numberOfBuckets)
    {
        return value % numberOfBuckets;
    }

    @Description(("Return the input string"))
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice identity(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return slice;
    }

    @ScalarFunction("sum")
    public static class SumFunction
    {
        private SumFunction()
        {}
        @Description("Returns sum of two integers")
        @SqlType(StandardTypes.BIGINT)
        public static long sum(
                @SqlType(StandardTypes.INTEGER) long a,
                @SqlType(StandardTypes.INTEGER) long b)
        {
            return a + b;
        }
    }
}
