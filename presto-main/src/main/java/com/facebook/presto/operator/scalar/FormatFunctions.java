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

import com.facebook.presto.cli.FormatUtils;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public final class FormatFunctions
{
    private FormatFunctions() {}

    @Description("format to human readable count value")
    @ScalarFunction("format_count")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice formatCount(@SqlType(StandardTypes.BIGINT) long num)
    {
        return Slices.utf8Slice(FormatUtils.formatCount(num));
    }

    @Description("format to human readable count value")
    @ScalarFunction("format_count")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice formatCountDouble(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Slices.utf8Slice(FormatUtils.formatCount((long) num));
    }
}
