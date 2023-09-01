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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

@ScalarFunction("trim_array")
@Description("Remove elements from the end of array")
public final class ArrayTrimFunction
{
    private ArrayTrimFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block trim(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array,
            @SqlType(StandardTypes.BIGINT) long size)
    {
        checkCondition(size >= 0, INVALID_FUNCTION_ARGUMENT, "size must not be negative: %s", size);
        checkCondition(size <= array.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "size must not exceed array cardinality %s: %s", array.getPositionCount(), size);

        return array.getRegion(0, toIntExact(array.getPositionCount() - size));
    }
}
