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
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;

@ScalarFunction(value = "repeat", calledOnNullInput = true)
@Description("Repeat an element for a given number of times")
public final class RepeatFunction
{
    private static final long MAX_RESULT_ENTRIES = 10_000;
    private static final long MAX_SIZE_IN_BYTES = 1_000_000;

    private RepeatFunction() {}

    @SqlType("array(unknown)")
    public static Block repeat(
            @SqlNullable @SqlType("unknown") Boolean element,
            @SqlType(StandardTypes.INTEGER) long count)
    {
        checkValidCount(count);
        checkCondition(element == null, INVALID_FUNCTION_ARGUMENT, "expect null values");
        return RunLengthEncodedBlock.create(UNKNOWN, null, (int) count);
    }

    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block repeat(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Object element,
            @SqlType(StandardTypes.INTEGER) long count)
    {
        checkValidCount(count);
        if (element == null) {
            return RunLengthEncodedBlock.create(type, null, (int) count);
        }
        Block result = RunLengthEncodedBlock.create(type, element, (int) count);
        checkCondition(result.getSizeInBytes() < MAX_SIZE_IN_BYTES, INVALID_FUNCTION_ARGUMENT,
                "result of repeat function must not take more than 1000000 bytes");
        return result;
    }

    private static void checkValidCount(long count)
    {
        checkCondition(count <= MAX_RESULT_ENTRIES, INVALID_FUNCTION_ARGUMENT, "count argument of repeat function must be less than or equal to 10000");
        checkCondition(count >= 0, INVALID_FUNCTION_ARGUMENT, "count argument of repeat function must be greater than or equal to 0");
    }
}
