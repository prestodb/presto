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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

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
        checkCondition(element == null, INVALID_FUNCTION_ARGUMENT, "expect null values");
        BlockBuilder blockBuilder = createBlockBuilder(UNKNOWN, count);
        return repeatNullValues(blockBuilder, count);
    }

    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block repeat(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Object element,
            @SqlType(StandardTypes.INTEGER) long count)
    {
        BlockBuilder blockBuilder = createBlockBuilder(type, count);
        if (element == null) {
            return repeatNullValues(blockBuilder, count);
        }
        if (count > 0) {
            type.writeObject(blockBuilder, element);
            checkMaxSize(blockBuilder.getSizeInBytes(), count);
        }
        for (int i = 1; i < count; i++) {
            type.writeObject(blockBuilder, element);
        }
        return blockBuilder.build();
    }

    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block repeat(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Long element,
            @SqlType(StandardTypes.INTEGER) long count)
    {
        BlockBuilder blockBuilder = createBlockBuilder(type, count);
        if (element == null) {
            return repeatNullValues(blockBuilder, count);
        }
        for (int i = 0; i < count; i++) {
            type.writeLong(blockBuilder, element);
        }
        return blockBuilder.build();
    }

    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block repeat(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Slice element,
            @SqlType(StandardTypes.INTEGER) long count)
    {
        BlockBuilder blockBuilder = createBlockBuilder(type, count);
        if (element == null) {
            return repeatNullValues(blockBuilder, count);
        }
        if (count > 0) {
            type.writeSlice(blockBuilder, element);
            checkMaxSize(blockBuilder.getSizeInBytes(), count);
        }
        for (int i = 1; i < count; i++) {
            type.writeSlice(blockBuilder, element);
        }
        return blockBuilder.build();
    }

    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block repeat(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Boolean element,
            @SqlType(StandardTypes.INTEGER) long count)
    {
        BlockBuilder blockBuilder = createBlockBuilder(type, count);
        if (element == null) {
            return repeatNullValues(blockBuilder, count);
        }
        for (int i = 0; i < count; i++) {
            type.writeBoolean(blockBuilder, element);
        }
        return blockBuilder.build();
    }

    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block repeat(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Double element,
            @SqlType(StandardTypes.INTEGER) long count)
    {
        BlockBuilder blockBuilder = createBlockBuilder(type, count);
        if (element == null) {
            return repeatNullValues(blockBuilder, count);
        }
        for (int i = 0; i < count; i++) {
            type.writeDouble(blockBuilder, element);
        }
        return blockBuilder.build();
    }

    private static BlockBuilder createBlockBuilder(Type type, long count)
    {
        checkCondition(count <= MAX_RESULT_ENTRIES, INVALID_FUNCTION_ARGUMENT, "count argument of repeat function must be less than or equal to 10000");
        checkCondition(count >= 0, INVALID_FUNCTION_ARGUMENT, "count argument of repeat function must be greater than or equal to 0");
        return type.createBlockBuilder(null, toIntExact(count));
    }

    private static Block repeatNullValues(BlockBuilder blockBuilder, long count)
    {
        for (int i = 0; i < count; i++) {
            blockBuilder.appendNull();
        }
        return blockBuilder.build();
    }

    private static void checkMaxSize(long bytes, long count)
    {
        checkCondition(
                bytes <= (MAX_SIZE_IN_BYTES + count) / count,
                INVALID_FUNCTION_ARGUMENT,
                "result of repeat function must not take more than 1000000 bytes");
    }
}
