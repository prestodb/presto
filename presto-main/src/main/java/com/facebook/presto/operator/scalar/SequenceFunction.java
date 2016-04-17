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

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.AbstractFixedWidthType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.DateTimeOperators;
import com.facebook.presto.type.SqlType;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.util.Failures.checkCondition;

public final class SequenceFunction
{
    private static final Slice MONTH = Slices.utf8Slice("month");

    private SequenceFunction() {}

    @Description("Sequence function to generate synthetic arrays")
    @ScalarFunction("sequence")
    @SqlType("array(bigint)")
    public static Block sequence(
            @SqlType(StandardTypes.BIGINT) long start,
            @SqlType(StandardTypes.BIGINT) long stop,
            @SqlType(StandardTypes.BIGINT) long step)
    {
        return fixedWidthSequence(start, stop, step, BIGINT);
    }

    @ScalarFunction("sequence")
    @SqlType("array(bigint)")
    public static Block sequenceDefaultStep(
            @SqlType(StandardTypes.BIGINT) long start,
            @SqlType(StandardTypes.BIGINT) long stop)
    {
        return fixedWidthSequence(start, stop, stop >= start ? 1 : -1, BIGINT);
    }

    @ScalarFunction("sequence")
    @SqlType("array(timestamp)")
    public static Block sequenceTimestampDayToSecond(
            @SqlType(StandardTypes.TIMESTAMP) long start,
            @SqlType(StandardTypes.TIMESTAMP) long stop,
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long step)
    {
        return fixedWidthSequence(start, stop, step, TIMESTAMP);
    }

    @ScalarFunction("sequence")
    @SqlType("array(timestamp)")
    public static Block sequenceTimestampYearToMonth(
            ConnectorSession session,
            @SqlType(StandardTypes.TIMESTAMP) long start,
            @SqlType(StandardTypes.TIMESTAMP) long end,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long step)
    {
        checkCondition(step != 0, INVALID_FUNCTION_ARGUMENT, "interval must not be zero");
        checkCondition(step > 0 ? end >= start : end <= start, INVALID_FUNCTION_ARGUMENT,
                "sequence end value should be greater than or equal to start value if step is greater than zero otherwise end should be less than start");

        int length = Ints.checkedCast(DateTimeFunctions.diffTimestamp(session, MONTH, start, end) / step + 1);

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), length);

        int value = 0;
        for (int i = 0; i < length; ++i) {
            BIGINT.writeLong(blockBuilder, DateTimeOperators.timestampPlusIntervalYearToMonth(session, start, value));
            value += step;
        }

        return blockBuilder.build();
    }

    private static Block fixedWidthSequence(long start, long stop, long step, AbstractFixedWidthType type)
    {
        checkCondition(step != 0, INVALID_FUNCTION_ARGUMENT, "step must not be zero");
        checkCondition(step > 0 ? stop >= start : stop < start, INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than start");

        int length = Ints.checkedCast((stop - start) / step + 1L);

        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), length);
        for (long i = 0, value = start; i < length; ++i, value += step) {
            type.writeLong(blockBuilder, value);
        }
        return blockBuilder.build();
    }
}
