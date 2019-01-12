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
package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.FixedWidthType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.type.DateTimeOperators;

import java.util.concurrent.TimeUnit;

import static io.prestosql.operator.scalar.DateTimeFunctions.diffDate;
import static io.prestosql.operator.scalar.DateTimeFunctions.diffTimestamp;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

public final class SequenceFunction
{
    private static final long MAX_RESULT_ENTRIES = 10_000;
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
    @SqlType("array(date)")
    public static Block sequenceDateDefaultStep(
            @SqlType(StandardTypes.DATE) long start,
            @SqlType(StandardTypes.DATE) long stop)
    {
        return fixedWidthSequence(start, stop, stop >= start ? 1 : -1, DATE);
    }

    @ScalarFunction("sequence")
    @SqlType("array(date)")
    public static Block sequenceDateDayToSecond(
            @SqlType(StandardTypes.DATE) long start,
            @SqlType(StandardTypes.DATE) long stop,
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long step)
    {
        checkCondition(
                step % TimeUnit.DAYS.toMillis(1) == 0,
                INVALID_FUNCTION_ARGUMENT,
                "sequence step must be a day interval if start and end values are dates");
        return fixedWidthSequence(start, stop, step / TimeUnit.DAYS.toMillis(1), DATE);
    }

    @ScalarFunction("sequence")
    @SqlType("array(date)")
    public static Block sequenceDateYearToMonth(
            ConnectorSession session,
            @SqlType(StandardTypes.DATE) long start,
            @SqlType(StandardTypes.DATE) long stop,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long step)
    {
        checkValidStep(start, stop, step);

        int length = toIntExact(diffDate(session, MONTH, start, stop) / step + 1);
        checkMaxEntry(length);

        BlockBuilder blockBuilder = DATE.createBlockBuilder(null, length);

        int value = 0;
        for (int i = 0; i < length; ++i) {
            DATE.writeLong(blockBuilder, DateTimeOperators.datePlusIntervalYearToMonth(start, value));
            value += step;
        }

        return blockBuilder.build();
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
            @SqlType(StandardTypes.TIMESTAMP) long stop,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long step)
    {
        checkValidStep(start, stop, step);

        int length = toIntExact(diffTimestamp(session, MONTH, start, stop) / step + 1);
        checkMaxEntry(length);

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, length);

        int value = 0;
        for (int i = 0; i < length; ++i) {
            BIGINT.writeLong(blockBuilder, DateTimeOperators.timestampPlusIntervalYearToMonth(session, start, value));
            value += step;
        }

        return blockBuilder.build();
    }

    private static Block fixedWidthSequence(long start, long stop, long step, FixedWidthType type)
    {
        checkValidStep(start, stop, step);

        int length = toIntExact((stop - start) / step + 1L);
        checkMaxEntry(length);

        BlockBuilder blockBuilder = type.createBlockBuilder(null, length);
        for (long i = 0, value = start; i < length; ++i, value += step) {
            type.writeLong(blockBuilder, value);
        }
        return blockBuilder.build();
    }

    private static void checkValidStep(long start, long stop, long step)
    {
        checkCondition(
                step != 0,
                INVALID_FUNCTION_ARGUMENT,
                "step must not be zero");
        checkCondition(
                step > 0 ? stop >= start : stop <= start,
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
    }

    private static void checkMaxEntry(int length)
    {
        checkCondition(
                length <= MAX_RESULT_ENTRIES,
                INVALID_FUNCTION_ARGUMENT,
                "result of sequence function must not have more than 10000 entries");
    }
}
