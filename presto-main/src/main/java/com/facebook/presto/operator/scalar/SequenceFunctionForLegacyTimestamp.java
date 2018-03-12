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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.scalar.DateTimeFunctionsForLegacyTimestamp.diffTimestamp;
import static com.facebook.presto.operator.scalar.SequenceFunction.checkMaxEntry;
import static com.facebook.presto.operator.scalar.SequenceFunction.checkValidStep;
import static com.facebook.presto.operator.scalar.SequenceFunction.fixedWidthSequence;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.type.DateTimeOperatorsForLegacyTimestamp.timestampPlusIntervalYearToMonth;
import static java.lang.Math.toIntExact;

public final class SequenceFunctionForLegacyTimestamp
{
    private static final Slice MONTH = Slices.utf8Slice("month");

    private SequenceFunctionForLegacyTimestamp() {}

    @ScalarFunction("sequence")
    @SqlType("array(timestamp)")
    public static Block sequenceTimestampDayToSecond(
            @SqlType(StandardTypes.TIMESTAMP) long start,
            @SqlType(StandardTypes.TIMESTAMP) long stop,
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long step)
    {
        // This function is semantically/fundamentally broken with legacy timestamp semantics.
        // It cannot be fixed.
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

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), length);

        int value = 0;
        for (int i = 0; i < length; ++i) {
            BIGINT.writeLong(blockBuilder, timestampPlusIntervalYearToMonth(session, start, value));
            value += step;
        }

        return blockBuilder.build();
    }
}
