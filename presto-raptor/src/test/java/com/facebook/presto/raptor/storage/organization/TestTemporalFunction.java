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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.TimeZone;

import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestTemporalFunction
{
    private static final DateTimeZone PST = DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST"));
    private static final DateTime UTC_TIME_1 = new DateTime(1970, 1, 2, 0, 0, 0, UTC);
    private static final DateTime PST_TIME_1 = new DateTime(1970, 1, 2, 0, 0, 0, PST);

    @Test
    void testDateBlock()
    {
        TemporalFunction temporalFunction = new TemporalFunction(PST);
        BlockBuilder blockBuilder = DATE.createBlockBuilder(new BlockBuilderStatus(), 2);
        DATE.writeLong(blockBuilder, 1);
        DATE.writeLong(blockBuilder, 2);
        Block block = blockBuilder.build();

        assertEquals(temporalFunction.getDay(DATE, block, 0), 1);
        assertEquals(temporalFunction.getDay(DATE, block, 1), 2);
    }

    @Test
    void testTimestampBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP.createBlockBuilder(new BlockBuilderStatus(), 4);

        // beginning of utc day 1
        TIMESTAMP.writeLong(blockBuilder, UTC_TIME_1.getMillis());
        // end of utc day 1
        TIMESTAMP.writeLong(blockBuilder, UTC_TIME_1.getMillis() + Duration.ofHours(23).toMillis());
        // beginning of pst day 1
        TIMESTAMP.writeLong(blockBuilder, PST_TIME_1.getMillis());
        // end of pst day 1
        TIMESTAMP.writeLong(blockBuilder, PST_TIME_1.getMillis() + Duration.ofHours(23).toMillis());
        Block block = blockBuilder.build();

        TemporalFunction temporalFunction = new TemporalFunction(UTC);
        assertEquals(temporalFunction.getDay(TIMESTAMP, block, 0), 1);
        assertEquals(temporalFunction.getDay(TIMESTAMP, block, 1), 1);
        assertEquals(temporalFunction.getDay(TIMESTAMP, block, 2), 1);
        assertEquals(temporalFunction.getDay(TIMESTAMP, block, 3), 2);

        temporalFunction = new TemporalFunction(PST);
        assertEquals(temporalFunction.getDay(TIMESTAMP, block, 0), 0);
        assertEquals(temporalFunction.getDay(TIMESTAMP, block, 1), 1);
        assertEquals(temporalFunction.getDay(TIMESTAMP, block, 2), 1);
        assertEquals(temporalFunction.getDay(TIMESTAMP, block, 3), 1);
    }

    @Test
    void testDateShardRange()
    {
        TemporalFunction temporalFunction = new TemporalFunction(UTC);
        assertEquals(temporalFunction.getDayFromRange(dateRange(2, 2)), 2);

        // date is determined from lowest shard
        assertEquals(temporalFunction.getDayFromRange(dateRange(0, 2)), 0);

        // switch timezone should not affect date
        temporalFunction = new TemporalFunction(PST);
        assertEquals(temporalFunction.getDayFromRange(dateRange(2, 2)), 2);
        assertEquals(temporalFunction.getDayFromRange(dateRange(0, 2)), 0);
    }

    @Test
    void testTimestampShardRange()
    {
        // The time frame should be look like following:
        //  UTC1 ... 8h ... PST1 ... 16h ... UTC2 ... 8h ... PST2 .....
        TemporalFunction temporalFunction = new TemporalFunction(UTC);

        // No time zone mismatching, time range cover full day of day 1
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME_1.getMillis(), Duration.ofDays(1))), 1);
        // No time zone mismatching, time range cover full day of day 1 and 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME_1.getMillis(), Duration.ofDays(2))), 2);
        // No time zone mismatching, time range cover 13 hours of day 1 and 11 hours of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME_1.getMillis() + Duration.ofHours(11).toMillis(), Duration.ofHours(24))), 1);
        // No time zone mismatching, time range cover 11 hours of day 0 and 13 hours of day 1
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME_1.getMillis() + Duration.ofHours(13).toMillis(), Duration.ofHours(24))), 2);

        // shard boundary time zone shifted +8h from input timezone, time range cover 16h of day 1 and 8h of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME_1.getMillis(), Duration.ofDays(1))), 1);
        // shard boundary time zone shifted +8h from input timezone, time range cover 16h of day 1 and full day of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME_1.getMillis(), Duration.ofDays(2))), 2);
        // shard boundary time zone shifted +8h from input timezone, time range cover 7h of day 1 and 17 hour of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME_1.getMillis() + Duration.ofHours(11).toMillis(), Duration.ofHours(24))), 2);
        // shard boundary time zone shifted +8h from input timezone, time range cover 3h of day 1 and 21 hour of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME_1.getMillis() + Duration.ofHours(13).toMillis(), Duration.ofHours(24))), 2);

        temporalFunction = new TemporalFunction(PST);
        // shard boundary time zone shifted -8h from input timezone, time range cover 8h of day 0 and 16h of day 1
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME_1.getMillis(), Duration.ofDays(1))), 1);
        // shard boundary time zone shifted -8h from input timezone, time range cover 8h of day 0 and full day of day 1
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME_1.getMillis(), Duration.ofDays(2))), 1);
        // shard boundary time zone shifted -8h from input timezone, time range cover 21h of day 1 and 3 hour of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME_1.getMillis() + Duration.ofHours(11).toMillis(), Duration.ofHours(24))), 1);
        // shard boundary time zone shifted -8h from input timezone, time range cover 19h of day 1 and 5 hour of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME_1.getMillis() + Duration.ofHours(13).toMillis(), Duration.ofHours(24))), 1);

        // No time zone mismatching, time range cover full day of day 1
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME_1.getMillis(), Duration.ofDays(1))), 1);
        // No time zone mismatching, time range cover full day of day 1 and 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME_1.getMillis(), Duration.ofDays(2))), 2);
        // No time zone mismatching, time range cover 13 hours of day 1 and 11 hours of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME_1.getMillis() + Duration.ofHours(11).toMillis(), Duration.ofHours(24))), 1);
        // No time zone mismatching, time range cover 11 hours of day 1 and 13 hours of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME_1.getMillis() + Duration.ofHours(13).toMillis(), Duration.ofHours(24))), 2);
    }

    private ShardRange dateRange(int start, int end)
    {
        return ShardRange.of(new Tuple(DATE, start), new Tuple(DATE, end));
    }

    private ShardRange timeRange(long start, Duration duration)
    {
        return ShardRange.of(
                new Tuple(TIMESTAMP, start),
                new Tuple(TIMESTAMP, start + duration.toMillis()));
    }
}
