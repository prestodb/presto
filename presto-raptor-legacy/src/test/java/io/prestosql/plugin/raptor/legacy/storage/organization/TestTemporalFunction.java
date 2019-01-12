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
package io.prestosql.plugin.raptor.legacy.storage.organization;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.TimeZone;

import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestTemporalFunction
{
    private static final DateTimeZone PST = DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST"));
    private static final DateTime UTC_TIME = new DateTime(1970, 1, 2, 0, 0, 0, UTC);
    private static final DateTime PST_TIME = new DateTime(1970, 1, 2, 0, 0, 0, PST);

    @Test
    void testDateBlock()
    {
        BlockBuilder blockBuilder = DATE.createBlockBuilder(null, 2);
        DATE.writeLong(blockBuilder, 13);
        DATE.writeLong(blockBuilder, 42);
        Block block = blockBuilder.build();

        // time zone is not used for dates
        TemporalFunction temporalFunction = new TemporalFunction(PST);
        assertEquals(temporalFunction.getDay(DATE, block, 0), 13);
        assertEquals(temporalFunction.getDay(DATE, block, 1), 42);
    }

    @Test
    void testTimestampBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP.createBlockBuilder(null, 4);

        // start and end of UTC day
        TIMESTAMP.writeLong(blockBuilder, UTC_TIME.getMillis());
        TIMESTAMP.writeLong(blockBuilder, UTC_TIME.getMillis() + Duration.ofHours(23).toMillis());

        // start and end of PST day
        TIMESTAMP.writeLong(blockBuilder, PST_TIME.getMillis());
        TIMESTAMP.writeLong(blockBuilder, PST_TIME.getMillis() + Duration.ofHours(23).toMillis());

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
        assertEquals(temporalFunction.getDayFromRange(dateRange(13, 13)), 13);

        // date is determined from lowest shard
        assertEquals(temporalFunction.getDayFromRange(dateRange(2, 5)), 2);

        // time zone is not used for dates
        temporalFunction = new TemporalFunction(PST);
        assertEquals(temporalFunction.getDayFromRange(dateRange(2, 2)), 2);
        assertEquals(temporalFunction.getDayFromRange(dateRange(2, 5)), 2);
    }

    @Test
    void testTimestampShardRange()
    {
        // The time frame should be look like following:
        //  UTC1 ... 8h ... PST1 ... 16h ... UTC2 ... 8h ... PST2 .....
        TemporalFunction temporalFunction = new TemporalFunction(UTC);

        // no time zone mismatch, time range covers full day of day 1
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME.getMillis(), Duration.ofDays(1))), 1);
        // no time zone mismatch, time range covers full day of day 1 and 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME.getMillis(), Duration.ofDays(2))), 2);
        // no time zone mismatch, time range covers 13 hours of day 1 and 11 hours of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME.getMillis() + Duration.ofHours(11).toMillis(), Duration.ofHours(24))), 1);
        // no time zone mismatch, time range covers 11 hours of day 0 and 13 hours of day 1
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME.getMillis() + Duration.ofHours(13).toMillis(), Duration.ofHours(24))), 2);

        // shard boundary time zone shifted +8h from input timezone, time range covers 16h of day 1 and 8h of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME.getMillis(), Duration.ofDays(1))), 1);
        // shard boundary time zone shifted +8h from input timezone, time range covers 16h of day 1 and full day of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME.getMillis(), Duration.ofDays(2))), 2);
        // shard boundary time zone shifted +8h from input timezone, time range covers 7h of day 1 and 17 hour of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME.getMillis() + Duration.ofHours(11).toMillis(), Duration.ofHours(24))), 2);
        // shard boundary time zone shifted +8h from input timezone, time range covers 3h of day 1 and 21 hour of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME.getMillis() + Duration.ofHours(13).toMillis(), Duration.ofHours(24))), 2);

        temporalFunction = new TemporalFunction(PST);
        // shard boundary time zone shifted -8h from input timezone, time range covers 8h of day 0 and 16h of day 1
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME.getMillis(), Duration.ofDays(1))), 1);
        // shard boundary time zone shifted -8h from input timezone, time range covers 8h of day 0 and full day of day 1
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME.getMillis(), Duration.ofDays(2))), 1);
        // shard boundary time zone shifted -8h from input timezone, time range covers 21h of day 1 and 3 hour of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME.getMillis() + Duration.ofHours(11).toMillis(), Duration.ofHours(24))), 1);
        // shard boundary time zone shifted -8h from input timezone, time range covers 19h of day 1 and 5 hour of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(UTC_TIME.getMillis() + Duration.ofHours(13).toMillis(), Duration.ofHours(24))), 1);

        // no time zone mismatch, time range covers full day of day 1
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME.getMillis(), Duration.ofDays(1))), 1);
        // no time zone mismatch, time range covers full day of day 1 and 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME.getMillis(), Duration.ofDays(2))), 2);
        // no time zone mismatch, time range covers 13 hours of day 1 and 11 hours of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME.getMillis() + Duration.ofHours(11).toMillis(), Duration.ofHours(24))), 1);
        // no time zone mismatch, time range covers 11 hours of day 1 and 13 hours of day 2
        assertEquals(temporalFunction.getDayFromRange(timeRange(PST_TIME.getMillis() + Duration.ofHours(13).toMillis(), Duration.ofHours(24))), 2);
    }

    private static ShardRange dateRange(int start, int end)
    {
        return ShardRange.of(new Tuple(DATE, start), new Tuple(DATE, end));
    }

    private static ShardRange timeRange(long start, Duration duration)
    {
        return ShardRange.of(
                new Tuple(TIMESTAMP, start),
                new Tuple(TIMESTAMP, start + duration.toMillis()));
    }
}
