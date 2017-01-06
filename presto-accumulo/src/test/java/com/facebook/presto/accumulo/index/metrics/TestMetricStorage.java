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
package com.facebook.presto.accumulo.index.metrics;

import com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.type.TimestampType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.Map;

import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.DAY;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.HOUR;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MILLISECOND;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MINUTE;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.SECOND;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.getTruncatedTimestamps;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.splitTimestampRange;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestMetricStorage
{
    private static final DateTimeFormatter PARSER = ISODateTimeFormat.dateTimeParser();
    private static final Long TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:05.321+0000").getMillis();
    private static final Long SECOND_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:05.000+0000").getMillis();
    private static final Long MINUTE_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:00.000+0000").getMillis();
    private static final Long HOUR_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:00:00.000+0000").getMillis();
    private static final Long DAY_TIMESTAMP = PARSER.parseDateTime("2001-08-22T00:00:00.000+0000").getMillis();

    private static final LexicoderRowSerializer SERIALIZER = new LexicoderRowSerializer();

    @Test
    public void testTruncateTimestamp()
    {
        Map<TimestampPrecision, Long> expected = ImmutableMap.of(
                SECOND, SECOND_TIMESTAMP,
                MINUTE, MINUTE_TIMESTAMP,
                HOUR, HOUR_TIMESTAMP,
                DAY, DAY_TIMESTAMP);
        assertEquals(getTruncatedTimestamps(TIMESTAMP), expected);
    }

    @Test
    public void testSplitTimestampRangeNone()
    {
        String startTime = "2001-08-02T00:30:05.321+0000";
        String endTime = "2001-08-02T00:30:05.456+0000";
        Range splitRange = new Range(text(startTime), text(endTime));

        ImmutableMultimap<TimestampPrecision, Range> expected = ImmutableMultimap.of(MILLISECOND, splitRange);
        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeNoneExclusive()
    {
        String startTime = "2001-08-02T00:30:05.321+0000";
        String endTime = "2001-08-02T00:30:05.456+0000";
        Range splitRange = new Range(text(startTime), false, text(endTime), false);

        ImmutableMultimap<TimestampPrecision, Range> expected = ImmutableMultimap.of(MILLISECOND, splitRange);
        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeSecond()
    {
        String startTime = "2001-08-02T00:30:05.321+0000";
        String endTime = "2001-08-02T00:30:12.456+0000";
        Range splitRange = new Range(text(startTime), text(endTime));

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), text("2001-08-02T00:30:05.999+0000")))
                .put(SECOND, new Range(text("2001-08-02T00:30:06.000+0000"), text("2001-08-02T00:30:11.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-02T00:30:12.000+0000"), text(endTime)));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeSecondExclusive()
    {
        String startTime = "2001-08-02T00:30:05.321+0000";
        String endTime = "2001-08-02T00:30:12.456+0000";
        Range splitRange = new Range(text(startTime), false, text(endTime), false);

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), false, text("2001-08-02T00:30:05.999+0000"), true))
                .put(SECOND, new Range(text("2001-08-02T00:30:06.000+0000"), text("2001-08-02T00:30:11.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-02T00:30:12.000+0000"), true, text(endTime), false));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeMinute()
    {
        String startTime = "2001-08-02T00:30:58.321+0000";
        String endTime = "2001-08-02T00:33:03.456+0000";
        Range splitRange = new Range(text(startTime), text(endTime));

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), text("2001-08-02T00:30:58.999+0000")))
                .put(SECOND, new Range(text("2001-08-02T00:30:59.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T00:31:00.000+0000"), text("2001-08-02T00:32:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-02T00:33:00.000+0000"), text("2001-08-02T00:33:02.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-02T00:33:03.000+0000"), text(endTime)));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeMinuteExclusive()
    {
        String startTime = "2001-08-02T00:30:58.321+0000";
        String endTime = "2001-08-02T00:33:03.456+0000";
        Range splitRange = new Range(text(startTime), false, text(endTime), false);

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), false, text("2001-08-02T00:30:58.999+0000"), true))
                .put(SECOND, new Range(text("2001-08-02T00:30:59.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T00:31:00.000+0000"), text("2001-08-02T00:32:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-02T00:33:00.000+0000"), text("2001-08-02T00:33:02.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-02T00:33:03.000+0000"), true, text(endTime), false));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeHour()
    {
        String startTime = "2001-08-02T00:58:58.321+0000";
        String endTime = "2001-08-02T03:03:03.456+0000";
        Range splitRange = new Range(text(startTime), text(endTime));

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), text("2001-08-02T00:58:58.999+0000")))
                .put(SECOND, new Range(text("2001-08-02T00:58:59.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T00:59:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-02T01:00:00.000+0000"), text("2001-08-02T02:00:00.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T03:00:00.000+0000"), text("2001-08-02T03:02:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-02T03:03:00.000+0000"), text("2001-08-02T03:03:02.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-02T03:03:03.000+0000"), text(endTime)));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeHourExclusive()
    {
        String startTime = "2001-08-02T00:58:58.321+0000";
        String endTime = "2001-08-02T03:03:03.456+0000";
        Range splitRange = new Range(text(startTime), false, text(endTime), false);

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), false, text("2001-08-02T00:58:58.999+0000"), true))
                .put(SECOND, new Range(text("2001-08-02T00:58:59.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T00:59:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-02T01:00:00.000+0000"), text("2001-08-02T02:00:00.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T03:00:00.000+0000"), text("2001-08-02T03:02:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-02T03:03:00.000+0000"), text("2001-08-02T03:03:02.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-02T03:03:03.000+0000"), true, text(endTime), false));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeDay()
    {
        String startTime = "2001-08-02T22:58:58.321+0000";
        String endTime = "2001-08-05T01:03:03.456+0000";
        Range splitRange = new Range(text(startTime), text(endTime));

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), text("2001-08-02T22:58:58.999+0000")))
                .put(SECOND, new Range(text("2001-08-02T22:58:59.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T22:59:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-02T23:00:00.000+0000")))
                .put(DAY, new Range(text("2001-08-03T00:00:00.000+0000"), text("2001-08-04T00:00:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-05T00:00:00.000+0000")))
                .put(MINUTE, new Range(text("2001-08-05T01:00:00.000+0000"), text("2001-08-05T01:02:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-05T01:03:00.000+0000"), text("2001-08-05T01:03:02.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-05T01:03:03.000+0000"), text(endTime)));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeDayExclusive()
    {
        String startTime = "2001-08-02T22:58:58.321+0000";
        String endTime = "2001-08-05T01:03:03.456+0000";
        Range splitRange = new Range(text(startTime), false, text(endTime), false);

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MILLISECOND, new Range(text(startTime), false, text("2001-08-02T22:58:58.999+0000"), true))
                .put(SECOND, new Range(text("2001-08-02T22:58:59.000+0000")))
                .put(MINUTE, new Range(text("2001-08-02T22:59:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-02T23:00:00.000+0000")))
                .put(DAY, new Range(text("2001-08-03T00:00:00.000+0000"), text("2001-08-04T00:00:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-05T00:00:00.000+0000")))
                .put(MINUTE, new Range(text("2001-08-05T01:00:00.000+0000"), text("2001-08-05T01:02:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-05T01:03:00.000+0000"), text("2001-08-05T01:03:02.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-05T01:03:03.000+0000"), true, text(endTime), false));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeHourRange()
    {
        String startTime = "2001-08-02T22:58:00.000+0000";
        String endTime = "2001-08-05T02:02:00.000+0000";
        Range splitRange = new Range(text(startTime), text(endTime));

        ImmutableMultimap.Builder<TimestampPrecision, Range> builder = ImmutableMultimap.builder();
        builder.put(MINUTE, new Range(text(startTime), text("2001-08-02T22:59:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-02T23:00:00.000+0000")))
                .put(DAY, new Range(text("2001-08-03T00:00:00.000+0000"), text("2001-08-04T00:00:00.000+0000")))
                .put(HOUR, new Range(text("2001-08-05T00:00:00.000+0000"), text("2001-08-05T01:00:00.000+0000")))
                .put(MINUTE, new Range(text("2001-08-05T02:00:00.000+0000")))
                .put(SECOND, new Range(text("2001-08-05T02:01:00.000+0000"), text("2001-08-05T02:01:58.000+0000")))
                .put(MILLISECOND, new Range(text("2001-08-05T02:01:59.000+0000"), text("2001-08-05T02:01:59.999+0000")));
        Multimap<TimestampPrecision, Range> expected = builder.build();

        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeInfiniteEnd()
    {
        String startTime = "2001-08-02T00:30:05.321+0000";
        Range splitRange = new Range(text(startTime), null);
        Multimap<TimestampPrecision, Range> expected = ImmutableMultimap.of(MILLISECOND, splitRange);
        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test
    public void testSplitTimestampRangeInfiniteStart()
    {
        String endTime = "2001-08-07T00:32:07.456+0000";
        Range splitRange = new Range(null, text(endTime));
        Multimap<TimestampPrecision, Range> expected = ImmutableMultimap.of(MILLISECOND, splitRange);
        Multimap<TimestampPrecision, Range> splitRanges = splitTimestampRange(splitRange);
        assertEquals(splitRanges, expected);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testSplitTimestampRangeRangeNull()
    {
        splitTimestampRange(null);
    }

    private Text text(String v)
    {
        return new Text(SERIALIZER.encode(TimestampType.TIMESTAMP, ts(v).getTime()));
    }

    private static Timestamp ts(String date)
    {
        return new Timestamp(PARSER.parseDateTime(date).getMillis());
    }
}
