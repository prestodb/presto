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

import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static java.lang.Math.toIntExact;

public abstract class TestSequenceFunctionBase
        extends AbstractTestFunctions
{
    protected static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Asia/Kathmandu");
    protected static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);

    public TestSequenceFunctionBase(boolean legacyTimestamp)
    {
        super(
                testSessionBuilder()
                        .setTimeZoneKey(TIME_ZONE_KEY)
                        .setSystemProperty(LEGACY_TIMESTAMP, String.valueOf(legacyTimestamp))
                        .build(),
                new FeaturesConfig()
                        .setLegacyTimestamp(legacyTimestamp));
    }

    @Test
    public void testSequence()
    {
        // defaults to a step of 1
        assertFunction("SEQUENCE(1, 5)", new ArrayType(BIGINT), ImmutableList.of(1L, 2L, 3L, 4L, 5L));
        assertFunction("SEQUENCE(-10, -5)", new ArrayType(BIGINT), ImmutableList.of(-10L, -9L, -8L, -7L, -6L, -5L));
        assertFunction("SEQUENCE(-5, 2)", new ArrayType(BIGINT), ImmutableList.of(-5L, -4L, -3L, -2L, -1L, 0L, 1L, 2L));
        assertFunction("SEQUENCE(2, 2)", new ArrayType(BIGINT), ImmutableList.of(2L));

        // defaults to a step of -1
        assertFunction("SEQUENCE(5, 1)", new ArrayType(BIGINT), ImmutableList.of(5L, 4L, 3L, 2L, 1L));
        assertFunction("SEQUENCE(-5, -10)", new ArrayType(BIGINT), ImmutableList.of(-5L, -6L, -7L, -8L, -9L, -10L));
        assertFunction("SEQUENCE(2, -5)", new ArrayType(BIGINT), ImmutableList.of(2L, 1L, 0L, -1L, -2L, -3L, -4L, -5L));

        // with increment
        assertFunction("SEQUENCE(1, 9, 4)", new ArrayType(BIGINT), ImmutableList.of(1L, 5L, 9L));
        assertFunction("SEQUENCE(-10, -5, 2)", new ArrayType(BIGINT), ImmutableList.of(-10L, -8L, -6L));
        assertFunction("SEQUENCE(-5, 2, 3)", new ArrayType(BIGINT), ImmutableList.of(-5L, -2L, 1L));
        assertFunction("SEQUENCE(2, 2, 2)", new ArrayType(BIGINT), ImmutableList.of(2L));
        assertFunction("SEQUENCE(5, 1, -1)", new ArrayType(BIGINT), ImmutableList.of(5L, 4L, 3L, 2L, 1L));
        assertFunction("SEQUENCE(10, 2, -2)", new ArrayType(BIGINT), ImmutableList.of(10L, 8L, 6L, 4L, 2L));

        // failure modes
        assertInvalidFunction("SEQUENCE(2, -1, 1)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("SEQUENCE(-1, -10, 1)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("SEQUENCE(1, 1000000)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testSequenceDateIntervalDayToSecond()
    {
        assertFunction(
                "SEQUENCE(date '2016-04-12', date '2016-04-14', interval '1' day)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-12"), sqlDate("2016-04-13"), sqlDate("2016-04-14")));
        assertFunction(
                "SEQUENCE(date '2016-04-14', date '2016-04-12', interval '-1' day)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-14"), sqlDate("2016-04-13"), sqlDate("2016-04-12")));

        assertFunction(
                "SEQUENCE(date '2016-04-12', date '2016-04-16', interval '2' day)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-12"), sqlDate("2016-04-14"), sqlDate("2016-04-16")));
        assertFunction(
                "SEQUENCE(date '2016-04-16', date '2016-04-12', interval '-2' day)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-16"), sqlDate("2016-04-14"), sqlDate("2016-04-12")));
    }

    @Test
    public void testSequenceTimestampIntervalDayToSecond()
    {
        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-16 01:07:00', interval '3' minute)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestamp("2016-04-16 01:00:10"), sqlTimestamp("2016-04-16 01:03:10"), sqlTimestamp("2016-04-16 01:06:10")));
        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:10:10', timestamp '2016-04-16 01:03:00', interval '-3' minute)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestamp("2016-04-16 01:10:10"), sqlTimestamp("2016-04-16 01:07:10"), sqlTimestamp("2016-04-16 01:04:10")));

        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-16 01:01:00', interval '20' second)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestamp("2016-04-16 01:00:10"), sqlTimestamp("2016-04-16 01:00:30"), sqlTimestamp("2016-04-16 01:00:50")));
        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:01:10', timestamp '2016-04-16 01:00:20', interval '-20' second)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestamp("2016-04-16 01:01:10"), sqlTimestamp("2016-04-16 01:00:50"), sqlTimestamp("2016-04-16 01:00:30")));

        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-18 01:01:00', interval '19' hour)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestamp("2016-04-16 01:00:10"), sqlTimestamp("2016-04-16 20:00:10"), sqlTimestamp("2016-04-17 15:00:10")));
        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-14 01:00:20', interval '-19' hour)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestamp("2016-04-16 01:00:10"), sqlTimestamp("2016-04-15 06:00:10"), sqlTimestamp("2016-04-14 11:00:10")));

        // failure modes
        assertInvalidFunction(
                "SEQUENCE(date '2016-04-12', date '2016-04-14', interval '-1' day)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(date '2016-04-14', date '2016-04-12', interval '1' day)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(date '2000-04-14', date '2030-04-12', interval '1' day)",
                INVALID_FUNCTION_ARGUMENT,
                "result of sequence function must not have more than 10000 entries");
        assertInvalidFunction(
                "SEQUENCE(date '2018-01-01', date '2018-01-04', interval '18' hour)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence step must be a day interval if start and end values are dates");
        assertInvalidFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-16 01:01:00', interval '-20' second)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(timestamp '2016-04-16 01:10:10', timestamp '2016-04-16 01:01:00', interval '20' second)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-16 09:01:00', interval '1' second)",
                INVALID_FUNCTION_ARGUMENT,
                "result of sequence function must not have more than 10000 entries");
    }

    @Test
    public void testSequenceDateIntervalYearToMonth()
    {
        assertFunction(
                "SEQUENCE(date '2016-04-12', date '2016-06-12', interval '1' month)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-12"), sqlDate("2016-05-12"), sqlDate("2016-06-12")));
        assertFunction(
                "SEQUENCE(date '2016-06-12', date '2016-04-12', interval '-1' month)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-06-12"), sqlDate("2016-05-12"), sqlDate("2016-04-12")));

        assertFunction(
                "SEQUENCE(date '2016-04-12', date '2016-08-12', interval '2' month)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-12"), sqlDate("2016-06-12"), sqlDate("2016-08-12")));
        assertFunction(
                "SEQUENCE(date '2016-08-12', date '2016-04-12', interval '-2' month)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-08-12"), sqlDate("2016-06-12"), sqlDate("2016-04-12")));
    }

    @Test
    public void testSequenceTimestampIntervalYearToMonth()
    {
        assertFunction(
                "SEQUENCE(date '2016-04-12', date '2018-04-12', interval '1' year)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2016-04-12"), sqlDate("2017-04-12"), sqlDate("2018-04-12")));
        assertFunction(
                "SEQUENCE(date '2018-04-12', date '2016-04-12', interval '-1' year)",
                new ArrayType(DATE),
                ImmutableList.of(sqlDate("2018-04-12"), sqlDate("2017-04-12"), sqlDate("2016-04-12")));

        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-09-16 01:10:00', interval '2' month)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestamp("2016-04-16 01:00:10"), sqlTimestamp("2016-06-16 01:00:10"), sqlTimestamp("2016-08-16 01:00:10")));
        assertFunction(
                "SEQUENCE(timestamp '2016-09-16 01:10:10', timestamp '2016-04-16 01:00:00', interval '-2' month)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestamp("2016-09-16 01:10:10"), sqlTimestamp("2016-07-16 01:10:10"), sqlTimestamp("2016-05-16 01:10:10")));

        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2021-04-16 01:01:00', interval '2' year)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestamp("2016-04-16 01:00:10"), sqlTimestamp("2018-04-16 01:00:10"), sqlTimestamp("2020-04-16 01:00:10")));
        assertFunction(
                "SEQUENCE(timestamp '2016-04-16 01:01:10', timestamp '2011-04-16 01:00:00', interval '-2' year)",
                new ArrayType(TIMESTAMP),
                ImmutableList.of(sqlTimestamp("2016-04-16 01:01:10"), sqlTimestamp("2014-04-16 01:01:10"), sqlTimestamp("2012-04-16 01:01:10")));

        // failure modes
        assertInvalidFunction(
                "SEQUENCE(date '2016-06-12', date '2016-04-12', interval '1' month)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(date '2016-04-12', date '2016-06-12', interval '-1' month)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(date '2000-04-12', date '3000-06-12', interval '1' month)",
                INVALID_FUNCTION_ARGUMENT,
                "result of sequence function must not have more than 10000 entries");
        assertInvalidFunction(
                "SEQUENCE(timestamp '2016-05-16 01:00:10', timestamp '2016-04-16 01:01:00', interval '1' month)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(timestamp '2016-04-16 01:10:10', timestamp '2016-05-16 01:01:00', interval '-1' month)",
                INVALID_FUNCTION_ARGUMENT,
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertInvalidFunction(
                "SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '3000-04-16 09:01:00', interval '1' month)",
                INVALID_FUNCTION_ARGUMENT,
                "result of sequence function must not have more than 10000 entries");
    }

    protected abstract SqlTimestamp sqlTimestamp(String dateString);

    private SqlDate sqlDate(String dateString)
    {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate localDate = LocalDate.parse(dateString, dateTimeFormatter);
        return new SqlDate(toIntExact(localDate.toEpochDay()));
    }
}
