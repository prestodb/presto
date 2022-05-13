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
package com.facebook.presto.orc;

import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.orc.OrcTester.Format.DWRF;
import static com.facebook.presto.orc.OrcTester.assertFileContentsPresto;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnsPresto;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static java.lang.Math.floorDiv;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class TestTimestampWriteAndRead
{
    // Few positive, negative timestamp values
    private static final List<SqlTimestamp> MICROSECOND_VALUES = ImmutableList.of(
            sqlTimestampOf(0L, SESSION, MICROSECONDS), // 1970-01-01 00:00:00.000000
            sqlTimestampOf(1L, SESSION, MICROSECONDS), // 1970-01-01 00:00:00.000001
            sqlTimestampOf(999_999L, SESSION, MICROSECONDS), // 1970-01-01 00:00:00.999999
            sqlTimestampOf(1_000_000L, SESSION, MICROSECONDS), // 1970-01-01 00:00:01.000000
            sqlTimestampOf(-60_000_000_000_000_789L, SESSION, MICROSECONDS), // 0068-09-03 13:19:59.999211
            sqlTimestampOf(-230_000_000_000_999_999L, SESSION, MICROSECONDS), // -5319-08-03 23:06:39.000001
            sqlTimestampOf(1_650_483_250_000_507L, SESSION, MICROSECONDS), // 2022-04-20 19:34:10.000507
            sqlTimestampOf(60_000_000_000_123_789L, SESSION, MICROSECONDS), // 3871-04-29 10:40:00.123789
            sqlTimestampOf(230_000_000_000_999_999L, SESSION, MICROSECONDS)); // 9258-05-30 00:53:20.999999

    @Test
    public void testMicroWriteAndRead()
            throws Exception
    {
        testPrestoRoundTrip(TIMESTAMP_MICROSECONDS, MICROSECOND_VALUES, TIMESTAMP_MICROSECONDS, MICROSECOND_VALUES, true);
    }

    @Test
    public void testMicroWriteAndMilliRead()
            throws Exception
    {
        List<SqlTimestamp> microSecondValuesInMilli = MICROSECOND_VALUES.stream()
                .map(microTimestamp -> new SqlTimestamp(
                        floorDiv(microTimestamp.getMicrosUtc(), 1000),
                        microTimestamp.getSessionTimeZoneKey().get(),
                        TimeUnit.MILLISECONDS))
                .collect(toList());

        testPrestoRoundTrip(TIMESTAMP_MICROSECONDS, MICROSECOND_VALUES, TIMESTAMP, microSecondValuesInMilli, false);
    }

    @Test
    public void testMilliWriteAndMicroRead()
            throws Exception
    {
        List<SqlTimestamp> milliSecondValues = ImmutableList.of(
                sqlTimestampOf(0L, SESSION, MILLISECONDS), // 1970-01-01 00:00:00.000
                sqlTimestampOf(1L, SESSION, MILLISECONDS), // 1970-01-01 00:00:00.001
                sqlTimestampOf(999L, SESSION, MILLISECONDS), // 1970-01-01 00:00:00.999
                sqlTimestampOf(1_000L, SESSION, MILLISECONDS), // 1970-01-01 00:00:01.000
                sqlTimestampOf(-60_000_000_000_789L, SESSION, MILLISECONDS), // 0068-09-03 13:19:59.211
                sqlTimestampOf(-230_000_000_999_999L, SESSION, MILLISECONDS), // -5319-08-03 22:50:00.001
                sqlTimestampOf(1_650_483_250_507L, SESSION, MILLISECONDS), // 2022-04-20 19:34:10.507
                sqlTimestampOf(60_000_000_000_789L, SESSION, MILLISECONDS), // 3871-04-29 10:40:00.789
                sqlTimestampOf(230_000_000_000_999L, SESSION, MILLISECONDS)); // 9258-05-30 00:53:20.999

        List<SqlTimestamp> milliSecondValuesInMicro = milliSecondValues.stream()
                .map(milliTimestamp -> new SqlTimestamp(
                        milliTimestamp.getMillisUtc() * 1000,
                        milliTimestamp.getSessionTimeZoneKey().get(),
                        MICROSECONDS))
                .collect(toList());

        testPrestoRoundTrip(TIMESTAMP, milliSecondValues, TIMESTAMP_MICROSECONDS, milliSecondValuesInMicro, true);
    }

    // Flaw in ORC encoding makes timestamp between 1969-12-31 23:59:59.000000, exclusive, and 1970-01-01 00:00:00.000000, exclusive.
    // to be 1 second later than the original value written.
    @Test
    public void testOrcEncodingTimestampFlaw()
            throws Exception
    {
        // Written Values
        // (-1L, MICROSECONDS),         "1969-12-31 23:59:59.999999"
        // (-999_999L, MICROSECONDS),   "1969-12-31 23:59:59.000001"
        List<SqlTimestamp> timestampsWithFlaw = ImmutableList.of(
                sqlTimestampOf(-1L, SESSION, MICROSECONDS),
                sqlTimestampOf(-999_999L, SESSION, MICROSECONDS));

        // Expected Values
        // (999_999L, MICROSECONDS),    "1970-01-01 00:00:00.999999"
        // (1L, MICROSECONDS),          "1970-01-01 00:00:00:000001"
        List<SqlTimestamp> expectedTimestamps = ImmutableList.of(
                sqlTimestampOf(999_999L, SESSION, MICROSECONDS),
                sqlTimestampOf(1L, SESSION, MICROSECONDS));

        testPrestoRoundTrip(TIMESTAMP_MICROSECONDS, timestampsWithFlaw, TIMESTAMP_MICROSECONDS, expectedTimestamps, true);
    }

    private void testPrestoRoundTrip(Type writeType, List<SqlTimestamp> writeValues, Type readType, List<SqlTimestamp> expectedValues, boolean readWithMicroPrecision)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            writeOrcColumnsPresto(
                    tempFile.getFile(),
                    DWRF,
                    CompressionKind.ZLIB,
                    Optional.empty(),
                    ImmutableList.of(writeType),
                    ImmutableList.of(writeValues),
                    new NoOpOrcWriterStats());

            assertFileContentsPresto(
                    ImmutableList.of(readType),
                    tempFile,
                    ImmutableList.of(expectedValues),
                    false,
                    false,
                    DWRF.getOrcEncoding(),
                    DWRF,
                    false,
                    true,
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    readWithMicroPrecision);
        }
    }
}
