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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.facebook.presto.common.predicate.TupleDomainFilter.BigintRange;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.reader.TimestampOutOfBoundsException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.orc.NoOpOrcWriterStats.NOOP_WRITER_STATS;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.DWRF;
import static com.facebook.presto.orc.OrcTester.Format.ORC_11;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.assertBlockEquals;
import static com.facebook.presto.orc.OrcTester.assertFileContentsPresto;
import static com.facebook.presto.orc.OrcTester.createCustomOrcRecordReaderWithNullForOutOfBoundsTimestamp;
import static com.facebook.presto.orc.OrcTester.createCustomOrcSelectiveRecordReaderWithNullForOutOfBoundsTimestamp;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnsPresto;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestTimestampWriteAndRead
{
    private static final Set<OrcTester.Format> FORMATS = ImmutableSet.of(DWRF, ORC_12, ORC_11);

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

    private static final List<SqlTimestamp> MILLISECOND_VALUES = ImmutableList.of(
            sqlTimestampOf(0L, SESSION, MILLISECONDS), // 1970-01-01 00:00:00.000
            sqlTimestampOf(1L, SESSION, MILLISECONDS), // 1970-01-01 00:00:00.001
            sqlTimestampOf(999L, SESSION, MILLISECONDS), // 1970-01-01 00:00:00.999
            sqlTimestampOf(1_000L, SESSION, MILLISECONDS), // 1970-01-01 00:00:01.000
            sqlTimestampOf(-60_000_000_000_789L, SESSION, MILLISECONDS), // 0068-09-03 13:19:59.211
            sqlTimestampOf(-230_000_000_999_999L, SESSION, MILLISECONDS), // -5319-08-03 22:50:00.001
            sqlTimestampOf(1_650_483_250_507L, SESSION, MILLISECONDS), // 2022-04-20 19:34:10.507
            sqlTimestampOf(60_000_000_000_789L, SESSION, MILLISECONDS), // 3871-04-29 10:40:00.789
            sqlTimestampOf(230_000_000_000_999L, SESSION, MILLISECONDS)); // 9258-05-30 00:53:20.999

    @Test
    public void testMicroWriteAndRead()
            throws Exception
    {
        testPrestoRoundTrip(TIMESTAMP_MICROSECONDS, MICROSECOND_VALUES, TIMESTAMP_MICROSECONDS, MICROSECOND_VALUES);
    }

    @Test
    public void testMilliWriteAndRead()
            throws Exception
    {
        testPrestoRoundTrip(TIMESTAMP, MILLISECOND_VALUES, TIMESTAMP, MILLISECOND_VALUES);
    }

    @Test
    public void testMicroWriteAndMilliRead()
            throws Exception
    {
        List<SqlTimestamp> microSecondValuesInMilli = MICROSECOND_VALUES.stream()
                .map(microTimestamp -> new SqlTimestamp(floorDiv(microTimestamp.getMicros(), 1000), TimeUnit.MILLISECONDS))
                .collect(toList());

        testPrestoRoundTrip(TIMESTAMP_MICROSECONDS, MICROSECOND_VALUES, TIMESTAMP, microSecondValuesInMilli);
    }

    @Test
    public void testMilliWriteAndMicroRead()
            throws Exception
    {
        List<SqlTimestamp> milliSecondValuesInMicro = getMilliTimestampsInMicros(MILLISECOND_VALUES);

        testPrestoRoundTrip(TIMESTAMP, MILLISECOND_VALUES, TIMESTAMP_MICROSECONDS, milliSecondValuesInMicro);
    }

    // Using micro precision reduces max timestamp range that can be represented compared to using milli precision
    // Micro uses last 6 digits of long variable for precision, whereas only last 3 digits are needed for millis
    // Long.MAX_VALUE is 9223372036854775807, Long.MIN_VALUE is -9223372036854775808
    // Max and min seconds supported reading millis are 9223372036854775 and -9223372036854775
    // Max and min seconds supported reading micros are 9223372036854 and -9223372036854
    @Test
    public void testOverflowReadingMicros()
            throws Exception
    {
        List<SqlTimestamp> milliSecondValuesNoOverflow = ImmutableList.of(
                sqlTimestampOf(9_223_372_036_854_000L, SESSION, MILLISECONDS),
                sqlTimestampOf(-9_223_372_036_854_000L, SESSION, MILLISECONDS));
        List<SqlTimestamp> valuesInMicroNoOverflow = getMilliTimestampsInMicros(milliSecondValuesNoOverflow);
        testPrestoRoundTrip(TIMESTAMP, milliSecondValuesNoOverflow, TIMESTAMP_MICROSECONDS, valuesInMicroNoOverflow);

        List<SqlTimestamp> millisecondValuesOverflow = ImmutableList.of(
                sqlTimestampOf(9_223_372_036_855_000L, SESSION, MILLISECONDS),
                sqlTimestampOf(-9_223_372_036_855_000L, SESSION, MILLISECONDS));
        List<SqlTimestamp> valuesInMicroOverflow = getMilliTimestampsInMicros(millisecondValuesOverflow);

        // Reading with milli precision works fine
        testPrestoRoundTrip(TIMESTAMP, millisecondValuesOverflow, TIMESTAMP, millisecondValuesOverflow);
        // Overflows while reading with micro precision
        assertThrows(TimestampOutOfBoundsException.class,
                () -> testPrestoRoundTrip(TIMESTAMP, millisecondValuesOverflow, TIMESTAMP_MICROSECONDS, valuesInMicroOverflow));
    }

    // When readNullForOutOfBoundsTimestamp is enabled, out-of-bounds timestamps are read back as null
    // instead of failing the read with a TimestampOutOfBoundsException.
    @Test
    public void testOverflowReadingMicrosReturnsNullWhenConfigured()
            throws Exception
    {
        List<SqlTimestamp> millisecondValuesOverflow = ImmutableList.of(
                sqlTimestampOf(9_223_372_036_855_000L, SESSION, MILLISECONDS),
                sqlTimestampOf(-9_223_372_036_855_000L, SESSION, MILLISECONDS));

        for (OrcTester.Format format : FORMATS) {
            try (TempFile tempFile = new TempFile()) {
                writeOrcColumnsPresto(
                        tempFile.getFile(),
                        format,
                        CompressionKind.ZLIB,
                        Optional.empty(),
                        ImmutableList.of(TIMESTAMP),
                        ImmutableList.of(millisecondValuesOverflow),
                        NOOP_WRITER_STATS);

                try (OrcBatchRecordReader recordReader = createCustomOrcRecordReaderWithNullForOutOfBoundsTimestamp(
                        tempFile,
                        format.getOrcEncoding(),
                        OrcPredicate.TRUE,
                        ImmutableList.of(TIMESTAMP_MICROSECONDS),
                        MAX_BATCH_SIZE)) {
                    int rowsProcessed = 0;
                    for (int batchSize = toIntExact(recordReader.nextBatch()); batchSize >= 0; batchSize = toIntExact(recordReader.nextBatch())) {
                        Block block = recordReader.readBlock(0);
                        assertEquals(block.getPositionCount(), batchSize);
                        for (int position = 0; position < batchSize; position++) {
                            assertTrue(block.isNull(position), "Expected out-of-bounds timestamp to be read back as null");
                        }
                        rowsProcessed += batchSize;
                    }
                    assertEquals(rowsProcessed, millisecondValuesOverflow.size());
                }
            }
        }
    }

    // Same as above, but through the selective reader (OrcSelectiveRecordReader) — this is the exact
    // path that failed in production (TimestampSelectiveStreamReader.readNoFilter).
    @Test
    public void testOverflowReadingMicrosReturnsNullWhenConfiguredSelective()
            throws Exception
    {
        List<SqlTimestamp> millisecondValuesOverflow = ImmutableList.of(
                sqlTimestampOf(9_223_372_036_855_000L, SESSION, MILLISECONDS),
                sqlTimestampOf(-9_223_372_036_855_000L, SESSION, MILLISECONDS));
        List<SqlTimestamp> expectedNulls = Arrays.asList((SqlTimestamp) null, null);

        for (OrcTester.Format format : FORMATS) {
            try (TempFile tempFile = new TempFile()) {
                writeOrcColumnsPresto(
                        tempFile.getFile(),
                        format,
                        CompressionKind.ZLIB,
                        Optional.empty(),
                        ImmutableList.of(TIMESTAMP),
                        ImmutableList.of(millisecondValuesOverflow),
                        NOOP_WRITER_STATS);

                try (OrcSelectiveRecordReader recordReader = createCustomOrcSelectiveRecordReaderWithNullForOutOfBoundsTimestamp(
                        tempFile,
                        format.getOrcEncoding(),
                        OrcPredicate.TRUE,
                        ImmutableList.of(TIMESTAMP_MICROSECONDS),
                        MAX_BATCH_SIZE,
                        ImmutableMap.of())) {
                    assertFileContentsPresto(
                            ImmutableList.of(TIMESTAMP_MICROSECONDS),
                            recordReader,
                            ImmutableList.of(expectedNulls),
                            ImmutableList.of(0));
                }
            }
        }
    }

    // Interleaves in-range, out-of-bounds, and explicitly-null timestamps to verify that only the
    // out-of-bounds rows become null while valid values are still decoded correctly. Exercises both
    // the batch reader (readNullBlock) and the selective reader (readNoFilter + present stream).
    @Test
    public void testMixedValidNullAndOverflowMicrosReturnsNullWhenConfigured()
            throws Exception
    {
        SqlTimestamp valid1 = sqlTimestampOf(1_650_483_250_507L, SESSION, MILLISECONDS);
        SqlTimestamp valid2 = sqlTimestampOf(-60_000_000_000_789L, SESSION, MILLISECONDS);
        SqlTimestamp overflow1 = sqlTimestampOf(9_223_372_036_855_000L, SESSION, MILLISECONDS);
        SqlTimestamp overflow2 = sqlTimestampOf(-9_223_372_036_855_000L, SESSION, MILLISECONDS);

        // Written as millisecond timestamps: valid, overflow, null, valid, overflow.
        List<SqlTimestamp> writeValues = Arrays.asList(valid1, overflow1, null, valid2, overflow2);

        // Read as micros: valid values survive, overflow -> null, explicit null stays null.
        List<SqlTimestamp> expectedValues = Arrays.asList(
                new SqlTimestamp(valid1.getMillis() * 1000, MICROSECONDS),
                null,
                null,
                new SqlTimestamp(valid2.getMillis() * 1000, MICROSECONDS),
                null);

        for (OrcTester.Format format : FORMATS) {
            try (TempFile tempFile = new TempFile()) {
                writeOrcColumnsPresto(
                        tempFile.getFile(),
                        format,
                        CompressionKind.ZLIB,
                        Optional.empty(),
                        ImmutableList.of(TIMESTAMP),
                        ImmutableList.of(writeValues),
                        NOOP_WRITER_STATS);

                // Selective reader (the path from the reported failure).
                try (OrcSelectiveRecordReader selectiveReader = createCustomOrcSelectiveRecordReaderWithNullForOutOfBoundsTimestamp(
                        tempFile,
                        format.getOrcEncoding(),
                        OrcPredicate.TRUE,
                        ImmutableList.of(TIMESTAMP_MICROSECONDS),
                        MAX_BATCH_SIZE,
                        ImmutableMap.of())) {
                    assertFileContentsPresto(
                            ImmutableList.of(TIMESTAMP_MICROSECONDS),
                            selectiveReader,
                            ImmutableList.of(expectedValues),
                            ImmutableList.of(0));
                }

                // Batch reader.
                try (OrcBatchRecordReader batchReader = createCustomOrcRecordReaderWithNullForOutOfBoundsTimestamp(
                        tempFile,
                        format.getOrcEncoding(),
                        OrcPredicate.TRUE,
                        ImmutableList.of(TIMESTAMP_MICROSECONDS),
                        MAX_BATCH_SIZE)) {
                    int rowsProcessed = 0;
                    for (int batchSize = toIntExact(batchReader.nextBatch()); batchSize >= 0; batchSize = toIntExact(batchReader.nextBatch())) {
                        Block block = batchReader.readBlock(0);
                        assertEquals(block.getPositionCount(), batchSize);
                        assertBlockEquals(TIMESTAMP_MICROSECONDS, block, expectedValues, rowsProcessed);
                        rowsProcessed += batchSize;
                    }
                    assertEquals(rowsProcessed, expectedValues.size());
                }
            }
        }
    }

    // Explicitly non-nullable data: a column written with no nulls (no present stream) that contains
    // out-of-bounds values. Verifies the reader still allocates its nulls buffer when the flag is on,
    // decodes in-range values correctly, and reads only the overflow rows back as null.
    @Test
    public void testNonNullableColumnValidAndOverflowMicrosReturnsNullWhenConfigured()
            throws Exception
    {
        SqlTimestamp valid1 = sqlTimestampOf(1_650_483_250_507L, SESSION, MILLISECONDS);
        SqlTimestamp valid2 = sqlTimestampOf(-60_000_000_000_789L, SESSION, MILLISECONDS);
        SqlTimestamp overflow = sqlTimestampOf(9_223_372_036_855_000L, SESSION, MILLISECONDS);

        // No nulls written -> no present stream in the file.
        List<SqlTimestamp> writeValues = ImmutableList.of(valid1, overflow, valid2);
        List<SqlTimestamp> expectedValues = Arrays.asList(
                new SqlTimestamp(valid1.getMillis() * 1000, MICROSECONDS),
                null,
                new SqlTimestamp(valid2.getMillis() * 1000, MICROSECONDS));

        for (OrcTester.Format format : FORMATS) {
            try (TempFile tempFile = new TempFile()) {
                writeOrcColumnsPresto(
                        tempFile.getFile(),
                        format,
                        CompressionKind.ZLIB,
                        Optional.empty(),
                        ImmutableList.of(TIMESTAMP),
                        ImmutableList.of(writeValues),
                        NOOP_WRITER_STATS);

                try (OrcSelectiveRecordReader recordReader = createCustomOrcSelectiveRecordReaderWithNullForOutOfBoundsTimestamp(
                        tempFile,
                        format.getOrcEncoding(),
                        OrcPredicate.TRUE,
                        ImmutableList.of(TIMESTAMP_MICROSECONDS),
                        MAX_BATCH_SIZE,
                        ImmutableMap.of())) {
                    assertFileContentsPresto(
                            ImmutableList.of(TIMESTAMP_MICROSECONDS),
                            recordReader,
                            ImmutableList.of(expectedValues),
                            ImmutableList.of(0));
                }
            }
        }
    }

    // Exercises the selective reader's filter path (readWithFilter). The filter admits nulls, so
    // out-of-bounds timestamps (treated as null when configured) are retained as null.
    @Test
    public void testOverflowReadingMicrosWithFilterReturnsNullWhenConfigured()
            throws Exception
    {
        List<SqlTimestamp> millisecondValuesOverflow = ImmutableList.of(
                sqlTimestampOf(9_223_372_036_855_000L, SESSION, MILLISECONDS),
                sqlTimestampOf(-9_223_372_036_855_000L, SESSION, MILLISECONDS));
        List<SqlTimestamp> expectedNulls = Arrays.asList((SqlTimestamp) null, null);

        Map<Integer, Map<Subfield, TupleDomainFilter>> filters = ImmutableMap.of(
                0,
                ImmutableMap.of(new Subfield("c"), BigintRange.of(Long.MIN_VALUE, Long.MAX_VALUE, true)));

        for (OrcTester.Format format : FORMATS) {
            try (TempFile tempFile = new TempFile()) {
                writeOrcColumnsPresto(
                        tempFile.getFile(),
                        format,
                        CompressionKind.ZLIB,
                        Optional.empty(),
                        ImmutableList.of(TIMESTAMP),
                        ImmutableList.of(millisecondValuesOverflow),
                        NOOP_WRITER_STATS);

                try (OrcSelectiveRecordReader recordReader = createCustomOrcSelectiveRecordReaderWithNullForOutOfBoundsTimestamp(
                        tempFile,
                        format.getOrcEncoding(),
                        OrcPredicate.TRUE,
                        ImmutableList.of(TIMESTAMP_MICROSECONDS),
                        MAX_BATCH_SIZE,
                        filters)) {
                    assertFileContentsPresto(
                            ImmutableList.of(TIMESTAMP_MICROSECONDS),
                            recordReader,
                            ImmutableList.of(expectedNulls),
                            ImmutableList.of(0));
                }
            }
        }
    }

    // Flaw in ORC encoding makes timestamp between 1969-12-31 23:59:59.000000, exclusive, and 1970-01-01 00:00:00.000000, exclusive.
    // to be 1 second later than the original value written.
    @Test
    public void testOrcEncodingTimestampFlawMicros()
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

        testPrestoRoundTrip(TIMESTAMP_MICROSECONDS, timestampsWithFlaw, TIMESTAMP_MICROSECONDS, expectedTimestamps);
    }

    @Test
    public void testOrcEncodingTimestampFlawMillis()
            throws Exception
    {
        // Written Values
        // (-1L, MICROSECONDS),         "1969-12-31 23:59:59.999"
        // (-999L, MICROSECONDS),       "1969-12-31 23:59:59.001"
        List<SqlTimestamp> timestampsWithFlaw = ImmutableList.of(
                sqlTimestampOf(-1L, SESSION, MILLISECONDS),
                sqlTimestampOf(-999L, SESSION, MILLISECONDS));

        // Expected Values
        // (999L, MICROSECONDS),        "1970-01-01 00:00:00.999"
        // (1L, MICROSECONDS),          "1970-01-01 00:00:00:001"
        List<SqlTimestamp> expectedTimestamps = ImmutableList.of(
                sqlTimestampOf(999L, SESSION, MILLISECONDS),
                sqlTimestampOf(1L, SESSION, MILLISECONDS));

        testPrestoRoundTrip(TIMESTAMP, timestampsWithFlaw, TIMESTAMP, expectedTimestamps);
    }

    private void testPrestoRoundTrip(Type writeType, List<SqlTimestamp> writeValues, Type readType, List<SqlTimestamp> expectedValues)
            throws Exception
    {
        for (OrcTester.Format format : FORMATS) {
            try (TempFile tempFile = new TempFile()) {
                writeOrcColumnsPresto(
                        tempFile.getFile(),
                        format,
                        CompressionKind.ZLIB,
                        Optional.empty(),
                        ImmutableList.of(writeType),
                        ImmutableList.of(writeValues),
                        NOOP_WRITER_STATS);

                assertFileContentsPresto(
                        ImmutableList.of(readType),
                        tempFile,
                        ImmutableList.of(expectedValues),
                        false,
                        false,
                        format.getOrcEncoding(),
                        format,
                        false,
                        true,
                        ImmutableList.of(),
                        ImmutableMap.of());
            }
        }
    }

    private List<SqlTimestamp> getMilliTimestampsInMicros(List<SqlTimestamp> millisecondValues)
    {
        return millisecondValues.stream()
                .map(milliTimestamp -> new SqlTimestamp(milliTimestamp.getMillis() * 1000, MICROSECONDS))
                .collect(toList());
    }
}
