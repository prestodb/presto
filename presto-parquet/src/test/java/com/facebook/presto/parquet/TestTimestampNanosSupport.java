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
package com.facebook.presto.parquet;

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int64TimeAndTimestampMicrosValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int64TimeAndTimestampNanosPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.Int64TimeAndTimestampNanosRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.dictionary.LongDictionary;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slices;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.facebook.presto.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static com.facebook.presto.parquet.ParquetTypeUtils.isTimeStampNanosType;
import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Tests for nanosecond timestamp support in Parquet reading/writing.
 * Also tests unsupported datatype scenarios.
 */
public class TestTimestampNanosSupport
{
    // ============================================
    // Tests for isTimeStampNanosType utility
    // ============================================

    @Test
    public void testIsTimeStampNanosTypeWithNanosTimestamp()
    {
        PrimitiveType nanosTimestampType = Types.required(INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                .named("timestamp_nanos");
        ColumnDescriptor descriptor = new ColumnDescriptor(
                new String[] {"timestamp_nanos"},
                nanosTimestampType,
                0,
                0);

        assertTrue(isTimeStampNanosType(descriptor));
    }

    @Test
    public void testIsTimeStampNanosTypeWithMicrosTimestamp()
    {
        PrimitiveType microsTimestampType = Types.required(INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                .named("timestamp_micros");
        ColumnDescriptor descriptor = new ColumnDescriptor(
                new String[] {"timestamp_micros"},
                microsTimestampType,
                0,
                0);

        assertFalse(isTimeStampNanosType(descriptor));
    }

    @Test
    public void testIsTimeStampNanosTypeWithMillisTimestamp()
    {
        PrimitiveType millisTimestampType = Types.required(INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("timestamp_millis");
        ColumnDescriptor descriptor = new ColumnDescriptor(
                new String[] {"timestamp_millis"},
                millisTimestampType,
                0,
                0);

        assertFalse(isTimeStampNanosType(descriptor));
    }

    @Test
    public void testIsTimeStampNanosTypeWithNoAnnotation()
    {
        PrimitiveType plainInt64Type = Types.required(INT64)
                .named("plain_int64");
        ColumnDescriptor descriptor = new ColumnDescriptor(
                new String[] {"plain_int64"},
                plainInt64Type,
                0,
                0);

        assertFalse(isTimeStampNanosType(descriptor));
    }

    // ============================================
    // Tests for Nanosecond Plain Values Decoder
    // ============================================

    @Test
    public void testNanosPlainValuesDecoderBasic()
            throws IOException
    {
        // Create test data: 1000 nanoseconds = 1 microsecond, 1000000 nanoseconds = 1 millisecond
        long[] nanosValues = {
                1_000_000L,           // 1 ms
                1_500_000_000L,       // 1500 ms = 1.5 seconds
                60_000_000_000L,      // 60 seconds = 1 minute
                3_600_000_000_000L    // 3600 seconds = 1 hour
        };

        byte[] buffer = createInt64Buffer(nanosValues);
        Int64TimeAndTimestampMicrosValuesDecoder decoder =
                new Int64TimeAndTimestampNanosPlainValuesDecoder(buffer, 0, buffer.length, false);

        long[] results = new long[nanosValues.length];
        decoder.readNext(results, 0, nanosValues.length);

        // Verify conversion: nanos -> millis
        assertEquals(results[0], 1L);        // 1_000_000 ns = 1 ms
        assertEquals(results[1], 1500L);     // 1_500_000_000 ns = 1500 ms
        assertEquals(results[2], 60000L);    // 60_000_000_000 ns = 60000 ms
        assertEquals(results[3], 3600000L);  // 3_600_000_000_000 ns = 3600000 ms
    }

    @Test
    public void testNanosPlainValuesDecoderWithTimezone()
            throws IOException
    {
        long nanosValue = 86_400_000_000_000L; // 1 day in nanoseconds
        byte[] buffer = createInt64Buffer(new long[] {nanosValue});

        Int64TimeAndTimestampMicrosValuesDecoder decoder =
                new Int64TimeAndTimestampNanosPlainValuesDecoder(buffer, 0, buffer.length, true);

        long[] results = new long[1];
        decoder.readNext(results, 0, 1);

        // The result should be packed with UTC timezone
        // packDateTimeWithZone packs millis and timezone key together
        // We just verify it's not the plain milliseconds value
        long plainMillis = 86_400_000L; // 1 day in milliseconds
        assertTrue(results[0] != plainMillis, "Value should be packed with timezone");
    }

    @Test
    public void testNanosPlainValuesDecoderSkip()
            throws IOException
    {
        long[] nanosValues = {1_000_000L, 2_000_000L, 3_000_000L, 4_000_000L};
        byte[] buffer = createInt64Buffer(nanosValues);

        Int64TimeAndTimestampMicrosValuesDecoder decoder =
                new Int64TimeAndTimestampNanosPlainValuesDecoder(buffer, 0, buffer.length, false);

        decoder.skip(2); // Skip first two values

        long[] results = new long[2];
        decoder.readNext(results, 0, 2);

        assertEquals(results[0], 3L);  // Third value: 3_000_000 ns = 3 ms
        assertEquals(results[1], 4L);  // Fourth value: 4_000_000 ns = 4 ms
    }

    @Test
    public void testNanosPlainValuesDecoderBoundaryConditions()
            throws IOException
    {
        // Test with max/min values
        long[] nanosValues = {
                0L,                          // Zero
                999_999L,                    // Just under 1 ms (should round to 0)
                Long.MAX_VALUE               // Max value
        };

        byte[] buffer = createInt64Buffer(nanosValues);
        Int64TimeAndTimestampMicrosValuesDecoder decoder =
                new Int64TimeAndTimestampNanosPlainValuesDecoder(buffer, 0, buffer.length, false);

        long[] results = new long[nanosValues.length];
        decoder.readNext(results, 0, nanosValues.length);

        assertEquals(results[0], 0L);
        assertEquals(results[1], 0L);  // 999_999 ns rounds down to 0 ms
        // Long.MAX_VALUE / 1_000_000 = max millis
        assertEquals(results[2], Long.MAX_VALUE / 1_000_000L);
    }

    // ============================================
    // Tests for Invalid/Unsupported Scenarios
    // ============================================

    @Test
    public void testNanosDecoderInvalidReadRequest()
    {
        long[] nanosValues = {1_000_000L, 2_000_000L};
        byte[] buffer = createInt64Buffer(nanosValues);

        Int64TimeAndTimestampMicrosValuesDecoder decoder =
                new Int64TimeAndTimestampNanosPlainValuesDecoder(buffer, 0, buffer.length, false);

        try {
            // Try to read more values than available
            long[] results = new long[10];
            decoder.readNext(results, 0, 10);
            fail("Should throw IllegalArgumentException for invalid read request");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("End of stream"));
        }
        catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    public void testNanosDecoderNegativeLength()
    {
        long[] nanosValues = {1_000_000L};
        byte[] buffer = createInt64Buffer(nanosValues);

        Int64TimeAndTimestampMicrosValuesDecoder decoder =
                new Int64TimeAndTimestampNanosPlainValuesDecoder(buffer, 0, buffer.length, false);

        try {
            decoder.skip(-1);
            fail("Should throw IllegalArgumentException for negative length");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid length"));
        }
    }

    @Test
    public void testNanosDecoderNegativeOffset()
    {
        long[] nanosValues = {1_000_000L};
        byte[] buffer = createInt64Buffer(nanosValues);

        Int64TimeAndTimestampMicrosValuesDecoder decoder =
                new Int64TimeAndTimestampNanosPlainValuesDecoder(buffer, 0, buffer.length, false);

        try {
            long[] results = new long[1];
            decoder.readNext(results, -1, 1);
            fail("Should throw IllegalArgumentException for negative offset");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid read request"));
        }
        catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    // ============================================
    // Tests for RLE Dictionary Decoder
    // ============================================

    @Test
    public void testNanosRLEDictionaryDecoder()
            throws IOException
    {
        // Create a simple dictionary with nanosecond values
        int dictionarySize = 4;
        long[] dictionaryValues = {
                1_000_000L,           // 1 ms
                1_500_000_000L,       // 1500 ms
                60_000_000_000L,      // 60000 ms
                3_600_000_000_000L    // 3600000 ms
        };

        byte[] dictionaryPage = createInt64Buffer(dictionaryValues);
        LongDictionary dictionary = new LongDictionary(
                new DictionaryPage(Slices.wrappedBuffer(dictionaryPage), dictionarySize, PLAIN_DICTIONARY));

        // Create a simple RLE encoded page with dictionary IDs
        // For simplicity, use RLE encoding with a single repeated value
        byte[] dataPage = createRLEEncodedPage(new int[] {0, 1, 2, 3}, dictionarySize);

        Int64TimeAndTimestampMicrosValuesDecoder decoder =
                new Int64TimeAndTimestampNanosRLEDictionaryValuesDecoder(
                        getWidthFromMaxInt(dictionarySize - 1),
                        new ByteArrayInputStream(dataPage),
                        dictionary,
                        false);

        long[] results = new long[4];
        decoder.readNext(results, 0, 4);

        assertEquals(results[0], 1L);
        assertEquals(results[1], 1500L);
        assertEquals(results[2], 60000L);
        assertEquals(results[3], 3600000L);
    }

    // ============================================
    // Data Provider for Parameterized Tests
    // ============================================

    @DataProvider(name = "unsupportedPrimitiveTypes")
    public Object[][] unsupportedPrimitiveTypes()
    {
        return new Object[][] {
                // Primitive types that are not supported for timestamp
                {"INT32 with NANOS annotation", Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named("invalid_timestamp")},
                {"FLOAT type", Types.required(PrimitiveType.PrimitiveTypeName.FLOAT)
                        .named("float_type")},
                {"DOUBLE type", Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .named("double_type")},
        };
    }

    @Test(dataProvider = "unsupportedPrimitiveTypes")
    public void testUnsupportedPrimitiveTypesForNanosTimestamp(String description, PrimitiveType type)
    {
        ColumnDescriptor descriptor = new ColumnDescriptor(
                new String[] {"test"},
                type,
                0,
                0);

        // INT32 with NANOS should not be detected as nanos type (only INT64 is valid)
        assertFalse(isTimeStampNanosType(descriptor),
                "Type " + description + " should not be detected as nanosecond timestamp");
    }

    // ============================================
    // Helper Methods
    // ============================================

    private static byte[] createInt64Buffer(long[] values)
    {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * 8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for (long value : values) {
            buffer.putLong(value);
        }
        return buffer.array();
    }

    private static byte[] createRLEEncodedPage(int[] values, int maxValue)
    {
        // Simple bit-packed encoding for small values
        int bitWidth = getWidthFromMaxInt(maxValue - 1);
        ByteBuffer buffer = ByteBuffer.allocate(1 + values.length * 4);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Write bit-packed header: (count << 1) | 1
        int header = (values.length << 1) | 1;
        buffer.put((byte) header);

        // Pack values (simplified - assumes small values and bit width)
        for (int value : values) {
            buffer.put((byte) value);
        }

        byte[] result = new byte[buffer.position()];
        buffer.flip();
        buffer.get(result);
        return result;
    }
}
