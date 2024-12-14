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
package com.facebook.presto.parquet.batchreader.decoders;

import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BinaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BooleanValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int32ValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int64TimeAndTimestampMicrosValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int64ValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.ShortDecimalValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.TimestampValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.UuidValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.BinaryPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.BooleanPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.FixedLenByteArrayUuidPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int32PlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int32ShortDecimalPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int64PlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int64ShortDecimalPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int64TimeAndTimestampMicrosPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.TimestampPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.BinaryRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.BooleanRLEValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.Int32RLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.Int32ShortDecimalRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.Int64RLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.Int64TimeAndTimestampMicrosRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.TimestampRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.UuidRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.dictionary.BinaryBatchDictionary;
import com.facebook.presto.parquet.batchreader.dictionary.TimestampDictionary;
import com.facebook.presto.parquet.dictionary.IntegerDictionary;
import com.facebook.presto.parquet.dictionary.LongDictionary;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.testng.Assert.assertEquals;

public abstract class TestValuesDecoders
{
    private static Int32ValuesDecoder int32Plain(byte[] pageBytes)
    {
        return new Int32PlainValuesDecoder(pageBytes, 0, pageBytes.length);
    }

    private static Int32ValuesDecoder int32Dictionary(byte[] pageBytes, int dictionarySize, IntegerDictionary dictionary)
    {
        return new Int32RLEDictionaryValuesDecoder(getWidthFromMaxInt(dictionarySize), new ByteArrayInputStream(pageBytes), dictionary);
    }

    private static BinaryValuesDecoder binaryPlain(byte[] pageBytes)
    {
        return new BinaryPlainValuesDecoder(pageBytes, 0, pageBytes.length);
    }

    private static BinaryValuesDecoder binaryDictionary(byte[] pageBytes, int dictionarySize, BinaryBatchDictionary dictionary)
    {
        return new BinaryRLEDictionaryValuesDecoder(getWidthFromMaxInt(dictionarySize), new ByteArrayInputStream(pageBytes), dictionary);
    }

    private static Int64ValuesDecoder int64Plain(byte[] pageBytes)
    {
        return new Int64PlainValuesDecoder(pageBytes, 0, pageBytes.length);
    }

    private static Int64TimeAndTimestampMicrosValuesDecoder int64TimestampMicrosPlain(byte[] pageBytes)
    {
        return new Int64TimeAndTimestampMicrosPlainValuesDecoder(pageBytes, 0, pageBytes.length);
    }

    private static Int64ValuesDecoder int64Dictionary(byte[] pageBytes, int dictionarySize, LongDictionary dictionary)
    {
        return new Int64RLEDictionaryValuesDecoder(getWidthFromMaxInt(dictionarySize), new ByteArrayInputStream(pageBytes), dictionary);
    }

    private static Int64TimeAndTimestampMicrosValuesDecoder int64TimestampMicrosDictionary(byte[] pageBytes, int dictionarySize, LongDictionary dictionary)
    {
        return new Int64TimeAndTimestampMicrosRLEDictionaryValuesDecoder(getWidthFromMaxInt(dictionarySize), new ByteArrayInputStream(pageBytes), dictionary);
    }

    private static TimestampValuesDecoder timestampPlain(byte[] pageBytes)
    {
        return new TimestampPlainValuesDecoder(pageBytes, 0, pageBytes.length);
    }

    private static TimestampValuesDecoder timestampDictionary(byte[] pageBytes, int dictionarySize, TimestampDictionary dictionary)
    {
        return new TimestampRLEDictionaryValuesDecoder(getWidthFromMaxInt(dictionarySize), new ByteArrayInputStream(pageBytes), dictionary);
    }

    private static BooleanValuesDecoder booleanPlain(byte[] pageBytes)
    {
        return new BooleanPlainValuesDecoder(pageBytes, 0, pageBytes.length);
    }

    private static BooleanValuesDecoder booleanRLE(byte[] pageBytes)
    {
        return new BooleanRLEValuesDecoder(ByteBuffer.wrap(pageBytes));
    }

    private static ShortDecimalValuesDecoder int32ShortDecimalPlain(byte[] pageBytes)
    {
        return new Int32ShortDecimalPlainValuesDecoder(pageBytes, 0, pageBytes.length);
    }

    private static ShortDecimalValuesDecoder int64ShortDecimalPlain(byte[] pageBytes)
    {
        return new Int64ShortDecimalPlainValuesDecoder(pageBytes, 0, pageBytes.length);
    }

    private static ShortDecimalValuesDecoder int32ShortDecimalRLE(byte[] pageBytes, int dictionarySize, IntegerDictionary dictionary)
    {
        return new Int32ShortDecimalRLEDictionaryValuesDecoder(getWidthFromMaxInt(dictionarySize), new ByteArrayInputStream(pageBytes), dictionary);
    }

    private static ShortDecimalValuesDecoder int64ShortDecimalRLE(byte[] pageBytes, int dictionarySize, LongDictionary dictionary)
    {
        return new Int64RLEDictionaryValuesDecoder(getWidthFromMaxInt(dictionarySize), new ByteArrayInputStream(pageBytes), dictionary);
    }

    private static UuidValuesDecoder uuidPlain(byte[] pageBytes)
    {
        return new FixedLenByteArrayUuidPlainValuesDecoder(16, pageBytes, 0, pageBytes.length);
    }

    private static UuidValuesDecoder uuidRle(byte[] pageBytes, int dictionarySize, BinaryBatchDictionary dictionary)
    {
        return new UuidRLEDictionaryValuesDecoder(getWidthFromMaxInt(dictionarySize), new ByteArrayInputStream(pageBytes), dictionary);
    }

    private static void uuidBatchReadWithSkipHelper(int batchSize, int skipSize, int valueCount, UuidValuesDecoder decoder, List<Object> expectedValues)
            throws IOException
    {
        long[] actualValues = new long[valueCount * 2];
        int inputOffset = 0;
        int outputOffset = 0;
        while (inputOffset < valueCount) {
            int readBatchSize = min(batchSize, valueCount - inputOffset);
            decoder.readNext(actualValues, outputOffset, readBatchSize);

            for (int i = 0; i < (readBatchSize * 2); i++) {
                assertEquals(actualValues[(outputOffset * 2) + i], expectedValues.get((inputOffset * 2) + i));
            }

            inputOffset += readBatchSize;
            outputOffset += readBatchSize;

            int skipBatchSize = min(skipSize, valueCount - inputOffset);
            decoder.skip(skipBatchSize);
            inputOffset += skipBatchSize;
        }
    }

    private static void int32BatchReadWithSkipHelper(int batchSize, int skipSize, int valueCount, Int32ValuesDecoder decoder, List<Object> expectedValues)
            throws IOException
    {
        int[] actualValues = new int[valueCount];
        int inputOffset = 0;
        int outputOffset = 0;
        while (inputOffset < valueCount) {
            int readBatchSize = min(batchSize, valueCount - inputOffset);
            decoder.readNext(actualValues, outputOffset, readBatchSize);

            for (int i = 0; i < readBatchSize; i++) {
                assertEquals(actualValues[outputOffset + i], (int) expectedValues.get(inputOffset + i));
            }

            inputOffset += readBatchSize;
            outputOffset += readBatchSize;

            int skipBatchSize = min(skipSize, valueCount - inputOffset);
            decoder.skip(skipBatchSize);
            inputOffset += skipBatchSize;
        }
    }

    private static void binaryBatchReadWithSkipHelper(int batchSize, int skipSize, int valueCount, BinaryValuesDecoder decoder, List<Object> expectedValues)
            throws IOException
    {
        int inputOffset = 0;
        while (inputOffset < valueCount) {
            int readBatchSize = min(batchSize, valueCount - inputOffset);
            BinaryValuesDecoder.ValueBuffer valueBuffer = decoder.readNext(readBatchSize);
            byte[] byteBuffer = new byte[valueBuffer.getBufferSize()];
            int[] offsets = new int[readBatchSize + 1];

            decoder.readIntoBuffer(byteBuffer, 0, offsets, 0, valueBuffer);

            for (int i = 0; i < readBatchSize; i++) {
                byte[] expected = ((String) expectedValues.get(inputOffset + i)).getBytes(UTF_8);
                byte[] actual = Arrays.copyOfRange(byteBuffer, offsets[i], offsets[i + 1]);
                assertEquals(expected, actual);
            }

            inputOffset += readBatchSize;

            int skipBatchSize = min(skipSize, valueCount - inputOffset);
            decoder.skip(skipBatchSize);
            inputOffset += skipBatchSize;
        }
    }

    private static void int64BatchReadWithSkipHelper(int batchSize, int skipSize, int valueCount, Int64TimeAndTimestampMicrosValuesDecoder decoder, List<Object> expectedValues)
            throws IOException
    {
        long[] actualValues = new long[valueCount];
        int inputOffset = 0;
        int outputOffset = 0;
        while (inputOffset < valueCount) {
            int readBatchSize = min(batchSize, valueCount - inputOffset);
            decoder.readNext(actualValues, outputOffset, readBatchSize);

            for (int i = 0; i < readBatchSize; i++) {
                assertEquals(actualValues[outputOffset + i], (long) expectedValues.get(inputOffset + i));
            }

            inputOffset += readBatchSize;
            outputOffset += readBatchSize;

            int skipBatchSize = min(skipSize, valueCount - inputOffset);
            decoder.skip(skipBatchSize);
            inputOffset += skipBatchSize;
        }
    }

    private static void int64BatchReadWithSkipHelper(int batchSize, int skipSize, int valueCount, Int64ValuesDecoder decoder, List<Object> expectedValues)
            throws IOException
    {
        long[] actualValues = new long[valueCount];
        int inputOffset = 0;
        int outputOffset = 0;
        while (inputOffset < valueCount) {
            int readBatchSize = min(batchSize, valueCount - inputOffset);
            decoder.readNext(actualValues, outputOffset, readBatchSize);

            for (int i = 0; i < readBatchSize; i++) {
                assertEquals(actualValues[outputOffset + i], (long) expectedValues.get(inputOffset + i));
            }

            inputOffset += readBatchSize;
            outputOffset += readBatchSize;

            int skipBatchSize = min(skipSize, valueCount - inputOffset);
            decoder.skip(skipBatchSize);
            inputOffset += skipBatchSize;
        }
    }

    private static void int32ShortDecimalBatchReadWithSkipHelper(int batchSize, int skipSize, int valueCount, ShortDecimalValuesDecoder decoder, List<Object> expectedValues)
            throws IOException
    {
        long[] actualValues = new long[valueCount];
        int inputOffset = 0;
        int outputOffset = 0;
        while (inputOffset < valueCount) {
            int readBatchSize = min(batchSize, valueCount - inputOffset);
            decoder.readNext(actualValues, outputOffset, readBatchSize);

            for (int i = 0; i < readBatchSize; i++) {
                assertEquals(actualValues[outputOffset + i], (int) expectedValues.get(inputOffset + i));
            }

            inputOffset += readBatchSize;
            outputOffset += readBatchSize;

            int skipBatchSize = min(skipSize, valueCount - inputOffset);
            decoder.skip(skipBatchSize);
            inputOffset += skipBatchSize;
        }
    }

    private static void int64ShortDecimalBatchReadWithSkipHelper(int batchSize, int skipSize, int valueCount, ShortDecimalValuesDecoder decoder, List<Object> expectedValues)
            throws IOException
    {
        long[] actualValues = new long[valueCount];
        int inputOffset = 0;
        int outputOffset = 0;
        while (inputOffset < valueCount) {
            int readBatchSize = min(batchSize, valueCount - inputOffset);
            decoder.readNext(actualValues, outputOffset, readBatchSize);

            for (int i = 0; i < readBatchSize; i++) {
                assertEquals(actualValues[outputOffset + i], expectedValues.get(inputOffset + i));
            }

            inputOffset += readBatchSize;
            outputOffset += readBatchSize;

            int skipBatchSize = min(skipSize, valueCount - inputOffset);
            decoder.skip(skipBatchSize);
            inputOffset += skipBatchSize;
        }
    }

    private static void timestampBatchReadWithSkipHelper(int batchSize, int skipSize, int valueCount, TimestampValuesDecoder decoder, List<Object> expectedValues)
            throws IOException
    {
        long[] actualValues = new long[valueCount];
        int inputOffset = 0;
        int outputOffset = 0;
        while (inputOffset < valueCount) {
            int readBatchSize = min(batchSize, valueCount - inputOffset);
            decoder.readNext(actualValues, outputOffset, readBatchSize);

            for (int i = 0; i < readBatchSize; i++) {
                assertEquals(actualValues[outputOffset + i], (long) expectedValues.get(inputOffset + i));
            }

            inputOffset += readBatchSize;
            outputOffset += readBatchSize;

            int skipBatchSize = min(skipSize, valueCount - inputOffset);
            decoder.skip(skipBatchSize);
            inputOffset += skipBatchSize;
        }
    }

    private static void booleanBatchReadWithSkipHelper(int batchSize, int skipSize, int valueCount, BooleanValuesDecoder decoder, List<Object> expectedValues)
    {
        byte[] actualValues = new byte[valueCount];
        int inputOffset = 0;
        int outputOffset = 0;
        while (inputOffset < valueCount) {
            int readBatchSize = min(batchSize, valueCount - inputOffset);
            decoder.readNext(actualValues, outputOffset, readBatchSize);

            for (int i = 0; i < readBatchSize; i++) {
                assertEquals(actualValues[outputOffset + i], (int) expectedValues.get(inputOffset + i));
            }

            inputOffset += readBatchSize;
            outputOffset += readBatchSize;

            int skipBatchSize = min(skipSize, valueCount - inputOffset);
            decoder.skip(skipBatchSize);
            inputOffset += skipBatchSize;
        }
    }

    public abstract byte[] generatePlainValuesPage(int valueCount, int valueSizeBits, List<Object> addedValues);

    public abstract byte[] generateDictionaryIdPage2048(int maxValue, List<Integer> addedValues);

    @Test
    public void testInt32Plain()
            throws IOException
    {
        int valueCount = 2048;
        List<Object> expectedValues = new ArrayList<>();

        byte[] pageBytes = generatePlainValuesPage(valueCount, 32, expectedValues);

        int32BatchReadWithSkipHelper(valueCount, 0, valueCount, int32Plain(pageBytes), expectedValues); // read all values in one batch
        int32BatchReadWithSkipHelper(29, 0, valueCount, int32Plain(pageBytes), expectedValues);
        int32BatchReadWithSkipHelper(89, 0, valueCount, int32Plain(pageBytes), expectedValues);
        int32BatchReadWithSkipHelper(1024, 0, valueCount, int32Plain(pageBytes), expectedValues);

        int32BatchReadWithSkipHelper(256, 29, valueCount, int32Plain(pageBytes), expectedValues);
        int32BatchReadWithSkipHelper(89, 29, valueCount, int32Plain(pageBytes), expectedValues);
        int32BatchReadWithSkipHelper(1024, 1024, valueCount, int32Plain(pageBytes), expectedValues);
    }

    @Test
    public void testInt32RLEDictionary()
            throws IOException
    {
        int valueCount = 2048;
        int dictionarySize = 29;
        List<Object> dictionary = new ArrayList<>();
        List<Integer> dictionaryIds = new ArrayList<>();

        byte[] dictionaryPage = generatePlainValuesPage(dictionarySize, 32, dictionary);
        byte[] dataPage = generateDictionaryIdPage2048(dictionarySize - 1, dictionaryIds);

        List<Object> expectedValues = new ArrayList<>();
        for (Integer dictionaryId : dictionaryIds) {
            expectedValues.add(dictionary.get(dictionaryId));
        }

        IntegerDictionary integerDictionary = new IntegerDictionary(new DictionaryPage(Slices.wrappedBuffer(dictionaryPage), dictionarySize, PLAIN_DICTIONARY));

        int32BatchReadWithSkipHelper(valueCount, 0, valueCount, int32Dictionary(dataPage, dictionarySize, integerDictionary), expectedValues);
        int32BatchReadWithSkipHelper(29, 0, valueCount, int32Dictionary(dataPage, dictionarySize, integerDictionary), expectedValues);
        int32BatchReadWithSkipHelper(89, 0, valueCount, int32Dictionary(dataPage, dictionarySize, integerDictionary), expectedValues);
        int32BatchReadWithSkipHelper(1024, 0, valueCount, int32Dictionary(dataPage, dictionarySize, integerDictionary), expectedValues);

        int32BatchReadWithSkipHelper(256, 29, valueCount, int32Dictionary(dataPage, dictionarySize, integerDictionary), expectedValues);
        int32BatchReadWithSkipHelper(89, 29, valueCount, int32Dictionary(dataPage, dictionarySize, integerDictionary), expectedValues);
        int32BatchReadWithSkipHelper(1024, 1024, valueCount, int32Dictionary(dataPage, dictionarySize, integerDictionary), expectedValues);
    }

    @Test
    public void testBinaryPlain()
            throws IOException
    {
        int valueCount = 2048;
        List<Object> expectedValues = new ArrayList<>();

        byte[] pageBytes = generatePlainValuesPage(valueCount, -1, expectedValues);

        binaryBatchReadWithSkipHelper(valueCount, 0, valueCount, binaryPlain(pageBytes), expectedValues); // read all values in one batch
        binaryBatchReadWithSkipHelper(29, 0, valueCount, binaryPlain(pageBytes), expectedValues);
        binaryBatchReadWithSkipHelper(89, 0, valueCount, binaryPlain(pageBytes), expectedValues);
        binaryBatchReadWithSkipHelper(1024, 0, valueCount, binaryPlain(pageBytes), expectedValues);

        binaryBatchReadWithSkipHelper(256, 29, valueCount, binaryPlain(pageBytes), expectedValues);
        binaryBatchReadWithSkipHelper(89, 29, valueCount, binaryPlain(pageBytes), expectedValues);
        binaryBatchReadWithSkipHelper(1024, 1024, valueCount, binaryPlain(pageBytes), expectedValues);
    }

    @Test
    public void testBinaryRLEDictionary()
            throws IOException
    {
        int valueCount = 2048;
        int dictionarySize = 29;
        List<Object> dictionary = new ArrayList<>();
        List<Integer> dictionaryIds = new ArrayList<>();

        byte[] dictionaryPage = generatePlainValuesPage(dictionarySize, -1, dictionary);
        byte[] dataPage = generateDictionaryIdPage2048(dictionarySize - 1, dictionaryIds);

        List<Object> expectedValues = new ArrayList<>();
        for (Integer dictionaryId : dictionaryIds) {
            expectedValues.add(dictionary.get(dictionaryId));
        }

        BinaryBatchDictionary binaryDictionary = new BinaryBatchDictionary(new DictionaryPage(Slices.wrappedBuffer(dictionaryPage), dictionarySize, PLAIN_DICTIONARY));

        binaryBatchReadWithSkipHelper(valueCount, 0, valueCount, binaryDictionary(dataPage, dictionarySize, binaryDictionary), expectedValues);
        binaryBatchReadWithSkipHelper(29, 0, valueCount, binaryDictionary(dataPage, dictionarySize, binaryDictionary), expectedValues);
        binaryBatchReadWithSkipHelper(89, 0, valueCount, binaryDictionary(dataPage, dictionarySize, binaryDictionary), expectedValues);
        binaryBatchReadWithSkipHelper(1024, 0, valueCount, binaryDictionary(dataPage, dictionarySize, binaryDictionary), expectedValues);

        binaryBatchReadWithSkipHelper(256, 29, valueCount, binaryDictionary(dataPage, dictionarySize, binaryDictionary), expectedValues);
        binaryBatchReadWithSkipHelper(89, 29, valueCount, binaryDictionary(dataPage, dictionarySize, binaryDictionary), expectedValues);
        binaryBatchReadWithSkipHelper(1024, 1024, valueCount, binaryDictionary(dataPage, dictionarySize, binaryDictionary), expectedValues);
    }

    @Test
    public void testInt64Plain()
            throws IOException
    {
        int valueCount = 2048;
        List<Object> expectedValues = new ArrayList<>();

        byte[] pageBytes = generatePlainValuesPage(valueCount, 64, expectedValues);

        int64BatchReadWithSkipHelper(valueCount, 0, valueCount, int64Plain(pageBytes), expectedValues); // read all values in one batch
        int64BatchReadWithSkipHelper(29, 0, valueCount, int64Plain(pageBytes), expectedValues);
        int64BatchReadWithSkipHelper(89, 0, valueCount, int64Plain(pageBytes), expectedValues);
        int64BatchReadWithSkipHelper(1024, 0, valueCount, int64Plain(pageBytes), expectedValues);

        int64BatchReadWithSkipHelper(256, 29, valueCount, int64Plain(pageBytes), expectedValues);
        int64BatchReadWithSkipHelper(89, 29, valueCount, int64Plain(pageBytes), expectedValues);
        int64BatchReadWithSkipHelper(1024, 1024, valueCount, int64Plain(pageBytes), expectedValues);

        List<Object> expectedTimestampValues = expectedValues.stream().map(v -> (long) v / 1000L).collect(Collectors.toList());
        int64BatchReadWithSkipHelper(valueCount, 0, valueCount, int64TimestampMicrosPlain(pageBytes), expectedTimestampValues); // read all values in one batch
        int64BatchReadWithSkipHelper(29, 0, valueCount, int64TimestampMicrosPlain(pageBytes), expectedTimestampValues);
        int64BatchReadWithSkipHelper(89, 0, valueCount, int64TimestampMicrosPlain(pageBytes), expectedTimestampValues);
        int64BatchReadWithSkipHelper(1024, 0, valueCount, int64TimestampMicrosPlain(pageBytes), expectedTimestampValues);

        int64BatchReadWithSkipHelper(256, 29, valueCount, int64TimestampMicrosPlain(pageBytes), expectedTimestampValues);
        int64BatchReadWithSkipHelper(89, 29, valueCount, int64TimestampMicrosPlain(pageBytes), expectedTimestampValues);
        int64BatchReadWithSkipHelper(1024, 1024, valueCount, int64TimestampMicrosPlain(pageBytes), expectedTimestampValues);
    }

    @Test
    public void testInt64RLEDictionary()
            throws IOException
    {
        int valueCount = 2048;
        int dictionarySize = 29;
        List<Object> dictionary = new ArrayList<>();
        List<Integer> dictionaryIds = new ArrayList<>();

        byte[] dictionaryPage = generatePlainValuesPage(dictionarySize, 64, dictionary);
        byte[] dataPage = generateDictionaryIdPage2048(dictionarySize - 1, dictionaryIds);

        List<Object> expectedValues = new ArrayList<>();
        for (Integer dictionaryId : dictionaryIds) {
            expectedValues.add(dictionary.get(dictionaryId));
        }

        LongDictionary longDictionary = new LongDictionary(new DictionaryPage(Slices.wrappedBuffer(dictionaryPage), dictionarySize, PLAIN_DICTIONARY));

        int64BatchReadWithSkipHelper(valueCount, 0, valueCount, int64Dictionary(dataPage, dictionarySize, longDictionary), expectedValues);
        int64BatchReadWithSkipHelper(29, 0, valueCount, int64Dictionary(dataPage, dictionarySize, longDictionary), expectedValues);
        int64BatchReadWithSkipHelper(89, 0, valueCount, int64Dictionary(dataPage, dictionarySize, longDictionary), expectedValues);
        int64BatchReadWithSkipHelper(1024, 0, valueCount, int64Dictionary(dataPage, dictionarySize, longDictionary), expectedValues);

        int64BatchReadWithSkipHelper(256, 29, valueCount, int64Dictionary(dataPage, dictionarySize, longDictionary), expectedValues);
        int64BatchReadWithSkipHelper(89, 29, valueCount, int64Dictionary(dataPage, dictionarySize, longDictionary), expectedValues);
        int64BatchReadWithSkipHelper(1024, 1024, valueCount, int64Dictionary(dataPage, dictionarySize, longDictionary), expectedValues);

        List<Object> expectedTimestampValues = expectedValues.stream().map(v -> (long) v / 1000L).collect(Collectors.toList());
        int64BatchReadWithSkipHelper(valueCount, 0, valueCount, int64TimestampMicrosDictionary(dataPage, dictionarySize, longDictionary), expectedTimestampValues);
        int64BatchReadWithSkipHelper(29, 0, valueCount, int64TimestampMicrosDictionary(dataPage, dictionarySize, longDictionary), expectedTimestampValues);
        int64BatchReadWithSkipHelper(89, 0, valueCount, int64TimestampMicrosDictionary(dataPage, dictionarySize, longDictionary), expectedTimestampValues);
        int64BatchReadWithSkipHelper(1024, 0, valueCount, int64TimestampMicrosDictionary(dataPage, dictionarySize, longDictionary), expectedTimestampValues);

        int64BatchReadWithSkipHelper(256, 29, valueCount, int64TimestampMicrosDictionary(dataPage, dictionarySize, longDictionary), expectedTimestampValues);
        int64BatchReadWithSkipHelper(89, 29, valueCount, int64TimestampMicrosDictionary(dataPage, dictionarySize, longDictionary), expectedTimestampValues);
        int64BatchReadWithSkipHelper(1024, 1024, valueCount, int64TimestampMicrosDictionary(dataPage, dictionarySize, longDictionary), expectedTimestampValues);
    }

    @Test
    public void testTimestampPlain()
            throws IOException
    {
        int valueCount = 2048;
        List<Object> expectedValues = new ArrayList<>();

        byte[] pageBytes = generatePlainValuesPage(valueCount, 96, expectedValues);

        timestampBatchReadWithSkipHelper(valueCount, 0, valueCount, timestampPlain(pageBytes), expectedValues); // read all values in one batch
        timestampBatchReadWithSkipHelper(29, 0, valueCount, timestampPlain(pageBytes), expectedValues);
        timestampBatchReadWithSkipHelper(89, 0, valueCount, timestampPlain(pageBytes), expectedValues);
        timestampBatchReadWithSkipHelper(1024, 0, valueCount, timestampPlain(pageBytes), expectedValues);

        timestampBatchReadWithSkipHelper(256, 29, valueCount, timestampPlain(pageBytes), expectedValues);
        timestampBatchReadWithSkipHelper(89, 29, valueCount, timestampPlain(pageBytes), expectedValues);
        timestampBatchReadWithSkipHelper(1024, 1024, valueCount, timestampPlain(pageBytes), expectedValues);
    }

    @Test
    public void testTimestampRLEDictionary()
            throws IOException
    {
        int valueCount = 2048;
        int dictionarySize = 29;
        List<Object> dictionary = new ArrayList<>();
        List<Integer> dictionaryIds = new ArrayList<>();

        byte[] dictionaryPage = generatePlainValuesPage(dictionarySize, 96, dictionary);
        byte[] dataPage = generateDictionaryIdPage2048(dictionarySize - 1, dictionaryIds);

        List<Object> expectedValues = new ArrayList<>();
        for (Integer dictionaryId : dictionaryIds) {
            expectedValues.add(dictionary.get(dictionaryId));
        }

        TimestampDictionary tsDictionary = new TimestampDictionary(new DictionaryPage(Slices.wrappedBuffer(dictionaryPage), dictionarySize, PLAIN_DICTIONARY));

        timestampBatchReadWithSkipHelper(valueCount, 0, valueCount, timestampDictionary(dataPage, dictionarySize, tsDictionary), expectedValues);
        timestampBatchReadWithSkipHelper(29, 0, valueCount, timestampDictionary(dataPage, dictionarySize, tsDictionary), expectedValues);
        timestampBatchReadWithSkipHelper(89, 0, valueCount, timestampDictionary(dataPage, dictionarySize, tsDictionary), expectedValues);
        timestampBatchReadWithSkipHelper(1024, 0, valueCount, timestampDictionary(dataPage, dictionarySize, tsDictionary), expectedValues);

        timestampBatchReadWithSkipHelper(256, 29, valueCount, timestampDictionary(dataPage, dictionarySize, tsDictionary), expectedValues);
        timestampBatchReadWithSkipHelper(89, 29, valueCount, timestampDictionary(dataPage, dictionarySize, tsDictionary), expectedValues);
        timestampBatchReadWithSkipHelper(1024, 1024, valueCount, timestampDictionary(dataPage, dictionarySize, tsDictionary), expectedValues);
    }

    @Test
    public void testBooleanPlain()
    {
        int valueCount = 2048;
        List<Object> expectedValues = new ArrayList<>();

        byte[] pageBytes = generatePlainValuesPage(valueCount, 1, expectedValues);

        booleanBatchReadWithSkipHelper(valueCount, 0, valueCount, booleanPlain(pageBytes), expectedValues); // read all values in one batch
        booleanBatchReadWithSkipHelper(29, 0, valueCount, booleanPlain(pageBytes), expectedValues);
        booleanBatchReadWithSkipHelper(89, 0, valueCount, booleanPlain(pageBytes), expectedValues);
        booleanBatchReadWithSkipHelper(1024, 0, valueCount, booleanPlain(pageBytes), expectedValues);

        booleanBatchReadWithSkipHelper(256, 29, valueCount, booleanPlain(pageBytes), expectedValues);
        booleanBatchReadWithSkipHelper(89, 29, valueCount, booleanPlain(pageBytes), expectedValues);
        booleanBatchReadWithSkipHelper(1024, 1024, valueCount, booleanPlain(pageBytes), expectedValues);
    }

    @Test
    public void testBooleanRLE()
    {
        int valueCount = 2048;
        List<Integer> values = new ArrayList<>();

        byte[] dataPage = generateDictionaryIdPage2048(1, values);

        List<Object> expectedValues = new ArrayList<>(values);

        booleanBatchReadWithSkipHelper(valueCount, 0, valueCount, booleanRLE(dataPage), expectedValues);
        booleanBatchReadWithSkipHelper(29, 0, valueCount, booleanRLE(dataPage), expectedValues);
        booleanBatchReadWithSkipHelper(89, 0, valueCount, booleanRLE(dataPage), expectedValues);
        booleanBatchReadWithSkipHelper(1024, 0, valueCount, booleanRLE(dataPage), expectedValues);

        booleanBatchReadWithSkipHelper(256, 29, valueCount, booleanRLE(dataPage), expectedValues);
        booleanBatchReadWithSkipHelper(89, 29, valueCount, booleanRLE(dataPage), expectedValues);
        booleanBatchReadWithSkipHelper(1024, 1024, valueCount, booleanRLE(dataPage), expectedValues);
    }

    @Test
    public void testInt32ShortDecimalPlain()
            throws IOException
    {
        int valueCount = 2048;
        List<Object> expectedValues = new ArrayList<>();

        byte[] pageBytes = generatePlainValuesPage(valueCount, 32, expectedValues);
        int32ShortDecimalBatchReadWithSkipHelper(valueCount, 0, valueCount, int32ShortDecimalPlain(pageBytes), expectedValues); // read all values in one batch
        int32ShortDecimalBatchReadWithSkipHelper(29, 0, valueCount, int32ShortDecimalPlain(pageBytes), expectedValues);
        int32ShortDecimalBatchReadWithSkipHelper(89, 0, valueCount, int32ShortDecimalPlain(pageBytes), expectedValues);
        int32ShortDecimalBatchReadWithSkipHelper(1024, 0, valueCount, int32ShortDecimalPlain(pageBytes), expectedValues);

        int32ShortDecimalBatchReadWithSkipHelper(256, 29, valueCount, int32ShortDecimalPlain(pageBytes), expectedValues);
        int32ShortDecimalBatchReadWithSkipHelper(89, 29, valueCount, int32ShortDecimalPlain(pageBytes), expectedValues);
        int32ShortDecimalBatchReadWithSkipHelper(1024, 1024, valueCount, int32ShortDecimalPlain(pageBytes), expectedValues);
    }

    @Test
    public void testInt64ShortDecimalPlain()
            throws IOException
    {
        int valueCount = 2048;
        List<Object> expectedValues = new ArrayList<>();

        byte[] pageBytes = generatePlainValuesPage(valueCount, 64, expectedValues);
        int64ShortDecimalBatchReadWithSkipHelper(valueCount, 0, valueCount, int64ShortDecimalPlain(pageBytes), expectedValues); // read all values in one batch
        int64ShortDecimalBatchReadWithSkipHelper(29, 0, valueCount, int64ShortDecimalPlain(pageBytes), expectedValues);
        int64ShortDecimalBatchReadWithSkipHelper(89, 0, valueCount, int64ShortDecimalPlain(pageBytes), expectedValues);
        int64ShortDecimalBatchReadWithSkipHelper(1024, 0, valueCount, int64ShortDecimalPlain(pageBytes), expectedValues);

        int64ShortDecimalBatchReadWithSkipHelper(256, 29, valueCount, int64ShortDecimalPlain(pageBytes), expectedValues);
        int64ShortDecimalBatchReadWithSkipHelper(89, 29, valueCount, int64ShortDecimalPlain(pageBytes), expectedValues);
        int64ShortDecimalBatchReadWithSkipHelper(1024, 1024, valueCount, int64ShortDecimalPlain(pageBytes), expectedValues);
    }

    @Test
    public void testInt32ShortDecimalRLE()
            throws IOException
    {
        int valueCount = 2048;
        int dictionarySize = 29;
        List<Object> dictionary = new ArrayList<>();
        List<Integer> dictionaryIds = new ArrayList<>();

        byte[] dictionaryPage = generatePlainValuesPage(dictionarySize, 32, dictionary);
        byte[] dataPage = generateDictionaryIdPage2048(dictionarySize - 1, dictionaryIds);

        List<Object> expectedValues = new ArrayList<>();
        for (Integer dictionaryId : dictionaryIds) {
            expectedValues.add(dictionary.get(dictionaryId));
        }

        IntegerDictionary integerDictionary = new IntegerDictionary(new DictionaryPage(Slices.wrappedBuffer(dictionaryPage), dictionarySize, PLAIN_DICTIONARY));

        int32ShortDecimalBatchReadWithSkipHelper(valueCount, 0, valueCount, int32ShortDecimalRLE(dataPage, dictionarySize, integerDictionary), expectedValues); // read all values in one batch
        int32ShortDecimalBatchReadWithSkipHelper(29, 0, valueCount, int32ShortDecimalRLE(dataPage, dictionarySize, integerDictionary), expectedValues);
        int32ShortDecimalBatchReadWithSkipHelper(89, 0, valueCount, int32ShortDecimalRLE(dataPage, dictionarySize, integerDictionary), expectedValues);
        int32ShortDecimalBatchReadWithSkipHelper(1024, 0, valueCount, int32ShortDecimalRLE(dataPage, dictionarySize, integerDictionary), expectedValues);

        int32ShortDecimalBatchReadWithSkipHelper(256, 29, valueCount, int32ShortDecimalRLE(dataPage, dictionarySize, integerDictionary), expectedValues);
        int32ShortDecimalBatchReadWithSkipHelper(89, 29, valueCount, int32ShortDecimalRLE(dataPage, dictionarySize, integerDictionary), expectedValues);
        int32ShortDecimalBatchReadWithSkipHelper(1024, 1024, valueCount, int32ShortDecimalRLE(dataPage, dictionarySize, integerDictionary), expectedValues);
    }

    @Test
    public void testInt64ShortDecimalRLE()
            throws IOException
    {
        int valueCount = 2048;
        int dictionarySize = 29;
        List<Object> dictionary = new ArrayList<>();
        List<Integer> dictionaryIds = new ArrayList<>();

        byte[] dictionaryPage = generatePlainValuesPage(dictionarySize, 64, dictionary);
        byte[] dataPage = generateDictionaryIdPage2048(dictionarySize - 1, dictionaryIds);

        List<Object> expectedValues = new ArrayList<>();
        for (Integer dictionaryId : dictionaryIds) {
            expectedValues.add(dictionary.get(dictionaryId));
        }

        LongDictionary longDictionary = new LongDictionary(new DictionaryPage(Slices.wrappedBuffer(dictionaryPage), dictionarySize, PLAIN_DICTIONARY));

        int64ShortDecimalBatchReadWithSkipHelper(valueCount, 0, valueCount, int64ShortDecimalRLE(dataPage, dictionarySize, longDictionary), expectedValues); // read all values in one batch
        int64ShortDecimalBatchReadWithSkipHelper(29, 0, valueCount, int64ShortDecimalRLE(dataPage, dictionarySize, longDictionary), expectedValues);
        int64ShortDecimalBatchReadWithSkipHelper(89, 0, valueCount, int64ShortDecimalRLE(dataPage, dictionarySize, longDictionary), expectedValues);
        int64ShortDecimalBatchReadWithSkipHelper(1024, 0, valueCount, int64ShortDecimalRLE(dataPage, dictionarySize, longDictionary), expectedValues);

        int64ShortDecimalBatchReadWithSkipHelper(256, 29, valueCount, int64ShortDecimalRLE(dataPage, dictionarySize, longDictionary), expectedValues);
        int64ShortDecimalBatchReadWithSkipHelper(89, 29, valueCount, int64ShortDecimalRLE(dataPage, dictionarySize, longDictionary), expectedValues);
        int64ShortDecimalBatchReadWithSkipHelper(1024, 1024, valueCount, int64ShortDecimalRLE(dataPage, dictionarySize, longDictionary), expectedValues);
    }

    @Test
    public void testUuidPlainPlain()
            throws IOException
    {
        int valueCount = 2048;
        List<Object> expectedValues = new ArrayList<>();

        byte[] pageBytes = generatePlainValuesPage(valueCount, 128, expectedValues);
        // page is read assuming in big endian, so we need to flip the bytes around when comparing read values
        expectedValues = expectedValues.stream()
                .map(Long.class::cast)
                .mapToLong(Long::longValue)
                .map(Long::reverseBytes)
                .boxed()
                .collect(toImmutableList());
        uuidBatchReadWithSkipHelper(valueCount, 0, valueCount, uuidPlain(pageBytes), expectedValues);
        uuidBatchReadWithSkipHelper(29, 0, valueCount, uuidPlain(pageBytes), expectedValues);
        uuidBatchReadWithSkipHelper(89, 0, valueCount, uuidPlain(pageBytes), expectedValues);
        uuidBatchReadWithSkipHelper(1024, 0, valueCount, uuidPlain(pageBytes), expectedValues);

        uuidBatchReadWithSkipHelper(256, 29, valueCount, uuidPlain(pageBytes), expectedValues);
        uuidBatchReadWithSkipHelper(89, 29, valueCount, uuidPlain(pageBytes), expectedValues);
        uuidBatchReadWithSkipHelper(1024, 1024, valueCount, uuidPlain(pageBytes), expectedValues);
    }

    @Test
    public void testUuidRLEDictionary()
            throws IOException
    {
        int valueCount = 2048;
        int dictionarySize = 29;
        List<Object> dictionary = new ArrayList<>();
        List<Integer> dictionaryIds = new ArrayList<>();

        byte[] dictionaryPage = generatePlainValuesPage(dictionarySize, 128, dictionary);
        byte[] dataPage = generateDictionaryIdPage2048(dictionarySize - 1, dictionaryIds);

        List<Object> expectedValues = new ArrayList<>();
        for (Integer dictionaryId : dictionaryIds) {
            expectedValues.add(dictionary.get(dictionaryId * 2));
            expectedValues.add(dictionary.get((dictionaryId * 2) + 1));
        }

        expectedValues = expectedValues.stream()
                .map(Long.class::cast)
                .mapToLong(Long::longValue)
                .map(Long::reverseBytes)
                .boxed()
                .collect(toImmutableList());

        BinaryBatchDictionary binaryDictionary = new BinaryBatchDictionary(new DictionaryPage(Slices.wrappedBuffer(dictionaryPage), dictionarySize, PLAIN_DICTIONARY), 16);
        uuidBatchReadWithSkipHelper(valueCount, 0, valueCount, uuidRle(dataPage, dictionarySize, binaryDictionary), expectedValues);
        uuidBatchReadWithSkipHelper(29, 0, valueCount, uuidRle(dataPage, dictionarySize, binaryDictionary), expectedValues);
        uuidBatchReadWithSkipHelper(89, 0, valueCount, uuidRle(dataPage, dictionarySize, binaryDictionary), expectedValues);
        uuidBatchReadWithSkipHelper(1024, 0, valueCount, uuidRle(dataPage, dictionarySize, binaryDictionary), expectedValues);

        uuidBatchReadWithSkipHelper(256, 29, valueCount, uuidRle(dataPage, dictionarySize, binaryDictionary), expectedValues);
        uuidBatchReadWithSkipHelper(89, 29, valueCount, uuidRle(dataPage, dictionarySize, binaryDictionary), expectedValues);
        uuidBatchReadWithSkipHelper(1024, 1024, valueCount, uuidRle(dataPage, dictionarySize, binaryDictionary), expectedValues);
    }

    public static class TestValueDecodersArbitrary
            extends TestValuesDecoders
    {
        public static final int ARBITRARY_VALUE = 237;

        @Override
        public byte[] generatePlainValuesPage(int valueCount, int valueSizeBits, List<Object> addedValues)
        {
            int positiveUpperBoundedInt = getPositiveUpperBoundedInt(valueSizeBits);
            return TestParquetUtils.generatePlainValuesPage(valueCount, valueSizeBits, addedValues, ARBITRARY_VALUE, ARBITRARY_VALUE * (1L << 31), positiveUpperBoundedInt);
        }

        @Override
        public byte[] generateDictionaryIdPage2048(int maxValue, List<Integer> addedValues)
        {
            return TestParquetUtils.generateDictionaryIdPage2048(maxValue, addedValues, ARBITRARY_VALUE % maxValue);
        }

        private int getPositiveUpperBoundedInt(int valueSizeBits)
        {
            if (valueSizeBits == 1) {
                return ARBITRARY_VALUE % 2;
            }
            if (valueSizeBits == 96) {
                return ARBITRARY_VALUE % 1572281176;
            }
            return ARBITRARY_VALUE;
        }
    }

    public static class TestValueDecodersLowerBounded
            extends TestValuesDecoders
    {
        @Override
        public byte[] generatePlainValuesPage(int valueCount, int valueSizeBits, List<Object> addedValues)
        {
            return TestParquetUtils.generatePlainValuesPage(valueCount, valueSizeBits, addedValues, Integer.MIN_VALUE, 0L, getPositiveUpperBoundedInt());
        }

        private int getPositiveUpperBoundedInt()
        {
            return 0;
        }

        @Override
        public byte[] generateDictionaryIdPage2048(int maxValue, List<Integer> addedValues)
        {
            return TestParquetUtils.generateDictionaryIdPage2048(maxValue, addedValues, getPositiveUpperBoundedInt());
        }
    }

    public static class TestValueDecodersUpperBounded
            extends TestValuesDecoders
    {
        private static int getPositiveUpperBoundedInt(int valueSizeBits)
        {
            int positiveUpperBoundedInt = Integer.MAX_VALUE;
            if (valueSizeBits == 1) {
                positiveUpperBoundedInt = 1;
            }
            if (valueSizeBits == 96) {
                positiveUpperBoundedInt = 1572281175;
            }
            return positiveUpperBoundedInt;
        }

        @Override
        public byte[] generatePlainValuesPage(int valueCount, int valueSizeBits, List<Object> addedValues)
        {
            int positiveUpperBoundedInt = getPositiveUpperBoundedInt(valueSizeBits);
            return TestParquetUtils.generatePlainValuesPage(valueCount, valueSizeBits, addedValues, Integer.MAX_VALUE, Long.MAX_VALUE, positiveUpperBoundedInt);
        }

        @Override
        public byte[] generateDictionaryIdPage2048(int maxValue, List<Integer> addedValues)
        {
            return TestParquetUtils.generateDictionaryIdPage2048(maxValue, addedValues, Math.abs(maxValue));
        }
    }
}
