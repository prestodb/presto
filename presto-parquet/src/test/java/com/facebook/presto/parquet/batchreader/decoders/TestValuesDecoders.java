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
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int64TimestampMicrosValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int64ValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.TimestampValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.BinaryPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.BooleanPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int32PlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int64PlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int64TimestampMicrosPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.TimestampPlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.BinaryRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.BooleanRLEValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.Int32RLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.Int64RLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.Int64TimestampMicrosRLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.TimestampRLEDictionaryValuesDecoder;
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
import java.util.Random;
import java.util.stream.Collectors;

import static com.facebook.presto.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static com.facebook.presto.parquet.batchreader.decoders.TestParquetUtils.generateDictionaryIdPage2048;
import static com.facebook.presto.parquet.batchreader.decoders.TestParquetUtils.generatePlainValuesPage;
import static java.lang.Math.min;
import static org.apache.parquet.bytes.BytesUtils.UTF8;
import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.testng.Assert.assertEquals;

public class TestValuesDecoders
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

    private static Int64TimestampMicrosValuesDecoder int64TimestampMicrosPlain(byte[] pageBytes)
    {
        return new Int64TimestampMicrosPlainValuesDecoder(pageBytes, 0, pageBytes.length);
    }

    private static Int64ValuesDecoder int64Dictionary(byte[] pageBytes, int dictionarySize, LongDictionary dictionary)
    {
        return new Int64RLEDictionaryValuesDecoder(getWidthFromMaxInt(dictionarySize), new ByteArrayInputStream(pageBytes), dictionary);
    }

    private static Int64TimestampMicrosValuesDecoder int64TimestampMicrosDictionary(byte[] pageBytes, int dictionarySize, LongDictionary dictionary)
    {
        return new Int64TimestampMicrosRLEDictionaryValuesDecoder(getWidthFromMaxInt(dictionarySize), new ByteArrayInputStream(pageBytes), dictionary);
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
                byte[] expected = ((String) expectedValues.get(inputOffset + i)).getBytes(UTF8);
                byte[] actual = Arrays.copyOfRange(byteBuffer, offsets[i], offsets[i + 1]);
                assertEquals(expected, actual);
            }

            inputOffset += readBatchSize;

            int skipBatchSize = min(skipSize, valueCount - inputOffset);
            decoder.skip(skipBatchSize);
            inputOffset += skipBatchSize;
        }
    }

    private static void int64BatchReadWithSkipHelper(int batchSize, int skipSize, int valueCount, Int64TimestampMicrosValuesDecoder decoder, List<Object> expectedValues)
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

    @Test
    public void testInt32Plain()
            throws IOException
    {
        int valueCount = 2048;
        List<Object> expectedValues = new ArrayList<>();

        byte[] pageBytes = generatePlainValuesPage(valueCount, 32, new Random(89), expectedValues);

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
        Random random = new Random(83);
        int valueCount = 2048;
        int dictionarySize = 29;
        List<Object> dictionary = new ArrayList<>();
        List<Integer> dictionaryIds = new ArrayList<>();

        byte[] dictionaryPage = generatePlainValuesPage(dictionarySize, 32, random, dictionary);
        byte[] dataPage = generateDictionaryIdPage2048(dictionarySize - 1, random, dictionaryIds);

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

        byte[] pageBytes = generatePlainValuesPage(valueCount, -1, new Random(113), expectedValues);

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
        Random random = new Random(83);
        int valueCount = 2048;
        int dictionarySize = 29;
        List<Object> dictionary = new ArrayList<>();
        List<Integer> dictionaryIds = new ArrayList<>();

        byte[] dictionaryPage = TestParquetUtils.generatePlainValuesPage(dictionarySize, -1, random, dictionary);
        byte[] dataPage = TestParquetUtils.generateDictionaryIdPage2048(dictionarySize - 1, random, dictionaryIds);

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

        byte[] pageBytes = generatePlainValuesPage(valueCount, 64, new Random(89), expectedValues);

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
        Random random = new Random(83);
        int valueCount = 2048;
        int dictionarySize = 29;
        List<Object> dictionary = new ArrayList<>();
        List<Integer> dictionaryIds = new ArrayList<>();

        byte[] dictionaryPage = generatePlainValuesPage(dictionarySize, 64, random, dictionary);
        byte[] dataPage = generateDictionaryIdPage2048(dictionarySize - 1, random, dictionaryIds);

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

        byte[] pageBytes = generatePlainValuesPage(valueCount, 96, new Random(83), expectedValues);

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
        Random random = new Random(83);
        int valueCount = 2048;
        int dictionarySize = 29;
        List<Object> dictionary = new ArrayList<>();
        List<Integer> dictionaryIds = new ArrayList<>();

        byte[] dictionaryPage = generatePlainValuesPage(dictionarySize, 96, random, dictionary);
        byte[] dataPage = generateDictionaryIdPage2048(dictionarySize - 1, random, dictionaryIds);

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

        byte[] pageBytes = generatePlainValuesPage(valueCount, 1, new Random(83), expectedValues);

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
        Random random = new Random(111);
        int valueCount = 2048;
        List<Integer> values = new ArrayList<>();

        byte[] dataPage = generateDictionaryIdPage2048(1, random, values);

        List<Object> expectedValues = new ArrayList<>();
        for (Integer value : values) {
            expectedValues.add(value.intValue());
        }

        booleanBatchReadWithSkipHelper(valueCount, 0, valueCount, booleanRLE(dataPage), expectedValues);
        booleanBatchReadWithSkipHelper(29, 0, valueCount, booleanRLE(dataPage), expectedValues);
        booleanBatchReadWithSkipHelper(89, 0, valueCount, booleanRLE(dataPage), expectedValues);
        booleanBatchReadWithSkipHelper(1024, 0, valueCount, booleanRLE(dataPage), expectedValues);

        booleanBatchReadWithSkipHelper(256, 29, valueCount, booleanRLE(dataPage), expectedValues);
        booleanBatchReadWithSkipHelper(89, 29, valueCount, booleanRLE(dataPage), expectedValues);
        booleanBatchReadWithSkipHelper(1024, 1024, valueCount, booleanRLE(dataPage), expectedValues);
    }
}
