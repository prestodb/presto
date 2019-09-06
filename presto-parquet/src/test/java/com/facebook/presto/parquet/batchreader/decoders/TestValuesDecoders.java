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
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int32ValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.plain.Int32PlainValuesDecoder;
import com.facebook.presto.parquet.batchreader.decoders.rle.Int32RLEDictionaryValuesDecoder;
import com.facebook.presto.parquet.dictionary.IntegerDictionary;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.facebook.presto.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static com.facebook.presto.parquet.batchreader.decoders.TestParquetUtils.generateDictionaryIdPage2048;
import static com.facebook.presto.parquet.batchreader.decoders.TestParquetUtils.generatePlainValuesPage;
import static java.lang.Math.min;
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

    @Test
    public void testInt32Plain()
            throws IOException
    {
        int valueCount = 2048;
        List<Object> expectedValues = new ArrayList<>();

        byte[] pageBytes = generatePlainValuesPage(valueCount, 4, new Random(89), expectedValues);

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

        byte[] dictionaryPage = generatePlainValuesPage(dictionarySize, 4, random, dictionary);
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
}
