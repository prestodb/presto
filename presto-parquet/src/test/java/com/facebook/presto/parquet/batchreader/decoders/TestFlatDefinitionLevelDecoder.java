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

import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.facebook.presto.parquet.batchreader.decoders.TestParquetUtils.addDLRLEBlock;
import static com.facebook.presto.parquet.batchreader.decoders.TestParquetUtils.addDLValues;
import static com.facebook.presto.parquet.batchreader.decoders.TestParquetUtils.randomValues;
import static java.lang.Math.min;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestFlatDefinitionLevelDecoder
{
    private static int valueCount;
    private static int nonNullCount;
    private static byte[] pageBytes;
    private static List<Integer> expectedValues = new ArrayList<>();

    @BeforeClass
    public void setup()
            throws IOException
    {
        Random random = new Random(200);
        RunLengthBitPackingHybridEncoder encoder = TestParquetUtils.getSimpleDLEncoder();

        addDLRLEBlock(1, 50, encoder, expectedValues);
        addDLValues(randomValues(random, 457, 1), encoder, expectedValues);
        addDLRLEBlock(0, 37, encoder, expectedValues);
        addDLValues(randomValues(random, 186, 1), encoder, expectedValues);

        valueCount = expectedValues.size();
        for (Integer value : expectedValues) {
            nonNullCount += value;
        }
        pageBytes = encoder.toBytes().toByteArray();
    }

    @Test
    public void rleOnlyBlock()
            throws IOException
    {
        rleOnlyBlockHelper(true);
        rleOnlyBlockHelper(false);
    }

    private static void rleOnlyBlockHelper(boolean rleValue)
            throws IOException
    {
        boolean[] isNulls = new boolean[200];

        // read all at once
        {
            FlatDefinitionLevelDecoder dlDecoder = new FlatDefinitionLevelDecoder(rleValue ? 0 : 1, 200);
            int nonNullCount = dlDecoder.readNext(isNulls, 0, 200);
            assertEquals(nonNullCount, rleValue ? 0 : 200);
            for (int i = 0; i < 200; i++) {
                assertEquals(isNulls[i], rleValue);
            }
        }

        // read in batches
        {
            FlatDefinitionLevelDecoder dlDecoder = new FlatDefinitionLevelDecoder(rleValue ? 0 : 1, 200);
            int offset = 0;
            int nonNullCount = 0;
            while (offset < 200) {
                nonNullCount += dlDecoder.readNext(isNulls, offset, min(29, 200 - offset));
                offset += 29;
            }

            assertEquals(nonNullCount, rleValue ? 0 : 200);
            for (int i = 0; i < 200; i++) {
                assertEquals(isNulls[i], rleValue);
            }
        }
    }

    @Test
    public void hybridReadInBatches()
            throws IOException
    {
        hybridReadInBatchesHelper(valueCount); // read all in one batch
        hybridReadInBatchesHelper(29);
        hybridReadInBatchesHelper(83);
        hybridReadInBatchesHelper(128);
    }

    private static void hybridReadInBatchesHelper(int batchSize)
            throws IOException
    {
        boolean[] isNulls = new boolean[valueCount];
        FlatDefinitionLevelDecoder dlDecoder = new FlatDefinitionLevelDecoder(valueCount, new ByteArrayInputStream(pageBytes));
        int offset = 0;
        int nonNullCount = 0;
        while (offset < valueCount) {
            nonNullCount += dlDecoder.readNext(isNulls, offset, min(batchSize, valueCount - offset));
            offset += batchSize;
        }

        compareExpectedActual(isNulls, nonNullCount);
    }

    private static void compareExpectedActual(boolean[] isNullsActual, int nonNullCountActual)
    {
        assertEquals(nonNullCountActual, nonNullCount);
        for (int i = 0; i < valueCount; i++) {
            assertEquals(isNullsActual[i], expectedValues.get(i) == 0);
        }
    }

    @Test
    public void tryReadingTooMany()
    {
        FlatDefinitionLevelDecoder dlDecoder = new FlatDefinitionLevelDecoder(valueCount, new ByteArrayInputStream(pageBytes));
        try {
            dlDecoder.readNext(new boolean[valueCount + 500], 0, valueCount + 500);
            fail("shouldn't come here");
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Failed to copy the requested number of definition levels");
        }
    }
}
