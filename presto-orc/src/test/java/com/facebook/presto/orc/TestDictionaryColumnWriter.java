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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.DictionaryCompressionOptimizer.DictionaryColumnManager;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.StripeFooter;
import com.facebook.presto.orc.writer.DictionaryColumnWriter;
import com.facebook.presto.orc.writer.SliceDictionaryColumnWriter;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.createCustomOrcSelectiveRecordReader;
import static com.facebook.presto.orc.OrcTester.createOrcWriter;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_DIRECT;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDictionaryColumnWriter
{
    private static final int COLUMN_ID = 1;
    private static final int BATCH_ROWS = 1_000;
    private static final int STRIPE_MAX_ROWS = 15_000;
    private static final Random RANDOM = new Random();

    private static int megabytes(int size)
    {
        return toIntExact(new DataSize(size, MEGABYTE).toBytes());
    }

    private static int getStripeSize(int size)
    {
        if (size == 0) {
            return 0;
        }
        return ((size - 1) / STRIPE_MAX_ROWS) + 1;
    }

    @Test
    public void testStringNoRows()
            throws Exception
    {
        List<String> values = ImmutableList.of();
        for (StringDictionaryInput input : StringDictionaryInput.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            List<StripeFooter> stripeFooters = testStringDictionary(directConversionTester, input, values);
            assertEquals(stripeFooters.size(), getStripeSize(values.size()));
        }
    }

    @Test
    public void testStringAllNullsWithDirectConversion()
            throws Exception
    {
        List<String> values = newArrayList(limit(cycle(new String[] {null}), 90_000));
        for (StringDictionaryInput input : StringDictionaryInput.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            directConversionTester.add(7, megabytes(1), true);
            directConversionTester.add(14, megabytes(1), true);
            directConversionTester.add(32, megabytes(1), true);
            List<StripeFooter> stripeFooters = testStringDictionary(directConversionTester, input, values);
            verifyDirectEncoding(getStripeSize(values.size()), input.getEncoding(), stripeFooters);
        }
    }

    @Test
    public void testStringRandomValuesWithNull()
            throws Exception
    {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 60_000; i++) {
            values.add(RANDOM.nextBoolean() ? null : UUID.randomUUID().toString());
        }
        for (StringDictionaryInput input : StringDictionaryInput.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            List<StripeFooter> stripeFooters = testStringDictionary(directConversionTester, input, values);
            verifyDirectEncoding(getStripeSize(values.size()), input.getEncoding(), stripeFooters);
        }
    }

    @Test
    public void testStringRandomRepeatingValues()
            throws Exception
    {
        List<String> stripeValues = new ArrayList<>();
        int dictionarySize = 1_000;
        for (int i = 0; i < dictionarySize; i++) {
            stripeValues.add(UUID.randomUUID().toString());
        }

        for (int i = dictionarySize; i < STRIPE_MAX_ROWS; i++) {
            stripeValues.add(stripeValues.get(RANDOM.nextInt(dictionarySize)));
        }

        Collections.shuffle(stripeValues);
        List<String> values = new ArrayList<>(stripeValues);

        Collections.shuffle(stripeValues);
        values.addAll(stripeValues);

        Collections.shuffle(stripeValues);
        values.addAll(stripeValues);

        for (StringDictionaryInput input : StringDictionaryInput.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            List<StripeFooter> stripeFooters = testStringDictionary(directConversionTester, input, values);
            verifyDictionaryEncoding(getStripeSize(values.size()), input.getEncoding(), stripeFooters, ImmutableList.of(dictionarySize, dictionarySize, dictionarySize));
        }
    }

    @Test
    public void testStringNonRepeatingValues()
            throws Exception
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (int i = 0; i < 60_000; i++) {
            builder.add(Integer.toString(i));
        }
        List<String> values = builder.build();
        for (StringDictionaryInput input : StringDictionaryInput.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            List<StripeFooter> stripeFooters = testStringDictionary(directConversionTester, input, values);
            verifyDirectEncoding(getStripeSize(values.size()), input.getEncoding(), stripeFooters);
        }
    }

    @Test
    public void testStringIncreasedStrideSize()
            throws Exception
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (int i = 0; i < 60_000; i++) {
            builder.add(Integer.toString(i));
        }
        List<String> values = builder.build();
        for (StringDictionaryInput input : StringDictionaryInput.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                    .withRowGroupMaxRowCount(14_876)
                    .withStripeMaxRowCount(STRIPE_MAX_ROWS)
                    .build();
            testDictionary(VARCHAR, input.getEncoding(), writerOptions, directConversionTester, values);
        }
    }

    @Test
    public void testStringRepeatingValues()
            throws Exception
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (int i = 0; i < 60_000; i++) {
            // Make a 7 letter String, by using million as base to force dictionary encoding.
            builder.add(Integer.toString((i % 1000) + 1_000_000));
        }
        for (StringDictionaryInput input : StringDictionaryInput.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            List<String> values = builder.build();
            List<StripeFooter> stripeFooters = testStringDictionary(directConversionTester, input, values);
            verifyDictionaryEncoding(getStripeSize(values.size()), input.getEncoding(), stripeFooters, ImmutableList.of(1000, 1000, 1000, 1000));
        }
    }

    @Test
    public void testStringRepeatingValuesWithDirectConversion()
            throws Exception
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (int i = 0; i < 60_000; i++) {
            builder.add(Integer.toString((i % 2000) + 1_000_000));
        }
        List<String> values = builder.build();

        for (StringDictionaryInput input : StringDictionaryInput.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            directConversionTester.add(0, megabytes(1), true);
            directConversionTester.add(16, 5_000, false);
            directConversionTester.add(16, megabytes(1), true);

            List<StripeFooter> stripeFooters = testStringDictionary(directConversionTester, input, values);
            assertEquals(getStripeSize(values.size()), stripeFooters.size());
            verifyDirectEncoding(stripeFooters, input.getEncoding(), 0);
            verifyDirectEncoding(stripeFooters, input.getEncoding(), 1);
            verifyDictionaryEncoding(stripeFooters, input.getEncoding(), 2, 2000);
            verifyDictionaryEncoding(stripeFooters, input.getEncoding(), 3, 2000);
        }
    }

    @Test
    public void testStringPreserveDirectEncoding()
            throws IOException
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (long i = 0; i < STRIPE_MAX_ROWS; i++) {
            builder.add(Long.toString(Integer.MAX_VALUE + i));
        }
        int repeatInterval = 1500;
        for (long i = 0; i < 100_000; i++) {
            builder.add(Long.toString(Integer.MAX_VALUE + i % repeatInterval));
        }
        int preserveDirectEncodingStripeCount = 2;
        OrcWriterOptions.Builder orcWriterOptionsBuilder = OrcWriterOptions.builder()
                .withStripeMaxRowCount(STRIPE_MAX_ROWS)
                .withIntegerDictionaryEncodingEnabled(true)
                .withPreserveDirectEncodingStripeCount(preserveDirectEncodingStripeCount);

        List<String> values = builder.build();
        for (StringDictionaryInput input : StringDictionaryInput.values()) {
            DirectConversionTester tester = new DirectConversionTester();
            OrcWriterOptions orcWriterOptions = orcWriterOptionsBuilder
                    .withStringDictionarySortingEnabled(input.isSortStringDictionaryKeys())
                    .build();

            List<StripeFooter> stripeFooters = testDictionary(VARCHAR, input.getEncoding(), orcWriterOptions, tester, values);
            assertEquals(getStripeSize(values.size()), stripeFooters.size());
            for (int i = 0; i <= preserveDirectEncodingStripeCount; i++) {
                verifyDirectEncoding(stripeFooters, input.getEncoding(), i);
            }
            for (int i = preserveDirectEncodingStripeCount + 1; i < stripeFooters.size(); i++) {
                verifyDictionaryEncoding(stripeFooters, input.getEncoding(), i, repeatInterval);
            }
        }
    }

    @Test
    public void testIntegerNoRows()
            throws IOException
    {
        DirectConversionTester directConversionTester = new DirectConversionTester();
        List<Integer> values = ImmutableList.of();
        List<StripeFooter> stripeFooters = testIntegerDictionary(directConversionTester, values);
        assertEquals(stripeFooters.size(), getStripeSize(values.size()));
    }

    @Test
    public void testIntegerDictionaryAllNulls()
            throws IOException
    {
        DirectConversionTester directConversionTester = new DirectConversionTester();
        directConversionTester.add(7, megabytes(1), true);
        directConversionTester.add(14, megabytes(1), true);
        directConversionTester.add(32, megabytes(1), true);
        List<Integer> values = newArrayList(limit(cycle(new Integer[] {null}), 60_000));

        List<StripeFooter> stripeFooters = testIntegerDictionary(directConversionTester, values);
        verifyDwrfDirectEncoding(getStripeSize(values.size()), stripeFooters);
    }

    @Test
    public void testIntegerDictionaryAlternatingNulls()
            throws IOException
    {
        DirectConversionTester directConversionTester = new DirectConversionTester();
        List<Integer> values = newArrayList(limit(cycle(Integer.MAX_VALUE, null, Integer.MIN_VALUE), 60_000));

        List<StripeFooter> stripeFooters = testIntegerDictionary(directConversionTester, values);
        verifyDictionaryEncoding(getStripeSize(values.size()), DWRF, stripeFooters, ImmutableList.of(2, 2, 2, 2));
    }

    @Test
    public void testIntegerRandomValues()
            throws IOException
    {
        List<Integer> values = generateRandomIntegers(70_000);
        DirectConversionTester directConversionTester = new DirectConversionTester();
        testIntegerDictionary(directConversionTester, values);
    }

    @Test
    public void testIntegerIncreasedStrideSize()
            throws IOException
    {
        List<Integer> values = generateRandomIntegers(90_000);
        DirectConversionTester directConversionTester = new DirectConversionTester();
        OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                .withStripeMaxRowCount(STRIPE_MAX_ROWS)
                .withIntegerDictionaryEncodingEnabled(true)
                .withRowGroupMaxRowCount(14_998)
                .build();
        testDictionary(INTEGER, DWRF, writerOptions, directConversionTester, values);
    }

    private List<Integer> generateRandomIntegers(int maxSize)
    {
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < maxSize; i++) {
            values.add(RANDOM.nextBoolean() ? null : RANDOM.nextInt());
        }
        return values;
    }

    @Test
    public void testDictionaryRetainedSizeWithDifferentSettings()
    {
        DictionaryColumnWriter ignoredRowGroupWriter = getStringDictionaryColumnWriter(true);
        DictionaryColumnWriter withRowGroupWriter = getStringDictionaryColumnWriter(false);

        int numEntries = 10_000;
        int numBlocks = 10;
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, numEntries);
        Slice slice = utf8Slice("SomeString");
        for (int i = 0; i < numEntries; i++) {
            VARCHAR.writeSlice(blockBuilder, slice);
        }

        Block block = blockBuilder.build();
        for (int i = 0; i < numBlocks; i++) {
            writeBlock(ignoredRowGroupWriter, block);
            writeBlock(withRowGroupWriter, block);
        }

        long ignoredRowGroupBytes = ignoredRowGroupWriter.getRowGroupRetainedSizeInBytes();
        long withRowGroupBytes = withRowGroupWriter.getRowGroupRetainedSizeInBytes();
        long expectedDictionaryIndexSize = (numBlocks * numEntries * SIZE_OF_INT);

        String message = String.format("Ignored bytes %s With bytes %s", ignoredRowGroupBytes, withRowGroupBytes);
        assertTrue(ignoredRowGroupBytes + expectedDictionaryIndexSize <= withRowGroupBytes, message);
    }

    private void writeBlock(DictionaryColumnWriter writer, Block block)
    {
        writer.beginRowGroup();
        writer.writeBlock(block);
        writer.finishRowGroup();
    }

    @Test
    public void testLongRandomValues()
            throws IOException
    {
        List<Long> values = generateRandomLongs(70_000);
        DirectConversionTester directConversionTester = new DirectConversionTester();
        testLongDictionary(directConversionTester, values);
    }

    @Test
    public void testLongIncreasedStrideSize()
            throws IOException
    {
        List<Long> values = generateRandomLongs(80_000);
        DirectConversionTester directConversionTester = new DirectConversionTester();
        OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                .withStripeMaxRowCount(STRIPE_MAX_ROWS)
                .withIntegerDictionaryEncodingEnabled(true)
                .withRowGroupMaxRowCount(14_998)
                .build();
        testDictionary(BIGINT, DWRF, writerOptions, directConversionTester, values);
    }

    private static DictionaryColumnWriter getStringDictionaryColumnWriter(boolean ignoreRowGroupSizes)
    {
        OrcEncoding orcEncoding = DWRF;
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder()
                .setCompressionKind(SNAPPY)
                .setIgnoreDictionaryRowGroupSizes(ignoreRowGroupSizes)
                .build();
        return new SliceDictionaryColumnWriter(
                COLUMN_ID,
                VARCHAR,
                columnWriterOptions,
                Optional.empty(),
                orcEncoding,
                orcEncoding.createMetadataWriter());
    }

    private List<Long> generateRandomLongs(int maxSize)
    {
        List<Long> values = new ArrayList<>();
        for (int i = 0; i < maxSize; i++) {
            values.add(RANDOM.nextBoolean() ? null : RANDOM.nextLong());
        }
        return values;
    }

    @Test
    public void testIntegerDictionaryRepeatingValues()
            throws IOException
    {
        ImmutableList<Integer> baseList = ImmutableList.of(Integer.MAX_VALUE, Integer.MIN_VALUE);
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        builder.addAll(baseList);
        int repeatInterval = 1500;
        for (int i = baseList.size(); i < 45_000; i++) {
            builder.add(i % repeatInterval);
        }
        List<Integer> values = builder.build();

        DirectConversionTester directConversionTester = new DirectConversionTester();
        List<StripeFooter> stripeFooters = testIntegerDictionary(directConversionTester, values);
        verifyDictionaryEncoding(getStripeSize(values.size()), DWRF, stripeFooters, ImmutableList.of(repeatInterval + baseList.size(), repeatInterval, repeatInterval));

        // Now disable Integer dictionary encoding and verify that integer dictionary encoding is disabled
        stripeFooters = testDictionary(INTEGER, DWRF, false, true, new DirectConversionTester(), values);
        verifyDwrfDirectEncoding(getStripeSize(values.size()), stripeFooters);
    }

    @Test
    public void testIntegerDictionaryNonRepeating()
            throws IOException
    {
        DirectConversionTester directConversionTester = new DirectConversionTester();
        ImmutableList<Integer> baseList = ImmutableList.of(Integer.MAX_VALUE, Integer.MIN_VALUE);
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        builder.addAll(baseList);
        for (int i = baseList.size(); i < 60_000; i++) {
            builder.add(i);
        }
        List<Integer> values = builder.build();
        List<StripeFooter> stripeFooters = testIntegerDictionary(directConversionTester, values);
        verifyDwrfDirectEncoding(getStripeSize(values.size()), stripeFooters);
    }

    @Test
    public void testIntegerDictionaryRepeatingValuesDirect()
            throws IOException
    {
        DirectConversionTester directConversionTester = new DirectConversionTester();
        directConversionTester.add(0, 1000, false);
        directConversionTester.add(0, 4000, true);
        directConversionTester.add(16, 10_000, true);

        ImmutableList<Integer> baseList = ImmutableList.of(Integer.MAX_VALUE, Integer.MIN_VALUE);
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        builder.addAll(baseList);
        int repeatInterval = 1500;
        for (int i = baseList.size(); i < 60_000; i++) {
            builder.add(i % repeatInterval);
        }
        List<Integer> values = builder.build();
        List<StripeFooter> stripeFooters = testIntegerDictionary(directConversionTester, values);
        assertEquals(getStripeSize(values.size()), stripeFooters.size());
        verifyDwrfDirectEncoding(stripeFooters, 0);
        verifyDwrfDirectEncoding(stripeFooters, 1);
        verifyDictionaryEncoding(stripeFooters, DWRF, 2, repeatInterval);
        verifyDictionaryEncoding(stripeFooters, DWRF, 3, repeatInterval);
    }

    @Test
    public void testLongDictionaryNonRepeating()
            throws IOException
    {
        DirectConversionTester directConversionTester = new DirectConversionTester();
        ImmutableList<Long> baseList = ImmutableList.of(Long.MAX_VALUE, Long.MIN_VALUE);
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        builder.addAll(baseList);
        for (long i = baseList.size(); i < 100_000; i++) {
            builder.add(i);
        }
        List<Long> values = builder.build();
        List<StripeFooter> stripeFooters = testLongDictionary(directConversionTester, values);
        verifyDwrfDirectEncoding(getStripeSize(values.size()), stripeFooters);
    }

    @Test
    public void testLongDictionaryRepeatingValuesDirect()
            throws IOException
    {
        DirectConversionTester directConversionTester = new DirectConversionTester();
        directConversionTester.add(0, 1000, false);
        directConversionTester.add(0, 4000, true);
        directConversionTester.add(16, 10_000, true);

        ImmutableList<Long> baseList = ImmutableList.of(Long.MAX_VALUE, Long.MIN_VALUE);
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        builder.addAll(baseList);
        int repeatInterval = 1500;
        for (long i = baseList.size(); i < 50_000; i++) {
            builder.add(i % repeatInterval);
        }

        List<Long> values = builder.build();
        List<StripeFooter> stripeFooters = testLongDictionary(directConversionTester, values);
        assertEquals(getStripeSize(values.size()), stripeFooters.size());
        verifyDwrfDirectEncoding(stripeFooters, 0);
        verifyDwrfDirectEncoding(stripeFooters, 1);
        verifyDictionaryEncoding(stripeFooters, DWRF, 2, repeatInterval);
        verifyDictionaryEncoding(stripeFooters, DWRF, 3, repeatInterval);
    }

    @Test
    public void testLongPreserveDirectEncoding()
            throws IOException
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (long i = 0; i < STRIPE_MAX_ROWS; i++) {
            builder.add(i);
        }
        int repeatInterval = 1500;
        for (long i = 0; i < 100_000; i++) {
            builder.add(i % repeatInterval);
        }
        DirectConversionTester tester = new DirectConversionTester();
        int preserveDirectEncodingStripeCount = 2;
        OrcWriterOptions orcWriterOptions = OrcWriterOptions.builder()
                .withStripeMaxRowCount(STRIPE_MAX_ROWS)
                .withIntegerDictionaryEncodingEnabled(true)
                .withPreserveDirectEncodingStripeCount(preserveDirectEncodingStripeCount)
                .build();

        List<Long> values = builder.build();
        List<StripeFooter> stripeFooters = testDictionary(BIGINT, DWRF, orcWriterOptions, tester, values);
        assertEquals(getStripeSize(values.size()), stripeFooters.size());
        for (int i = 0; i <= preserveDirectEncodingStripeCount; i++) {
            verifyDwrfDirectEncoding(stripeFooters, i);
        }
        for (int i = preserveDirectEncodingStripeCount + 1; i < stripeFooters.size(); i++) {
            verifyDictionaryEncoding(stripeFooters, DWRF, i, repeatInterval);
        }
    }

    @Test
    public void verifyIntegerInList()
            throws IOException
    {
        List<List<Integer>> values = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            int childSize = i % 5;
            if (childSize == 4) {
                values.add(null);
            }
            else {
                List<Integer> childList = new ArrayList<>();
                for (int j = 0; j < childSize; j++) {
                    childList.add(i + j);
                }
                values.add(childList);
            }
        }
        DirectConversionTester directConversionTester = new DirectConversionTester();
        Type listType = new ArrayType(INTEGER);
        testDictionary(listType, DWRF, true, true, directConversionTester, values);
    }

    @Test
    public void verifyChildElementEmptyOrMissingInList()
            throws IOException
    {
        List<List<Integer>> values = new ArrayList<>();
        List<Integer> emptyChildList = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            int childSize = i % 2;
            values.add(childSize == 0 ? null : emptyChildList);
        }
        DirectConversionTester directConversionTester = new DirectConversionTester();
        Type listType = new ArrayType(INTEGER);
        testDictionary(listType, DWRF, true, true, directConversionTester, values);
    }

    @Test
    public void verifyStringInList()
            throws IOException
    {
        List<List<String>> values = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            int childSize = i % 5;
            if (childSize == 4) {
                values.add(null);
            }
            else {
                List<String> childList = new ArrayList<>();
                for (int j = 0; j < childSize; j++) {
                    childList.add(Integer.toString(i + j));
                }
                values.add(childList);
            }
        }
        DirectConversionTester directConversionTester = new DirectConversionTester();
        Type listType = new ArrayType(VARCHAR);
        testDictionary(listType, DWRF, true, true, directConversionTester, values);
    }

    @Test
    public void verifyStringEmptyOrMissingInList()
            throws IOException
    {
        List<List<String>> values = new ArrayList<>();
        List<String> emptyChildList = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            int childSize = i % 2;
            values.add(childSize == 0 ? null : emptyChildList);
        }
        DirectConversionTester directConversionTester = new DirectConversionTester();
        Type listType = new ArrayType(VARCHAR);
        testDictionary(listType, DWRF, true, true, directConversionTester, values);
    }

    private ColumnEncoding getColumnEncoding(List<StripeFooter> stripeFooters, int stripeId)
    {
        StripeFooter stripeFooter = stripeFooters.get(stripeId);
        return stripeFooter.getColumnEncodings().get(COLUMN_ID);
    }

    private void verifyDwrfDirectEncoding(List<StripeFooter> stripeFooters, int stripeId)
    {
        assertEquals(getColumnEncoding(stripeFooters, stripeId).getColumnEncodingKind(), DWRF_DIRECT, "StripeId " + stripeId);
    }

    private void verifyDirectEncoding(List<StripeFooter> stripeFooters, OrcEncoding encoding, int stripeId)
    {
        ColumnEncoding columnEncoding = getColumnEncoding(stripeFooters, stripeId);
        if (encoding.equals(DWRF)) {
            assertEquals(columnEncoding.getColumnEncodingKind(), DIRECT, "Encoding " + encoding + " StripeId " + stripeId);
        }
        else {
            assertEquals(encoding, ORC);
            assertEquals(columnEncoding.getColumnEncodingKind(), DIRECT_V2, "Encoding " + encoding + " StripeId " + stripeId);
        }
    }

    private void verifyDictionaryEncoding(List<StripeFooter> stripeFooters, OrcEncoding encoding, int stripeId, int dictionarySize)
    {
        ColumnEncoding columnEncoding = getColumnEncoding(stripeFooters, stripeId);
        if (encoding.equals(DWRF)) {
            assertEquals(columnEncoding.getColumnEncodingKind(), DICTIONARY, "Encoding " + encoding + " StripeId " + stripeId);
        }
        else {
            assertEquals(encoding, ORC);
            assertEquals(columnEncoding.getColumnEncodingKind(), DICTIONARY_V2, "Encoding " + encoding + " StripeId " + stripeId);
        }
        assertEquals(columnEncoding.getDictionarySize(), dictionarySize, "Encoding " + encoding + " StripeId " + stripeId);
    }

    private void verifyDictionaryEncoding(int stripeCount, OrcEncoding encoding, List<StripeFooter> stripeFooters, List<Integer> dictionarySizes)
    {
        assertEquals(stripeFooters.size(), stripeCount);
        for (int i = 0; i < stripeFooters.size(); i++) {
            verifyDictionaryEncoding(stripeFooters, encoding, i, dictionarySizes.get(i));
        }
    }

    private void verifyDirectEncoding(int stripeCount, OrcEncoding encoding, List<StripeFooter> stripeFooters)
    {
        assertEquals(stripeFooters.size(), stripeCount);
        for (int i = 0; i < stripeCount; i++) {
            verifyDirectEncoding(stripeFooters, encoding, i);
        }
    }

    private void verifyDwrfDirectEncoding(int stripeCount, List<StripeFooter> stripeFooters)
    {
        assertEquals(stripeFooters.size(), stripeCount);
        for (StripeFooter footer : stripeFooters) {
            ColumnEncoding encoding = footer.getColumnEncodings().get(COLUMN_ID);
            assertEquals(encoding.getColumnEncodingKind(), DWRF_DIRECT);
        }
    }

    private List<StripeFooter> testLongDictionary(DirectConversionTester directConversionTester, List<?> values)
            throws IOException
    {
        return testDictionary(BIGINT, DWRF, true, true, directConversionTester, values);
    }

    private List<StripeFooter> testIntegerDictionary(DirectConversionTester directConversionTester, List<?> values)
            throws IOException
    {
        return testDictionary(INTEGER, DWRF, true, true, directConversionTester, values);
    }

    private List<StripeFooter> testStringDictionary(DirectConversionTester directConversionTester, StringDictionaryInput dictionaryInput, List<String> values)
            throws IOException
    {
        return testDictionary(VARCHAR, dictionaryInput.getEncoding(), false, dictionaryInput.isSortStringDictionaryKeys(), directConversionTester, values);
    }

    private List<StripeFooter> testDictionary(Type type, OrcEncoding encoding, boolean enableIntDictionary, boolean sortStringDictionaryKeys, DirectConversionTester directConversionTester, List<?> values)
            throws IOException
    {
        OrcWriterOptions orcWriterOptions = OrcWriterOptions.builder()
                .withStripeMaxRowCount(STRIPE_MAX_ROWS)
                .withIntegerDictionaryEncodingEnabled(enableIntDictionary)
                .withStringDictionarySortingEnabled(sortStringDictionaryKeys)
                .build();
        return testDictionary(type, encoding, orcWriterOptions, directConversionTester, values);
    }

    private static boolean isArrayType(Type type)
    {
        return type.getTypeSignature().getBase().equals(ARRAY);
    }

    private void appendListToBlock(Type type, List<?> values, BlockBuilder blockBuilder, int startIndex, int endIndex)
    {
        while (startIndex < endIndex) {
            Object value = values.get(startIndex++);
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                if (isArrayType(type)) {
                    List<?> childList = (List<?>) value;
                    BlockBuilder childBlockBuilder = blockBuilder.beginBlockEntry();
                    appendListToBlock(type.getTypeParameters().get(0), childList, childBlockBuilder, 0, childList.size());
                    blockBuilder.closeEntry();
                }
                else if (type.equals(VARCHAR)) {
                    type.writeSlice(blockBuilder, utf8Slice((String) value));
                }
                else {
                    Number number = (Number) value;
                    type.writeLong(blockBuilder, number.longValue());
                }
            }
        }
    }

    private List<StripeFooter> testDictionary(Type type, OrcEncoding encoding, OrcWriterOptions orcWriterOptions, DirectConversionTester directConversionTester, List<?> values)
            throws IOException
    {
        List<Type> types = ImmutableList.of(type);
        try (TempFile tempFile = new TempFile()) {
            OrcWriter writer = createOrcWriter(tempFile.getFile(), encoding, ZSTD, Optional.empty(), types, orcWriterOptions, new OrcWriterStats());

            int index = 0;
            int batchId = 0;
            while (index < values.size()) {
                int end = Math.min(index + BATCH_ROWS, values.size());
                BlockBuilder blockBuilder = type.createBlockBuilder(null, end - index);
                appendListToBlock(type, values, blockBuilder, index, end);
                Block[] blocks = new Block[] {blockBuilder.build()};
                writer.write(new Page(blocks));
                directConversionTester.validate(batchId, writer);
                batchId++;
                index = end;
            }

            writer.close();
            writer.validate(new FileOrcDataSource(
                    tempFile.getFile(),
                    new DataSize(1, MEGABYTE),
                    new DataSize(1, MEGABYTE),
                    new DataSize(1, MEGABYTE),
                    true));

            index = 0;
            try (OrcSelectiveRecordReader reader = createCustomOrcSelectiveRecordReader(
                    tempFile,
                    encoding,
                    OrcPredicate.TRUE,
                    type,
                    INITIAL_BATCH_SIZE,
                    true)) {
                while (index < values.size()) {
                    Page page = reader.getNextPage();
                    if (page == null) {
                        break;
                    }

                    Block block = page.getBlock(0).getLoadedBlock();
                    index = verifyBlock(type, values, index, block);
                }
                assertEquals(index, values.size());
            }

            return OrcTester.getStripes(tempFile.getFile(), encoding);
        }
    }

    private int verifyBlock(Type type, List<?> values, int index, Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            Object value = values.get(index++);
            assertEquals(block.isNull(i), value == null);
            if (value != null) {
                if (type.equals(VARCHAR)) {
                    assertEquals(block.getSlice(i, 0, block.getSliceLength(i)), utf8Slice((String) value));
                }
                else if (isArrayType(type)) {
                    List<?> childList = (List<?>) value;
                    Block childBlock = block.getBlock(i);
                    int childIndex = verifyBlock(type.getTypeParameters().get(0), childList, 0, childBlock);
                    assertEquals(childIndex, childList.size());
                }
                else {
                    Number number = (Number) value;
                    assertEquals(type.getLong(block, i), number.longValue());
                }
            }
        }
        return index;
    }

    private static class StringDictionaryInput
    {
        private final OrcEncoding encoding;
        private final boolean sortStringDictionaryKeys;

        StringDictionaryInput(OrcEncoding encoding, boolean sortStringDictionaryKeys)
        {
            this.encoding = encoding;
            this.sortStringDictionaryKeys = sortStringDictionaryKeys;
        }

        public OrcEncoding getEncoding()
        {
            return encoding;
        }

        public boolean isSortStringDictionaryKeys()
        {
            return sortStringDictionaryKeys;
        }

        static List<StringDictionaryInput> values()
        {
            return ImmutableList.of(
                    new StringDictionaryInput(ORC, true),
                    new StringDictionaryInput(DWRF, true),
                    new StringDictionaryInput(DWRF, false));
        }
    }

    private static class DirectConversionTester
    {
        private final List<Integer> batchIds = new ArrayList<>();
        private final List<Integer> maxDirectBytes = new ArrayList<>();
        private final List<Boolean> results = new ArrayList<>();
        private int index;
        private int lastBatchId = -1;

        void add(int batchId, int maxBytes, boolean result)
        {
            batchIds.add(batchId);
            maxDirectBytes.add(maxBytes);
            results.add(result);
        }

        void validate(int batchId, OrcWriter writer)
        {
            checkState(batchId > lastBatchId);
            lastBatchId = batchId;
            while (true) {
                if (index >= batchIds.size() || batchIds.get(index) != batchId) {
                    return;
                }

                DictionaryColumnWriter columnWriter = (DictionaryColumnWriter) writer.getColumnWriters().get(0);
                assertFalse(columnWriter.isDirectEncoded(), "BatchId " + batchId + "is Direct encoded");

                int bufferedBytes = maxDirectBytes.get(index);
                if (!results.get(index)) {
                    // Failed Conversion to direct, can be invoked on column writer, as the dictionary
                    // compression optimizer state does not change.
                    assertFalse(columnWriter.tryConvertToDirect(bufferedBytes).isPresent(), "BatchId " + batchId + " bytes " + bufferedBytes);
                }
                else {
                    // Successful conversion to direct, changes the state of the dictionary compression
                    // optimizer and it should go only via dictionary compression optimizer.
                    List<DictionaryColumnManager> directConversionCandidates = writer.getDictionaryCompressionOptimizer().getDirectConversionCandidates();
                    boolean contains = directConversionCandidates.stream().anyMatch(x -> x.getDictionaryColumn() == columnWriter);
                    assertTrue(contains);
                    writer.getDictionaryCompressionOptimizer().convertLowCompressionStreams(true, bufferedBytes);
                    assertTrue(columnWriter.isDirectEncoded(), "BatchId " + batchId + " bytes " + bufferedBytes);
                    contains = directConversionCandidates.stream().anyMatch(x -> x.getDictionaryColumn() == columnWriter);
                    assertFalse(contains);
                }
                index++;
            }
        }
    }
}
