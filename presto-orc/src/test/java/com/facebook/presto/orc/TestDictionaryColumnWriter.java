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
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.facebook.presto.orc.metadata.StripeFooter;
import com.facebook.presto.orc.writer.DictionaryColumnWriter;
import com.facebook.presto.orc.writer.SliceDictionaryColumnWriter;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.createCustomOrcSelectiveRecordReader;
import static com.facebook.presto.orc.OrcTester.createOrcWriter;
import static com.facebook.presto.orc.OrcWriterOptions.DEFAULT_MAX_COMPRESSION_BUFFER_SIZE;
import static com.facebook.presto.orc.OrcWriterOptions.DEFAULT_MAX_STRING_STATISTICS_LIMIT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_DIRECT;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
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
    public void testStringDirectConversion()
    {
        CompressionParameters compressionParameters = new CompressionParameters(
                CompressionKind.NONE,
                OptionalInt.empty(),
                toIntExact(DEFAULT_MAX_COMPRESSION_BUFFER_SIZE.toBytes()));
        DictionaryColumnWriter writer = new SliceDictionaryColumnWriter(
                0,
                VARCHAR,
                compressionParameters,
                Optional.empty(),
                ORC,
                DEFAULT_MAX_STRING_STATISTICS_LIMIT,
                ORC.createMetadataWriter());

        // a single row group exceeds 2G after direct conversion
        byte[] value = new byte[megabytes(1)];
        ThreadLocalRandom.current().nextBytes(value);
        Block data = RunLengthEncodedBlock.create(VARCHAR, Slices.wrappedBuffer(value), 3000);
        writer.beginRowGroup();
        writer.writeBlock(data);
        writer.finishRowGroup();
        assertFalse(writer.tryConvertToDirect(megabytes(64)).isPresent());
    }

    @Test
    public void testStringNoRows()
            throws Exception
    {
        List<String> values = ImmutableList.of();
        for (OrcEncoding encoding : OrcEncoding.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            List<StripeFooter> stripeFooters = testStringDictionary(directConversionTester, encoding, values);
            assertEquals(stripeFooters.size(), getStripeSize(values.size()));
        }
    }

    @Test
    public void testStringAllNullsWithDirectConversion()
            throws Exception
    {
        List<String> values = newArrayList(limit(cycle(new String[] {null}), 90_000));
        for (OrcEncoding encoding : OrcEncoding.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            directConversionTester.add(7, megabytes(1), true);
            directConversionTester.add(14, megabytes(1), true);
            directConversionTester.add(32, megabytes(1), true);
            List<StripeFooter> stripeFooters = testStringDictionary(directConversionTester, encoding, values);
            verifyDirectEncoding(getStripeSize(values.size()), encoding, stripeFooters);
        }
    }

    @Test
    public void testStringNonRepeatingValues()
            throws Exception
    {
        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
        for (int i = 0; i < 60_000; i++) {
            builder.add(Integer.toString(i));
        }
        List<String> values = builder.build();
        for (OrcEncoding encoding : OrcEncoding.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            List<StripeFooter> stripeFooters = testStringDictionary(directConversionTester, encoding, values);
            verifyDirectEncoding(getStripeSize(values.size()), encoding, stripeFooters);
        }
    }

    @Test
    public void testStringRepeatingValues()
            throws Exception
    {
        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
        for (int i = 0; i < 60_000; i++) {
            // Make a 7 letter String, by using million as base to force dictionary encoding.
            builder.add(Integer.toString((i % 1000) + 1_000_000));
        }
        for (OrcEncoding encoding : OrcEncoding.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            List<String> values = builder.build();
            List<StripeFooter> stripeFooters = testStringDictionary(directConversionTester, encoding, values);
            verifyDictionaryEncoding(getStripeSize(values.size()), encoding, stripeFooters, ImmutableList.of(1000, 1000, 1000, 1000));
        }
    }

    @Test
    public void testStringRepeatingValuesWithDirectConversion()
            throws Exception
    {
        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
        for (int i = 0; i < 60_000; i++) {
            builder.add(Integer.toString((i % 2000) + 1_000_000));
        }
        List<String> values = builder.build();

        for (OrcEncoding encoding : OrcEncoding.values()) {
            DirectConversionTester directConversionTester = new DirectConversionTester();
            directConversionTester.add(0, megabytes(1), true);
            directConversionTester.add(16, 5_000, false);
            directConversionTester.add(16, megabytes(1), true);

            List<StripeFooter> stripeFooters = testStringDictionary(directConversionTester, encoding, values);
            assertEquals(getStripeSize(values.size()), stripeFooters.size());
            verifyDirectEncoding(stripeFooters, encoding, 0);
            verifyDirectEncoding(stripeFooters, encoding, 1);
            verifyDictionaryEncoding(stripeFooters, encoding, 2, 2000);
            verifyDictionaryEncoding(stripeFooters, encoding, 3, 2000);
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
    public void testIntegerDictionaryRepeatingValues()
            throws IOException
    {
        ImmutableList<Integer> baseList = ImmutableList.of(Integer.MAX_VALUE, Integer.MIN_VALUE);
        ImmutableList.Builder<Integer> builder = new ImmutableList.Builder<>();
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
        stripeFooters = testDictionary(INTEGER, DWRF, false, new DirectConversionTester(), values);
        verifyDwrfDirectEncoding(getStripeSize(values.size()), stripeFooters);
    }

    @Test
    public void testIntegerDictionaryNonRepeating()
            throws IOException
    {
        DirectConversionTester directConversionTester = new DirectConversionTester();
        ImmutableList<Integer> baseList = ImmutableList.of(Integer.MAX_VALUE, Integer.MIN_VALUE);
        ImmutableList.Builder<Integer> builder = new ImmutableList.Builder<>();
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
        ImmutableList.Builder<Integer> builder = new ImmutableList.Builder<>();
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
        ImmutableList.Builder<Long> builder = new ImmutableList.Builder<>();
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
        ImmutableList.Builder<Long> builder = new ImmutableList.Builder<>();
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

    private ColumnEncoding getColumnEncoding(List<StripeFooter> stripeFooters, int stripeId)
    {
        StripeFooter stripeFooter = stripeFooters.get(stripeId);
        return stripeFooter.getColumnEncodings().get(COLUMN_ID);
    }

    private void verifyDwrfDirectEncoding(List<StripeFooter> stripeFooters, int stripeId)
    {
        assertEquals(getColumnEncoding(stripeFooters, stripeId).getColumnEncodingKind(), DWRF_DIRECT);
    }

    private void verifyDirectEncoding(List<StripeFooter> stripeFooters, OrcEncoding encoding, int stripeId)
    {
        ColumnEncoding columnEncoding = getColumnEncoding(stripeFooters, stripeId);
        if (encoding.equals(DWRF)) {
            assertEquals(columnEncoding.getColumnEncodingKind(), DIRECT);
        }
        else {
            assertEquals(encoding, ORC);
            assertEquals(columnEncoding.getColumnEncodingKind(), DIRECT_V2);
        }
    }

    private void verifyDictionaryEncoding(List<StripeFooter> stripeFooters, OrcEncoding encoding, int stripeId, int dictionarySize)
    {
        ColumnEncoding columnEncoding = getColumnEncoding(stripeFooters, stripeId);
        if (encoding.equals(DWRF)) {
            assertEquals(columnEncoding.getColumnEncodingKind(), DICTIONARY);
        }
        else {
            assertEquals(encoding, ORC);
            assertEquals(columnEncoding.getColumnEncodingKind(), DICTIONARY_V2);
        }
        assertEquals(columnEncoding.getDictionarySize(), dictionarySize);
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
        return testDictionary(BIGINT, DWRF, true, directConversionTester, values);
    }

    private List<StripeFooter> testIntegerDictionary(DirectConversionTester directConversionTester, List<?> values)
            throws IOException
    {
        return testDictionary(INTEGER, DWRF, true, directConversionTester, values);
    }

    private List<StripeFooter> testStringDictionary(DirectConversionTester directConversionTester, OrcEncoding encoding, List<String> values)
            throws IOException
    {
        return testDictionary(VARCHAR, encoding, false, directConversionTester, values);
    }

    private List<StripeFooter> testDictionary(Type type, OrcEncoding encoding, boolean enableIntDictionary, DirectConversionTester directConversionTester, List<?> values)
            throws IOException
    {
        List<Type> types = ImmutableList.of(type);
        OrcWriterOptions orcWriterOptions = new OrcWriterOptions().withStripeMaxRowCount(STRIPE_MAX_ROWS).withIntegerDictionaryEncodingEnabled(enableIntDictionary);
        try (TempFile tempFile = new TempFile()) {
            OrcWriter writer = createOrcWriter(tempFile.getFile(), encoding, ZSTD, Optional.empty(), types, orcWriterOptions, new OrcWriterStats());

            int index = 0;
            int batchId = 0;
            while (index < values.size()) {
                int end = Math.min(index + BATCH_ROWS, values.size());
                BlockBuilder blockBuilder = type.createBlockBuilder(null, BATCH_ROWS);
                while (index < end) {
                    Object value = values.get(index++);
                    if (value == null) {
                        blockBuilder.appendNull();
                    }
                    else {
                        if (type.equals(VARCHAR)) {
                            String strValue = (String) value;
                            type.writeSlice(blockBuilder, utf8Slice(strValue));
                        }
                        else {
                            Number number = (Number) value;
                            type.writeLong(blockBuilder, number.longValue());
                        }
                    }
                }
                Block[] blocks = new Block[] {blockBuilder.build()};
                writer.write(new Page(blocks));
                directConversionTester.validate(batchId, writer);
                batchId++;
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
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        Object value = values.get(index++);
                        assertEquals(block.isNull(i), value == null);
                        if (value != null) {
                            if (type.equals(VARCHAR)) {
                                assertEquals(block.getSlice(i, 0, block.getSliceLength(i)), utf8Slice((String) value));
                            }
                            else {
                                Number number = (Number) value;
                                assertEquals(type.getLong(block, i), number.longValue());
                            }
                        }
                    }
                }
                assertEquals(index, values.size());
            }

            return OrcTester.getStripes(tempFile.getFile(), encoding);
        }
    }

    static class DirectConversionTester
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
                    writer.getDictionaryCompressionOptimizer().convertLowCompressionStreams(bufferedBytes);
                    assertTrue(columnWriter.isDirectEncoded(), "BatchId " + batchId + " bytes " + bufferedBytes);
                }
                index++;
            }
        }
    }
}
