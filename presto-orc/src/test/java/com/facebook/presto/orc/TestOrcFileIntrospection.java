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
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.orc.NoOpOrcWriterStats.NOOP_WRITER_STATS;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcTester.createOrcWriter;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_INDEX;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestOrcFileIntrospection
{
    @Test
    public void testFileIntrospection()
            throws Exception
    {
        Type type = INTEGER;
        CapturingOrcFileIntrospector introspector = new CapturingOrcFileIntrospector();

        Page page = createTestPage(type, 15);

        // write two stripes, first stripe with two row groups of 5 rows each, and the second stripe with one row group with 5 rows
        try (TempFile tempFile = new TempFile()) {
            writeFile(type, page, tempFile);
            readFile(type, introspector, tempFile);
        }

        // check we got all objects
        assertNotNull(introspector.fileTail);
        assertNotNull(introspector.fileFooter);
        assertEquals(introspector.fileFooter.getNumberOfRows(), 15);

        assertEquals(introspector.stripes.size(), 2);
        assertEquals(introspector.stripes.get(0).getRowCount(), 10);
        assertEquals(introspector.stripes.get(1).getRowCount(), 5);

        assertEquals(introspector.stripeInformations.size(), 2);
        assertEquals(introspector.stripeInformations.get(0).getNumberOfRows(), 10);
        assertEquals(introspector.stripeInformations.get(1).getNumberOfRows(), 5);

        assertEquals(introspector.rowGroupIndexesByStripeOffset.size(), 2);

        // check we got the file column statistics
        assertEquals(introspector.fileFooter.getFileStats().size(), 2);

        // check we got the row group column statistics
        Map<StreamId, List<RowGroupIndex>> stripeRowGroupIndexes1 = introspector.rowGroupIndexesByStripeOffset.get(introspector.stripeInformations.get(0).getOffset());
        Map<StreamId, List<RowGroupIndex>> stripeRowGroupIndexes2 = introspector.rowGroupIndexesByStripeOffset.get(introspector.stripeInformations.get(1).getOffset());
        List<RowGroupIndex> rowGroupIndexes1 = stripeRowGroupIndexes1.get(new StreamId(1, 0, ROW_INDEX));
        List<RowGroupIndex> rowGroupIndexes2 = stripeRowGroupIndexes2.get(new StreamId(1, 0, ROW_INDEX));
        assertEquals(rowGroupIndexes1.size(), 2);
        assertEquals(rowGroupIndexes2.size(), 1);

        assertNotNull(rowGroupIndexes1.get(0).getColumnStatistics());
        assertNotNull(rowGroupIndexes1.get(1).getColumnStatistics());
        assertNotNull(rowGroupIndexes2.get(0).getColumnStatistics());
    }

    private void writeFile(Type type, Page page, TempFile tempFile)
            throws IOException
    {
        DefaultOrcWriterFlushPolicy flushPolicy = DefaultOrcWriterFlushPolicy.builder()
                .withStripeMaxRowCount(10)
                .build();

        OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                .withFlushPolicy(flushPolicy)
                .withRowGroupMaxRowCount(5)
                .build();

        try (OrcWriter orcWriter = createOrcWriter(
                tempFile.getFile(),
                OrcEncoding.DWRF,
                CompressionKind.ZSTD,
                Optional.empty(),
                ImmutableList.of(type),
                writerOptions,
                NOOP_WRITER_STATS)) {
            // write a block with 2 keys
            orcWriter.write(page);
        }
    }

    private static void readFile(Type type, CapturingOrcFileIntrospector introspector, TempFile tempFile)
            throws IOException
    {
        OrcDataSource dataSource = new FileOrcDataSource(tempFile.getFile(),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                true);

        OrcReaderOptions readerOptions = OrcReaderOptions.builder()
                .withMaxMergeDistance(new DataSize(1, MEGABYTE))
                .withTinyStripeThreshold(new DataSize(1, MEGABYTE))
                .withMaxBlockSize(new DataSize(1, MEGABYTE))
                .build();

        OrcReader reader = new OrcReader(
                dataSource,
                OrcEncoding.DWRF,
                new StorageOrcFileTailSource(),
                StripeMetadataSourceFactory.of(new StorageStripeMetadataSource()),
                Optional.empty(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                readerOptions,
                false,
                DwrfEncryptionProvider.NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY,
                new RuntimeStats(),
                Optional.of(introspector));

        OrcSelectiveRecordReader recordReader = reader.createSelectiveRecordReader(
                ImmutableMap.of(0, type),
                ImmutableList.of(0),
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                OrcPredicate.TRUE,
                0,
                dataSource.getSize(),
                DateTimeZone.UTC,
                false,
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                Optional.empty(),
                1000);
        while (recordReader.getNextPage() != null) {
            // ignore
        }
        recordReader.close();
    }

    private static Page createTestPage(Type type, int positionCount)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, positionCount);
        for (int i = 0; i < positionCount; i++) {
            type.writeLong(blockBuilder, i);
        }
        return new Page(blockBuilder.build());
    }

    private static class CapturingOrcFileIntrospector
            implements OrcFileIntrospector
    {
        private final List<StripeInformation> stripeInformations = new ArrayList<>();
        private final List<Stripe> stripes = new ArrayList<>();
        private final Map<Long, Map<StreamId, List<RowGroupIndex>>> rowGroupIndexesByStripeOffset = new HashMap<>();
        private Footer fileFooter;
        private OrcFileTail fileTail;

        @Override
        public void onFileFooter(Footer fileFooter)
        {
            assertNull(this.fileFooter);
            this.fileFooter = fileFooter;
        }

        @Override
        public void onFileTail(OrcFileTail fileTail)
        {
            assertNull(this.fileTail);
            this.fileTail = fileTail;
        }

        @Override
        public void onStripe(StripeInformation stripeInformation, Stripe stripe)
        {
            this.stripeInformations.add(stripeInformation);
            this.stripes.add(stripe);
        }

        @Override
        public void onRowGroupIndexes(StripeInformation stripe, Map<StreamId, List<RowGroupIndex>> columnIndexes)
        {
            Long stripeOffset = stripe.getOffset();
            assertNull(rowGroupIndexesByStripeOffset.get(stripeOffset));
            rowGroupIndexesByStripeOffset.put(stripeOffset, columnIndexes);
        }
    }
}
