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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.DwrfStripeCacheMode;
import com.facebook.presto.orc.proto.DwrfProto;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.FOOTER;
import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.INDEX;
import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.INDEX_AND_FOOTER;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestOrcRecordReaderDwrfStripeCaching
        extends AbstractTestDwrfStripeCaching
{
    private static final int READ_TAIL_SIZE_IN_BYTES = 256;

    @Test(dataProvider = "Stripe cache for ALL stripes with mode BOTH")
    public void testBothAllStripes(File orcFile)
            throws IOException
    {
        DwrfProto.Footer footer = readFileFooter(orcFile);
        List<DwrfProto.StripeInformation> stripes = footer.getStripesList();
        List<DiskRange> forbiddenRanges = getStripeRanges(INDEX_AND_FOOTER, stripes);
        assertFileContentCachingEnabled(orcFile, forbiddenRanges);
        assertFileContentCachingDisabled(orcFile);
    }

    @Test(dataProvider = "Stripe cache for HALF stripes with mode BOTH")
    public void testBothHalfStripes(File orcFile)
            throws IOException
    {
        DwrfProto.Footer footer = readFileFooter(orcFile);
        List<DwrfProto.StripeInformation> stripes = footer.getStripesList().subList(0, 1);
        List<DiskRange> forbiddenRanges = getStripeRanges(INDEX_AND_FOOTER, stripes);
        assertFileContentCachingEnabled(orcFile, forbiddenRanges);
        assertFileContentCachingDisabled(orcFile);
    }

    @Test(dataProvider = "Stripe cache for ALL stripes with mode INDEX")
    public void testIndexAllStripes(File orcFile)
            throws IOException
    {
        DwrfProto.Footer footer = readFileFooter(orcFile);
        List<DwrfProto.StripeInformation> stripes = footer.getStripesList();
        List<DiskRange> forbiddenRanges = getStripeRanges(INDEX, stripes);
        assertFileContentCachingEnabled(orcFile, forbiddenRanges);
        assertFileContentCachingDisabled(orcFile);
    }

    @Test(dataProvider = "Stripe cache for HALF stripes with mode INDEX")
    public void testIndexHalfStripes(File orcFile)
            throws IOException
    {
        DwrfProto.Footer footer = readFileFooter(orcFile);
        List<DwrfProto.StripeInformation> stripes = footer.getStripesList().subList(0, 1);
        List<DiskRange> forbiddenRanges = getStripeRanges(INDEX, stripes);
        assertFileContentCachingEnabled(orcFile, forbiddenRanges);
        assertFileContentCachingDisabled(orcFile);
    }

    @Test(dataProvider = "Stripe cache for ALL stripes with mode FOOTER")
    public void testFooterAllStripes(File orcFile)
            throws IOException
    {
        DwrfProto.Footer footer = readFileFooter(orcFile);
        List<DwrfProto.StripeInformation> stripes = footer.getStripesList();
        List<DiskRange> forbiddenRanges = getStripeRanges(FOOTER, stripes);
        assertFileContentCachingEnabled(orcFile, forbiddenRanges);
        assertFileContentCachingDisabled(orcFile);
    }

    @Test(dataProvider = "Stripe cache for HALF stripes with mode FOOTER")
    public void testFooterHalfStripes(File orcFile)
            throws IOException
    {
        DwrfProto.Footer footer = readFileFooter(orcFile);
        List<DwrfProto.StripeInformation> stripes = footer.getStripesList().subList(0, 1);
        List<DiskRange> forbiddenRanges = getStripeRanges(FOOTER, stripes);
        assertFileContentCachingEnabled(orcFile, forbiddenRanges);
        assertFileContentCachingDisabled(orcFile);
    }

    @Test(dataProvider = "Stripe cache with mode NONE")
    public void testNoneAllStripes(File orcFile)
            throws IOException
    {
        List<DiskRange> forbiddenRanges = ImmutableList.of();
        assertFileContentCachingEnabled(orcFile, forbiddenRanges);
        assertFileContentCachingDisabled(orcFile);
    }

    @Test(dataProvider = "Stripe cache disabled")
    public void testStripeCacheDisabled(File orcFile)
            throws IOException
    {
        List<DiskRange> forbiddenRanges = ImmutableList.of();
        assertFileContentCachingEnabled(orcFile, forbiddenRanges);
        assertFileContentCachingDisabled(orcFile);
    }

    private void assertFileContentCachingEnabled(File orcFile, List<DiskRange> forbiddenRanges)
            throws IOException
    {
        try (TestingOrcDataSource orcDataSource = new TestingOrcDataSource(createFileOrcDataSource(orcFile))) {
            StripeMetadataSourceFactory delegateSourceFactory = StripeMetadataSourceFactory.of(new StorageStripeMetadataSource());
            DwrfAwareStripeMetadataSourceFactory dwrfAwareFactory = new DwrfAwareStripeMetadataSourceFactory(delegateSourceFactory);

            // set zeroes to avoid file caching and merging of small disk ranges
            OrcReaderOptions orcReaderOptions = new OrcReaderOptions(
                    new DataSize(0, MEGABYTE),
                    new DataSize(0, MEGABYTE),
                    new DataSize(1, MEGABYTE),
                    false);

            OrcReader orcReader = new OrcReader(
                    orcDataSource,
                    DWRF,
                    new StorageOrcFileTailSource(READ_TAIL_SIZE_IN_BYTES, true),
                    dwrfAwareFactory,
                    NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                    orcReaderOptions,
                    false,
                    NO_ENCRYPTION,
                    DwrfKeyProvider.EMPTY,
                    new RuntimeStats());

            assertRecordValues(orcDataSource, orcReader);

            // check that the reader used the cache to read stripe indexes and footers
            assertForbiddenRanges(orcDataSource, forbiddenRanges);
        }
    }

    private void assertFileContentCachingDisabled(File orcFile)
            throws IOException
    {
        try (TestingOrcDataSource orcDataSource = new TestingOrcDataSource(createFileOrcDataSource(orcFile))) {
            StripeMetadataSourceFactory delegateSourceFactory = StripeMetadataSourceFactory.of(new StorageStripeMetadataSource());
            DwrfAwareStripeMetadataSourceFactory dwrfAwareFactory = new DwrfAwareStripeMetadataSourceFactory(delegateSourceFactory);
            OrcReader orcReader = new OrcReader(
                    orcDataSource,
                    DWRF,
                    new StorageOrcFileTailSource(READ_TAIL_SIZE_IN_BYTES, false),
                    dwrfAwareFactory,
                    NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                    OrcReaderTestingUtils.createDefaultTestConfig(),
                    false,
                    NO_ENCRYPTION,
                    DwrfKeyProvider.EMPTY,
                    new RuntimeStats());

            assertRecordValues(orcDataSource, orcReader);
        }
    }

    // all files have 400 rows (4 stripes with 100 rows each) with the following values:
    // column 0 (int) = i
    // column 1 (int) = Integer.MAX_VALUE
    // column 2 (int) = i * 10
    private void assertRecordValues(TestingOrcDataSource orcDataSource, OrcReader orcReader)
            throws IOException
    {
        OrcSelectiveRecordReader recordReader = orcReader.createSelectiveRecordReader(
                ImmutableMap.of(0, INTEGER, 1, INTEGER, 2, INTEGER),
                ImmutableList.of(0, 1, 2),
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                OrcPredicate.TRUE,
                0,
                orcDataSource.getSize(),
                HIVE_STORAGE_TIME_ZONE,
                false,
                new TestingHiveOrcAggregatedMemoryContext(),
                Optional.empty(),
                MAX_BATCH_SIZE);
        Page page;
        int cnt = 0;
        while ((page = recordReader.getNextPage()) != null) {
            Block block0 = page.getBlock(0);
            Block block1 = page.getBlock(1);
            Block block2 = page.getBlock(2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertEquals(block0.getInt(i), cnt);
                assertEquals(block1.getInt(i), Integer.MAX_VALUE);
                assertEquals(block2.getInt(i), cnt * 10);
                cnt++;
            }
        }

        assertEquals(cnt, 400);
    }

    private void assertForbiddenRanges(TestingOrcDataSource orcDataSource, List<DiskRange> forbiddenRanges)
    {
        List<DiskRange> readRanges = orcDataSource.getReadRanges();
        for (DiskRange readRange : readRanges) {
            for (DiskRange forbiddenRange : forbiddenRanges) {
                if (intersect(readRange, forbiddenRange)) {
                    fail("Read range " + readRange + " is not supposed to intersect with " + forbiddenRange);
                }
            }
        }
    }

    // get disk ranges for stripe indexes and footers depending on the mode
    private List<DiskRange> getStripeRanges(DwrfStripeCacheMode mode, List<DwrfProto.StripeInformation> stripes)
    {
        ImmutableList.Builder<DiskRange> rangesBuilder = ImmutableList.builder();
        for (DwrfProto.StripeInformation stripe : stripes) {
            if (mode.hasFooter()) {
                long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
                rangesBuilder.add(new DiskRange(offset, toIntExact(stripe.getFooterLength())));
            }

            if (mode.hasIndex()) {
                rangesBuilder.add(new DiskRange(stripe.getOffset(), toIntExact(stripe.getIndexLength())));
            }
        }
        return rangesBuilder.build();
    }

    private boolean intersect(DiskRange r1, DiskRange r2)
    {
        DiskRange left = r1;
        DiskRange right = r2;
        if (r2.getOffset() < r1.getOffset()) {
            left = r2;
            right = r1;
        }

        return right.getOffset() == left.getOffset() || right.getOffset() < left.getEnd();
    }

    private OrcDataSource createFileOrcDataSource(File orcFile)
            throws IOException
    {
        return new FileOrcDataSource(
                orcFile,
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                true);
    }
}
