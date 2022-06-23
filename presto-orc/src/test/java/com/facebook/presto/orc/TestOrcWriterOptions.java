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

import com.facebook.presto.orc.metadata.DwrfStripeCacheMode;
import com.facebook.presto.orc.writer.StreamLayoutFactory;
import com.facebook.presto.orc.writer.StreamLayoutFactory.ColumnSizeLayoutFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.INDEX_AND_FOOTER;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;

public class TestOrcWriterOptions
{
    private static final DwrfStripeCacheMode DWRF_STRIPE_CACHE_MODE = INDEX_AND_FOOTER;
    private static final DataSize DWRF_STRIPE_CACHE_MAX_SIZE = new DataSize(27, MEGABYTE);

    @Test
    public void testDwrfWriterOptionsProperties()
    {
        for (boolean value : ImmutableList.of(true, false)) {
            OrcWriterOptions options = OrcWriterOptions.builder()
                    .withDwrfStripeCacheEnabled(value)
                    .withDwrfStripeCacheMode(DWRF_STRIPE_CACHE_MODE)
                    .withDwrfStripeCacheMaxSize(DWRF_STRIPE_CACHE_MAX_SIZE)
                    .build();

            assertEquals(options.getDwrfStripeCacheOptions().isPresent(), value);
            if (value) {
                DwrfStripeCacheOptions dwrfStripeCacheOptions = options.getDwrfStripeCacheOptions().get();
                assertEquals(dwrfStripeCacheOptions.getStripeCacheMode(), DWRF_STRIPE_CACHE_MODE);
                assertEquals(dwrfStripeCacheOptions.getStripeCacheMaxSize(), DWRF_STRIPE_CACHE_MAX_SIZE);
            }
            else {
                assertEquals(Optional.empty(), options.getDwrfStripeCacheOptions());
            }
        }
    }

    @Test
    public void tesDefaultValues()
    {
        OrcWriterOptions options = OrcWriterOptions.builder().build();

        assertEquals(ImmutableSet.of(), options.getFlattenedColumns());
    }

    @Test
    public void testProperties()
    {
        DataSize stripeMinSize = new DataSize(13, MEGABYTE);
        DataSize stripeMaxSize = new DataSize(27, MEGABYTE);
        int stripeMaxRowCount = 1_100_000;
        int rowGroupMaxRowCount = 15_000;
        DataSize dictionaryMaxMemory = new DataSize(13_000, KILOBYTE);
        DataSize dictionaryMemoryRange = new DataSize(1_000, KILOBYTE);
        int dictionaryUsefulCheckPerChunkFrequency = 9_999;
        DataSize dictionaryUsefulCheckColumnSize = new DataSize(1, MEGABYTE);
        DataSize stringMaxStatisticsLimit = new DataSize(128, BYTE);
        DataSize maxCompressionBufferSize = new DataSize(512, KILOBYTE);
        OptionalInt compressionLevel = OptionalInt.of(5);
        StreamLayoutFactory streamLayoutFactory = new StreamLayoutFactory.StreamSizeLayoutFactory();
        boolean integerDictionaryEncodingEnabled = true;
        boolean stringDictionarySortingEnabled = false;
        boolean stringDictionaryEncodingEnabled = false;
        int preserveDirectEncodingStripeCount = 10;

        OrcWriterOptions.Builder builder = OrcWriterOptions.builder()
                .withFlushPolicy(DefaultOrcWriterFlushPolicy.builder()
                        .withStripeMinSize(stripeMinSize)
                        .withStripeMaxSize(stripeMaxSize)
                        .withStripeMaxRowCount(stripeMaxRowCount)
                        .build())
                .withRowGroupMaxRowCount(rowGroupMaxRowCount)
                .withDictionaryMaxMemory(dictionaryMaxMemory)
                .withDictionaryMemoryAlmostFullRange(dictionaryMemoryRange)
                .withDictionaryUsefulCheckPerChunkFrequency(dictionaryUsefulCheckPerChunkFrequency)
                .withDictionaryUsefulCheckColumnSize(dictionaryUsefulCheckColumnSize)
                .withMaxStringStatisticsLimit(stringMaxStatisticsLimit)
                .withMaxCompressionBufferSize(maxCompressionBufferSize)
                .withCompressionLevel(compressionLevel)
                .withStreamLayoutFactory(streamLayoutFactory)
                .withIntegerDictionaryEncodingEnabled(integerDictionaryEncodingEnabled)
                .withStringDictionarySortingEnabled(stringDictionarySortingEnabled)
                .withStringDictionaryEncodingEnabled(stringDictionaryEncodingEnabled)
                .withPreserveDirectEncodingStripeCount(preserveDirectEncodingStripeCount)
                .withFlattenedColumns(ImmutableSet.of(4, 3));

        OrcWriterOptions options = builder.build();

        assertEquals(toIntExact(stripeMinSize.toBytes()), options.getFlushPolicy().getStripeMinBytes());
        assertEquals(toIntExact(stripeMaxSize.toBytes()), options.getFlushPolicy().getStripeMaxBytes());
        assertEquals(stripeMaxRowCount, options.getFlushPolicy().getStripeMaxRowCount());
        assertEquals(rowGroupMaxRowCount, options.getRowGroupMaxRowCount());
        assertEquals(dictionaryMaxMemory, options.getDictionaryMaxMemory());
        assertEquals(dictionaryMemoryRange, options.getDictionaryMemoryAlmostFullRange());
        assertEquals(dictionaryUsefulCheckPerChunkFrequency, options.getDictionaryUsefulCheckPerChunkFrequency());
        assertEquals(dictionaryUsefulCheckColumnSize, options.getDictionaryUsefulCheckColumnSize());
        assertEquals(stringMaxStatisticsLimit, options.getMaxStringStatisticsLimit());
        assertEquals(maxCompressionBufferSize, options.getMaxCompressionBufferSize());
        assertEquals(compressionLevel, options.getCompressionLevel());
        assertEquals(streamLayoutFactory, options.getStreamLayoutFactory());
        assertEquals(integerDictionaryEncodingEnabled, options.isIntegerDictionaryEncodingEnabled());
        assertEquals(stringDictionarySortingEnabled, options.isStringDictionarySortingEnabled());
        assertEquals(stringDictionaryEncodingEnabled, options.isStringDictionaryEncodingEnabled());
        assertEquals(Optional.empty(), options.getDwrfStripeCacheOptions());
        assertEquals(preserveDirectEncodingStripeCount, options.getPreserveDirectEncodingStripeCount());
        assertEquals(ImmutableSet.of(4, 3), options.getFlattenedColumns());
    }

    @Test
    public void testToString()
    {
        DataSize stripeMinSize = new DataSize(13, MEGABYTE);
        DataSize stripeMaxSize = new DataSize(27, MEGABYTE);
        int stripeMaxRowCount = 1_100_000;
        int rowGroupMaxRowCount = 15_000;
        DataSize dictionaryMaxMemory = new DataSize(13_000, KILOBYTE);
        DataSize dictionaryMemoryRange = new DataSize(1_000, KILOBYTE);
        int dictionaryUsefulCheckPerChunkFrequency = 9_999;
        DataSize dictionaryUsefulCheckColumnSize = new DataSize(1, MEGABYTE);
        DataSize stringMaxStatisticsLimit = new DataSize(128, BYTE);
        DataSize maxCompressionBufferSize = new DataSize(512, KILOBYTE);
        DataSize dwrfStripeCacheMaxSize = new DataSize(4, MEGABYTE);
        DwrfStripeCacheMode dwrfStripeCacheMode = DwrfStripeCacheMode.INDEX_AND_FOOTER;
        OptionalInt compressionLevel = OptionalInt.of(5);
        StreamLayoutFactory streamLayoutFactory = new ColumnSizeLayoutFactory();
        boolean integerDictionaryEncodingEnabled = false;
        boolean stringDictionarySortingEnabled = true;
        int preserveDirectEncodingStripeCount = 0;

        OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                .withFlushPolicy(DefaultOrcWriterFlushPolicy.builder()
                        .withStripeMinSize(stripeMinSize)
                        .withStripeMaxSize(stripeMaxSize)
                        .withStripeMaxRowCount(stripeMaxRowCount)
                        .build())
                .withRowGroupMaxRowCount(rowGroupMaxRowCount)
                .withDictionaryMaxMemory(dictionaryMaxMemory)
                .withDictionaryMemoryAlmostFullRange(dictionaryMemoryRange)
                .withDictionaryUsefulCheckPerChunkFrequency(dictionaryUsefulCheckPerChunkFrequency)
                .withDictionaryUsefulCheckColumnSize(dictionaryUsefulCheckColumnSize)
                .withMaxStringStatisticsLimit(stringMaxStatisticsLimit)
                .withMaxCompressionBufferSize(maxCompressionBufferSize)
                .withCompressionLevel(compressionLevel)
                .withStreamLayoutFactory(streamLayoutFactory)
                .withIntegerDictionaryEncodingEnabled(integerDictionaryEncodingEnabled)
                .withStringDictionarySortingEnabled(stringDictionarySortingEnabled)
                .withDwrfStripeCacheEnabled(true)
                .withDwrfStripeCacheMaxSize(dwrfStripeCacheMaxSize)
                .withDwrfStripeCacheMode(dwrfStripeCacheMode)
                .withPreserveDirectEncodingStripeCount(preserveDirectEncodingStripeCount)
                .withFlattenedColumns(ImmutableSet.of(4))
                .build();

        String expectedString = "OrcWriterOptions{flushPolicy=DefaultOrcWriterFlushPolicy{stripeMaxRowCount=1100000, " +
                "stripeMinBytes=13631488, stripeMaxBytes=28311552}, rowGroupMaxRowCount=15000, " +
                "dictionaryMaxMemory=13000kB, dictionaryMemoryAlmostFullRange=1000kB, dictionaryUsefulCheckPerChunkFrequency=9999, " +
                "dictionaryUsefulCheckColumnSize=1MB, maxStringStatisticsLimit=128B, maxCompressionBufferSize=512kB, " +
                "compressionLevel=OptionalInt[5], streamLayoutFactory=ColumnSizeLayoutFactory{}, integerDictionaryEncodingEnabled=false, " +
                "stringDictionarySortingEnabled=true, stringDictionaryEncodingEnabled=true, " +
                "dwrfWriterOptions=Optional[DwrfStripeCacheOptions{stripeCacheMode=INDEX_AND_FOOTER, stripeCacheMaxSize=4MB}], " +
                "ignoreDictionaryRowGroupSizes=false, preserveDirectEncodingStripeCount=0, flattenedColumns=[4]}";
        assertEquals(expectedString, writerOptions.toString());
    }
}
