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
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.OptionalInt;

import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.FOOTER;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

public class TestOrcWriterOptions
{
    @Test
    public void testProperties()
    {
        DataSize stripeMinSize = new DataSize(13, MEGABYTE);
        DataSize stripeMaxSize = new DataSize(27, MEGABYTE);
        int stripeMaxRowCount = 1_100_000;
        int rowGroupMaxRowCount = 15_000;
        DataSize dictionaryMaxMemory = new DataSize(13_000, KILOBYTE);
        DataSize stringMaxStatisticsLimit = new DataSize(128, BYTE);
        DataSize maxCompressionBufferSize = new DataSize(512, KILOBYTE);
        OptionalInt compressionLevel = OptionalInt.of(5);
        StreamLayout streamLayout = new StreamLayout.ByColumnSize();
        boolean integerDictionaryEncodingEnabled = true;
        boolean stringDictionarySortingEnabled = false;
        boolean dwrfStripeCacheEnabled = true;
        DwrfStripeCacheMode dwrfStripeCacheMode = FOOTER;
        DataSize dwrfStripeCacheMaxSize = new DataSize(7, MEGABYTE);

        OrcWriterOptions.Builder builder = OrcWriterOptions.builder()
                .withStripeMinSize(stripeMinSize)
                .withStripeMaxSize(stripeMaxSize)
                .withStripeMaxRowCount(stripeMaxRowCount)
                .withRowGroupMaxRowCount(rowGroupMaxRowCount)
                .withDictionaryMaxMemory(dictionaryMaxMemory)
                .withMaxStringStatisticsLimit(stringMaxStatisticsLimit)
                .withMaxCompressionBufferSize(maxCompressionBufferSize)
                .withCompressionLevel(compressionLevel)
                .withStreamLayout(streamLayout)
                .withIntegerDictionaryEncodingEnabled(integerDictionaryEncodingEnabled)
                .withStringDictionarySortingEnabled(stringDictionarySortingEnabled)
                .withDwrfStripeCacheEnabled(dwrfStripeCacheEnabled)
                .withDwrfStripeCacheMode(dwrfStripeCacheMode)
                .withDwrfStripeCacheMaxSize(dwrfStripeCacheMaxSize);

        OrcWriterOptions options = builder.build();

        assertEquals(stripeMinSize, options.getStripeMinSize());
        assertEquals(stripeMaxSize, options.getStripeMaxSize());
        assertEquals(stripeMaxRowCount, options.getStripeMaxRowCount());
        assertEquals(rowGroupMaxRowCount, options.getRowGroupMaxRowCount());
        assertEquals(dictionaryMaxMemory, options.getDictionaryMaxMemory());
        assertEquals(stringMaxStatisticsLimit, options.getMaxStringStatisticsLimit());
        assertEquals(maxCompressionBufferSize, options.getMaxCompressionBufferSize());
        assertEquals(compressionLevel, options.getCompressionLevel());
        assertEquals(streamLayout, options.getStreamLayout());
        assertEquals(integerDictionaryEncodingEnabled, options.isIntegerDictionaryEncodingEnabled());
        assertEquals(stringDictionarySortingEnabled, options.isStringDictionarySortingEnabled());
        assertEquals(options.isDwrfStripeCacheEnabled(), dwrfStripeCacheEnabled);
        assertEquals(options.getDwrfStripeCacheMode(), dwrfStripeCacheMode);
        assertEquals(options.getDwrfStripeCacheMaxSize(), dwrfStripeCacheMaxSize);
    }

    @Test
    public void testToString()
    {
        DataSize stripeMinSize = new DataSize(13, MEGABYTE);
        DataSize stripeMaxSize = new DataSize(27, MEGABYTE);
        int stripeMaxRowCount = 1_100_000;
        int rowGroupMaxRowCount = 15_000;
        DataSize dictionaryMaxMemory = new DataSize(13_000, KILOBYTE);
        DataSize stringMaxStatisticsLimit = new DataSize(128, BYTE);
        DataSize maxCompressionBufferSize = new DataSize(512, KILOBYTE);
        OptionalInt compressionLevel = OptionalInt.of(5);
        StreamLayout streamLayout = new StreamLayout.ByColumnSize();
        boolean integerDictionaryEncodingEnabled = false;
        boolean stringDictionarySortingEnabled = true;
        boolean dwrfStripeCacheEnabled = false;
        DwrfStripeCacheMode dwrfStripeCacheMode = FOOTER;
        DataSize dwrfStripeCacheMaxSize = new DataSize(7, MEGABYTE);

        OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                .withStripeMinSize(stripeMinSize)
                .withStripeMaxSize(stripeMaxSize)
                .withStripeMaxRowCount(stripeMaxRowCount)
                .withRowGroupMaxRowCount(rowGroupMaxRowCount)
                .withDictionaryMaxMemory(dictionaryMaxMemory)
                .withMaxStringStatisticsLimit(stringMaxStatisticsLimit)
                .withMaxCompressionBufferSize(maxCompressionBufferSize)
                .withCompressionLevel(compressionLevel)
                .withStreamLayout(streamLayout)
                .withIntegerDictionaryEncodingEnabled(integerDictionaryEncodingEnabled)
                .withStringDictionarySortingEnabled(stringDictionarySortingEnabled)
                .withDwrfStripeCacheEnabled(dwrfStripeCacheEnabled)
                .withDwrfStripeCacheMode(dwrfStripeCacheMode)
                .withDwrfStripeCacheMaxSize(dwrfStripeCacheMaxSize)
                .build();

        String expectedString = "OrcWriterOptions{stripeMinSize=13MB, stripeMaxSize=27MB, stripeMaxRowCount=1100000, "
                + "rowGroupMaxRowCount=15000, dictionaryMaxMemory=13000kB, maxStringStatisticsLimit=128B, "
                + "maxCompressionBufferSize=512kB, compressionLevel=OptionalInt[5], streamLayout=ByColumnSize{}, "
                + "integerDictionaryEncodingEnabled=false, stringDictionarySortingEnabled=true, "
                + "dwrfStripeCacheEnabled=false, dwrfStripeCacheMode=FOOTER, dwrfStripeCacheMaxSize=7MB}";
        assertEquals(writerOptions.toString(), expectedString);
    }
}
