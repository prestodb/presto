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

import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.writer.CompressionBufferPool;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.OptionalInt;
import java.util.Set;

import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestColumnWriterOptions
{
    @Test
    public void testProperties()
    {
        CompressionKind compressionKind = CompressionKind.ZSTD;
        OptionalInt compressionLevel = OptionalInt.of(3);
        DataSize compressionMaxBufferSize = DataSize.valueOf("56kB");
        DataSize stringStatisticsLimit = DataSize.valueOf("17MB");
        boolean integerDictionaryEncodingEnabled = true;
        boolean stringDictionarySortingEnabled = true;
        boolean stringDictionaryEncodingEnabled = true;
        boolean ignoreDictionaryRowGroupSizes = true;
        int preserveDirectEncodingStripeCount = 27;
        CompressionBufferPool compressionBufferPool = new CompressionBufferPool.LastUsedCompressionBufferPool();
        Set<Integer> flattenedNodes = ImmutableSet.of(1, 5);

        ColumnWriterOptions options = ColumnWriterOptions.builder()
                .setCompressionKind(compressionKind)
                .setCompressionLevel(compressionLevel)
                .setCompressionMaxBufferSize(compressionMaxBufferSize)
                .setStringStatisticsLimit(stringStatisticsLimit)
                .setIntegerDictionaryEncodingEnabled(integerDictionaryEncodingEnabled)
                .setStringDictionarySortingEnabled(stringDictionarySortingEnabled)
                .setStringDictionaryEncodingEnabled(stringDictionaryEncodingEnabled)
                .setIgnoreDictionaryRowGroupSizes(ignoreDictionaryRowGroupSizes)
                .setPreserveDirectEncodingStripeCount(preserveDirectEncodingStripeCount)
                .setCompressionBufferPool(compressionBufferPool)
                .setFlattenedNodes(flattenedNodes)
                .build();

        boolean checkDisabledDictionaryEncoding = false;

        for (ColumnWriterOptions actual : ImmutableList.of(options, options.copyWithDisabledDictionaryEncoding())) {
            assertEquals(actual.getCompressionKind(), compressionKind);
            assertEquals(actual.getCompressionLevel(), compressionLevel);
            assertEquals(actual.getCompressionMaxBufferSize(), toIntExact(compressionMaxBufferSize.toBytes()));
            assertEquals(actual.getStringStatisticsLimit(), toIntExact(stringStatisticsLimit.toBytes()));
            assertEquals(actual.isStringDictionarySortingEnabled(), stringDictionarySortingEnabled);
            assertEquals(actual.isIgnoreDictionaryRowGroupSizes(), ignoreDictionaryRowGroupSizes);
            assertEquals(actual.getPreserveDirectEncodingStripeCount(), preserveDirectEncodingStripeCount);
            assertEquals(actual.getCompressionBufferPool(), compressionBufferPool);
            assertEquals(actual.getFlattenedNodes(), flattenedNodes);

            if (checkDisabledDictionaryEncoding) {
                assertFalse(actual.isStringDictionaryEncodingEnabled());
                assertFalse(actual.isIntegerDictionaryEncodingEnabled());
            }
            else {
                assertEquals(actual.isStringDictionaryEncodingEnabled(), stringDictionaryEncodingEnabled);
                assertEquals(actual.isIntegerDictionaryEncodingEnabled(), integerDictionaryEncodingEnabled);
            }
            checkDisabledDictionaryEncoding = true;
        }
    }
}
