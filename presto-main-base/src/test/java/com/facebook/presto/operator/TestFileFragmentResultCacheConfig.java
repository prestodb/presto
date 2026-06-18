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
package com.facebook.presto.operator;

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.CompressionCodec;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.airlift.units.DataSize.Unit.GIGABYTE;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;

public class TestFileFragmentResultCacheConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FileFragmentResultCacheConfig.class)
                .setCachingEnabled(false)
                .setBaseDirectory(null)
                .setBlockEncodingCompressionCodec(CompressionCodec.NONE)
                .setMaxCachedEntries(10_000)
                .setCacheTtl(new Duration(2, DAYS))
                .setMaxInFlightSize(new DataSize(1, GIGABYTE))
                .setMaxSinglePagesSize(new DataSize(500, MEGABYTE))
                .setMaxCacheSize(new DataSize(100, GIGABYTE))
                .setInputDataStatsEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("fragment-result-cache.enabled", "true")
                .put("fragment-result-cache.base-directory", "tcp://abc")
                .put("fragment-result-cache.block-encoding-compression-codec", "LZ4")
                .put("fragment-result-cache.max-cached-entries", "100000")
                .put("fragment-result-cache.cache-ttl", "1d")
                .put("fragment-result-cache.max-in-flight-size", "2GB")
                .put("fragment-result-cache.max-single-pages-size", "200MB")
                .put("fragment-result-cache.max-cache-size", "200GB")
                .put("fragment-result-cache.input-data-stats-enabled", "true")
                .build();

        FileFragmentResultCacheConfig expected = new FileFragmentResultCacheConfig()
                .setCachingEnabled(true)
                .setBaseDirectory(new URI("tcp://abc"))
                .setBlockEncodingCompressionCodec(CompressionCodec.LZ4)
                .setMaxCachedEntries(100000)
                .setCacheTtl(new Duration(1, DAYS))
                .setMaxInFlightSize(new DataSize(2, GIGABYTE))
                .setMaxSinglePagesSize(new DataSize(200, MEGABYTE))
                .setMaxCacheSize(new DataSize(200, GIGABYTE))
                .setInputDataStatsEnabled(true);

        assertFullMapping(properties, expected);
    }
}
