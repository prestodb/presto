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

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.presto.orc.cache.OrcCacheConfig;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.orc.OrcDataSourceUtils.EXPECTED_FOOTER_SIZE_IN_BYTES;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestOrcCacheConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(OrcCacheConfig.class)
                .setFileTailCacheEnabled(false)
                .setFileTailCacheSize(new DataSize(0, BYTE))
                .setFileTailCacheTtlSinceLastAccess(new Duration(0, SECONDS))
                .setStripeMetadataCacheEnabled(false)
                .setStripeFooterCacheSize(new DataSize(0, BYTE))
                .setStripeFooterCacheTtlSinceLastAccess(new Duration(0, SECONDS))
                .setStripeStreamCacheSize(new DataSize(0, BYTE))
                .setStripeStreamCacheTtlSinceLastAccess(new Duration(0, SECONDS))
                .setDwrfStripeCacheEnabled(false)
                .setExpectedFileTailSize(new DataSize(EXPECTED_FOOTER_SIZE_IN_BYTES, BYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("orc.file-tail-cache-enabled", "true")
                .put("orc.file-tail-cache-size", "1GB")
                .put("orc.file-tail-cache-ttl-since-last-access", "10m")
                .put("orc.stripe-metadata-cache-enabled", "true")
                .put("orc.stripe-footer-cache-size", "2GB")
                .put("orc.stripe-footer-cache-ttl-since-last-access", "5m")
                .put("orc.stripe-stream-cache-size", "3GB")
                .put("orc.stripe-stream-cache-ttl-since-last-access", "10m")
                .put("orc.dwrf-stripe-cache-enabled", "true")
                .put("orc.expected-file-tail-size", "8MB")
                .build();

        OrcCacheConfig expected = new OrcCacheConfig()
                .setFileTailCacheEnabled(true)
                .setFileTailCacheSize(new DataSize(1, GIGABYTE))
                .setFileTailCacheTtlSinceLastAccess(new Duration(10, MINUTES))
                .setStripeMetadataCacheEnabled(true)
                .setStripeFooterCacheSize(new DataSize(2, GIGABYTE))
                .setStripeFooterCacheTtlSinceLastAccess(new Duration(5, MINUTES))
                .setStripeStreamCacheSize(new DataSize(3, GIGABYTE))
                .setStripeStreamCacheTtlSinceLastAccess(new Duration(10, MINUTES))
                .setDwrfStripeCacheEnabled(true)
                .setExpectedFileTailSize(new DataSize(8, MEGABYTE));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
