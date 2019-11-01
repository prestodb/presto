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
package com.facebook.presto.cache;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;

public class TestCacheConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CacheConfig.class)
                .setBaseDirectory(null)
                .setValidationEnabled(false)
                .setMaxInMemoryCacheSize(new DataSize(2, GIGABYTE))
                .setMaxCachedEntries(1_000)
                .setCacheTtl(new Duration(2, DAYS)));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cache.base-directory", "tcp://abc")
                .put("cache.validation-enabled", "true")
                .put("cache.max-in-memory-cache-size", "42MB")
                .put("cache.max-cached-entries", "10000")
                .put("cache.ttl", "20d")
                .build();

        CacheConfig expected = new CacheConfig()
                .setBaseDirectory(new URI("tcp://abc"))
                .setValidationEnabled(true)
                .setMaxInMemoryCacheSize(new DataSize(42, MEGABYTE))
                .setMaxCachedEntries(10_000)
                .setCacheTtl(new Duration(20, DAYS));

        assertFullMapping(properties, expected);
    }
}
