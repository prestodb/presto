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
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.presto.cache.CacheType.LOCAL_RANGE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestCacheConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CacheConfig.class)
                .setCachingEnabled(false)
                .setCacheType(null)
                .setBaseDirectory(null)
                .setValidationEnabled(false)
                .setMaxInMemoryCacheSize(new DataSize(2, GIGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cache.enabled", "true")
                .put("cache.type", "LOCAL_RANGE")
                .put("cache.base-directory", "tcp://abc")
                .put("cache.validation-enabled", "true")
                .put("cache.max-in-memory-cache-size", "42MB")
                .build();

        CacheConfig expected = new CacheConfig()
                .setCachingEnabled(true)
                .setCacheType(LOCAL_RANGE)
                .setBaseDirectory(new URI("tcp://abc"))
                .setValidationEnabled(true)
                .setMaxInMemoryCacheSize(new DataSize(42, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
