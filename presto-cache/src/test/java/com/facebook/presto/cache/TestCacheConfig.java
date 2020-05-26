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
import static com.facebook.presto.cache.CacheType.FILE_MERGE;
import static com.facebook.presto.hive.CacheQuotaScope.GLOBAL;
import static com.facebook.presto.hive.CacheQuotaScope.TABLE;

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
                .setCacheQuotaScope(GLOBAL)
                .setDefaultCacheQuota(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cache.enabled", "true")
                .put("cache.type", "FILE_MERGE")
                .put("cache.base-directory", "tcp://abc")
                .put("cache.validation-enabled", "true")
                .put("cache.cache-quota-scope", "TABLE")
                .put("cache.default-cache-quota", "1GB")
                .build();

        CacheConfig expected = new CacheConfig()
                .setCachingEnabled(true)
                .setCacheType(FILE_MERGE)
                .setBaseDirectory(new URI("tcp://abc"))
                .setValidationEnabled(true)
                .setCacheQuotaScope(TABLE)
                .setDefaultCacheQuota(DataSize.succinctDataSize(1, DataSize.Unit.GIGABYTE));

        assertFullMapping(properties, expected);
    }
}
