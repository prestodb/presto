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
package com.facebook.presto.server;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestCodeCacheGcConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CodeCacheGcConfig.class)
                .setCodeCacheCheckInterval(new Duration(20, TimeUnit.SECONDS))
                .setCodeCacheCollectionThreshold(40));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("code-cache-check-interval", "5m")
                .put("code-cache-collection-threshold", "50")
                .build();

        CodeCacheGcConfig expected = new CodeCacheGcConfig()
                .setCodeCacheCheckInterval(new Duration(5, TimeUnit.MINUTES))
                .setCodeCacheCollectionThreshold(50);

        assertFullMapping(properties, expected);
    }
}
