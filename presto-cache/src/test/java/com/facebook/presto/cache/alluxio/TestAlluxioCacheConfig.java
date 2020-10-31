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
package com.facebook.presto.cache.alluxio;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestAlluxioCacheConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AlluxioCacheConfig.class)
                .setAsyncWriteEnabled(false)
                .setConfigValidationEnabled(false)
                .setJmxClass("alluxio.metrics.sink.JmxSink")
                .setMaxCacheSize(new DataSize(2, GIGABYTE))
                .setMetricsCollectionEnabled(true)
                .setMetricsDomain("com.facebook.alluxio")
                .setTimeoutDuration(new Duration(60, SECONDS))
                .setTimeoutEnabled(false)
                .setTimeoutThreads(64));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cache.alluxio.async-write-enabled", "true")
                .put("cache.alluxio.config-validation-enabled", "true")
                .put("cache.alluxio.jmx-class", "test.TestJmxSink")
                .put("cache.alluxio.max-cache-size", "42MB")
                .put("cache.alluxio.metrics-domain", "test.alluxio")
                .put("cache.alluxio.metrics-enabled", "false")
                .put("cache.alluxio.timeout-duration", "120s")
                .put("cache.alluxio.timeout-enabled", "true")
                .put("cache.alluxio.timeout-threads", "512")
                .build();

        AlluxioCacheConfig expected = new AlluxioCacheConfig()
                .setAsyncWriteEnabled(true)
                .setMaxCacheSize(new DataSize(42, MEGABYTE))
                .setMetricsCollectionEnabled(false)
                .setMetricsDomain("test.alluxio")
                .setJmxClass("test.TestJmxSink")
                .setConfigValidationEnabled(true)
                .setTimeoutDuration(new Duration(120, SECONDS))
                .setTimeoutEnabled(true)
                .setTimeoutThreads(512);

        assertFullMapping(properties, expected);
    }
}
