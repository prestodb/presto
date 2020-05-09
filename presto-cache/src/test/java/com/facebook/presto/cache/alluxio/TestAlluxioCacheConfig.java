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
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestAlluxioCacheConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AlluxioCacheConfig.class)
                .setAsyncWriteEnabled(false)
                .setMetricsCollectionEnabled(true)
                .setMetricsDomain("com.facebook.alluxio")
                .setJmxClass("alluxio.metrics.sink.JmxSink"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cache.alluxio.async-write.enabled", "true")
                .put("cache.alluxio.metrics.enabled", "false")
                .put("cache.alluxio.metrics.domain", "test.alluxio")
                .put("cache.alluxio.jmx.class", "test.TestJmxSink")
                .build();

        AlluxioCacheConfig expected = new AlluxioCacheConfig()
                .setAsyncWriteEnabled(true)
                .setMetricsCollectionEnabled(false)
                .setMetricsDomain("test.alluxio")
                .setJmxClass("test.TestJmxSink");

        assertFullMapping(properties, expected);
    }
}
