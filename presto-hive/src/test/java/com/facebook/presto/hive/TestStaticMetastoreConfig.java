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
package com.facebook.presto.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.testng.Assert.assertEquals;

public class TestStaticMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StaticMetastoreConfig.class)
                .setMetastoreUris(null)
                .setEnableTransportPool(false)
                .setMaxTransport(128)
                .setTransportIdleTimeout(300_000L)
                .setTransportEvictInterval(10_000L)
                .setTransportEvictNumTests(3));
    }

    @Test
    public void testExplicitPropertyMappingsSingleMetastore()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.uri", "thrift://localhost:9083")
                .put("hive.metastore.enable-transport-pool", "true")
                .put("hive.metastore.max-transport-num-per-endpoint", "64")
                .put("hive.metastore.transport-idle-timeout", "10000")
                .put("hive.metastore.transport-eviction-interval", "1000")
                .put("hive.metastore.transport-eviction-num-tests", "10")
                .build();

        StaticMetastoreConfig expected = new StaticMetastoreConfig()
                .setMetastoreUris("thrift://localhost:9083")
                .setEnableTransportPool(true)
                .setMaxTransport(64)
                .setTransportIdleTimeout(10_000L)
                .setTransportEvictInterval(1_000L)
                .setTransportEvictNumTests(10);

        assertFullMapping(properties, expected);
        assertEquals(expected.getMetastoreUris(), ImmutableList.of(URI.create("thrift://localhost:9083")));
    }

    @Test
    public void testExplicitPropertyMappingsMultipleMetastores()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.uri", "thrift://localhost:9083,thrift://192.0.2.3:8932")
                .put("hive.metastore.enable-transport-pool", "true")
                .put("hive.metastore.max-transport-num-per-endpoint", "64")
                .put("hive.metastore.transport-idle-timeout", "10000")
                .put("hive.metastore.transport-eviction-interval", "1000")
                .put("hive.metastore.transport-eviction-num-tests", "10")
                .build();

        StaticMetastoreConfig expected = new StaticMetastoreConfig()
                .setMetastoreUris("thrift://localhost:9083,thrift://192.0.2.3:8932")
                .setEnableTransportPool(true)
                .setMaxTransport(64)
                .setTransportIdleTimeout(10_000L)
                .setTransportEvictInterval(1_000L)
                .setTransportEvictNumTests(10);

        assertFullMapping(properties, expected);
        assertEquals(expected.getMetastoreUris(), ImmutableList.of(
                URI.create("thrift://localhost:9083"),
                URI.create("thrift://192.0.2.3:8932")));
    }
}
