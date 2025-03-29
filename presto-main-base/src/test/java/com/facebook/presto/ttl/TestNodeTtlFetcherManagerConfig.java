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
package com.facebook.presto.ttl;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManagerConfig;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManagerConfig.NodeTtlFetcherManagerType.CONFIDENCE;
import static com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManagerConfig.NodeTtlFetcherManagerType.THROWING;

public class TestNodeTtlFetcherManagerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(NodeTtlFetcherManagerConfig.class)
                .setInitialDelayBeforeRefresh(new Duration(1, TimeUnit.MINUTES))
                .setStaleTtlThreshold(new Duration(15, TimeUnit.MINUTES))
                .setNodeTtlFetcherManagerType(THROWING));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("node-ttl-fetcher-manager.initial-delay-before-refresh", "5m")
                .put("node-ttl-fetcher-manager.stale-ttl-threshold", "10m")
                .put("node-ttl-fetcher-manager.type", "CONFIDENCE")
                .build();

        NodeTtlFetcherManagerConfig expected = new NodeTtlFetcherManagerConfig()
                .setInitialDelayBeforeRefresh(new Duration(5, TimeUnit.MINUTES))
                .setStaleTtlThreshold(new Duration(10, TimeUnit.MINUTES))
                .setNodeTtlFetcherManagerType(CONFIDENCE);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
