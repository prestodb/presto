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
package com.facebook.presto.router.scheduler;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestPlanCheckerProviderRouterPluginConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(PlanCheckerRouterPluginConfig.class)
                .setJavaRouterURI(null)
                .setNativeRouterURI(null)
                .setPlanCheckClustersURIs(null)
                .setClientRequestTimeout(new Duration(2, MINUTES))
                .setJavaClusterFallbackEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws URISyntaxException
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("router-java-url", "192.168.0.1")
                .put("router-native-url", "192.168.0.2")
                .put("plan-check-clusters-uris", "192.168.0.3, 192.168.0.4")
                .put("client-request-timeout", "5m")
                .put("enable-java-cluster-fallback", "true")
                .build();
        PlanCheckerRouterPluginConfig expected = new PlanCheckerRouterPluginConfig()
                .setJavaRouterURI(new URI("192.168.0.1"))
                .setNativeRouterURI(new URI("192.168.0.2"))
                .setPlanCheckClustersURIs("192.168.0.3, 192.168.0.4")
                .setClientRequestTimeout(new Duration(5, MINUTES))
                .setJavaClusterFallbackEnabled(true);
        assertFullMapping(properties, expected);
    }
}
