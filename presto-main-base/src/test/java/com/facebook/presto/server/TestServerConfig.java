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

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.airlift.units.Duration;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.presto.spi.NodePoolType.DEFAULT;
import static com.facebook.presto.spi.NodePoolType.LEAF;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestServerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(ServerConfig.class)
                .setCoordinator(true)
                .setPrestoVersion(null)
                .setDataSources(null)
                .setIncludeExceptionInResponse(true)
                .setGracePeriod(new Duration(2, MINUTES))
                .setEnhancedErrorReporting(true)
                .setQueryResultsCompressionEnabled(true)
                .setResourceManagerEnabled(false)
                .setResourceManager(false)
                .setCatalogServer(false)
                .setCatalogServerEnabled(false)
                .setCoordinatorSidecarEnabled(false)
                .setPoolType(DEFAULT)
                .setClusterStatsExpirationDuration(new Duration(0, MILLISECONDS))
                .setNestedDataSerializationEnabled(true)
                .setClusterResourceGroupStateInfoExpirationDuration(new Duration(0, MILLISECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("coordinator", "false")
                .put("presto.version", "test")
                .put("datasources", "jmx")
                .put("http.include-exception-in-response", "false")
                .put("shutdown.grace-period", "5m")
                .put("sql.parser.enhanced-error-reporting", "false")
                .put("query-results.compression-enabled", "false")
                .put("resource-manager-enabled", "true")
                .put("resource-manager", "true")
                .put("catalog-server-enabled", "true")
                .put("catalog-server", "true")
                .put("coordinator-sidecar-enabled", "true")
                .put("pool-type", "LEAF")
                .put("cluster-stats-expiration-duration", "10s")
                .put("nested-data-serialization-enabled", "false")
                .put("cluster-resource-group-state-info-expiration-duration", "10s")
                .build();

        ServerConfig expected = new ServerConfig()
                .setCoordinator(false)
                .setPrestoVersion("test")
                .setDataSources("jmx")
                .setIncludeExceptionInResponse(false)
                .setGracePeriod(new Duration(5, MINUTES))
                .setEnhancedErrorReporting(false)
                .setQueryResultsCompressionEnabled(false)
                .setResourceManagerEnabled(true)
                .setResourceManager(true)
                .setCatalogServer(true)
                .setCatalogServerEnabled(true)
                .setCoordinatorSidecarEnabled(true)
                .setPoolType(LEAF)
                .setClusterStatsExpirationDuration(new Duration(10, SECONDS))
                .setNestedDataSerializationEnabled(false)
                .setClusterResourceGroupStateInfoExpirationDuration(new Duration(10, SECONDS));

        assertFullMapping(properties, expected);
    }
}
