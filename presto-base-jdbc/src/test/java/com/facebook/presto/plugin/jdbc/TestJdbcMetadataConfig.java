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
package com.facebook.presto.plugin.jdbc;

import com.facebook.airlift.units.Duration;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestJdbcMetadataConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(JdbcMetadataConfig.class)
                .setAllowDropTable(false)
                .setMetadataTransactionCacheEnabled(true)
                .setMetadataCacheTtl(new Duration(0, SECONDS))
                .setMetadataCacheRefreshInterval(new Duration(0, SECONDS))
                .setMetadataCacheMaximumSize(10000)
                .setMetadataTransactionCacheMaximumSize(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("allow-drop-table", "true")
                .put("metadata-transaction-cache-enabled", "false")
                .put("metadata-cache-ttl", "1h")
                .put("metadata-cache-refresh-interval", "10s")
                .put("metadata-cache-maximum-size", "100")
                .put("metadata-transaction-cache-maximum-size", "200")
                .build();

        JdbcMetadataConfig expected = new JdbcMetadataConfig()
                .setAllowDropTable(true)
                .setMetadataTransactionCacheEnabled(false)
                .setMetadataCacheTtl(new Duration(1, HOURS))
                .setMetadataCacheRefreshInterval(new Duration(10, SECONDS))
                .setMetadataCacheMaximumSize(100)
                .setMetadataTransactionCacheMaximumSize(200);

        assertFullMapping(properties, expected);
    }
}
