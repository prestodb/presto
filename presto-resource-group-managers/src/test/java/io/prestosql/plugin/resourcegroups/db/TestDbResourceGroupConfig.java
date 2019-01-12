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
package io.prestosql.plugin.resourcegroups.db;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestDbResourceGroupConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(DbResourceGroupConfig.class)
                .setConfigDbUrl(null)
                .setMaxRefreshInterval(new Duration(1, HOURS))
                .setExactMatchSelectorEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("resource-groups.config-db-url", "jdbc:mysql//localhost:3306/config?user=presto_admin")
                .put("resource-groups.max-refresh-interval", "1m")
                .put("resource-groups.exact-match-selector-enabled", "true")
                .build();
        DbResourceGroupConfig expected = new DbResourceGroupConfig()
                .setConfigDbUrl("jdbc:mysql//localhost:3306/config?user=presto_admin")
                .setMaxRefreshInterval(new Duration(1, MINUTES))
                .setExactMatchSelectorEnabled(true);

        assertFullMapping(properties, expected);
    }
}
