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
package io.prestosql.plugin.atop;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.TimeZone;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.prestosql.plugin.atop.AtopConnectorConfig.SECURITY_FILE;
import static io.prestosql.plugin.atop.AtopConnectorConfig.SECURITY_NONE;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestAtopConnectorConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AtopConnectorConfig.class)
                .setExecutablePath("atop")
                .setConcurrentReadersPerNode(1)
                .setSecurity(SECURITY_NONE)
                .setReadTimeout(new Duration(5, MINUTES))
                .setMaxHistoryDays(30)
                .setTimeZone(TimeZone.getDefault().getID()));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("atop.executable-path", "/test/atop")
                .put("atop.concurrent-readers-per-node", "10")
                .put("atop.executable-read-timeout", "1m")
                .put("atop.security", "file")
                .put("atop.max-history-days", "10")
                .put("atop.time-zone", "PST")
                .build();

        AtopConnectorConfig expected = new AtopConnectorConfig()
                .setExecutablePath("/test/atop")
                .setConcurrentReadersPerNode(10)
                .setSecurity(SECURITY_FILE)
                .setReadTimeout(new Duration(1, MINUTES))
                .setMaxHistoryDays(10)
                .setTimeZone("PST");

        assertFullMapping(properties, expected);
    }
}
