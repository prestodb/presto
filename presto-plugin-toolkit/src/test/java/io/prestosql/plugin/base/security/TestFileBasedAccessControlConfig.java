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
package io.prestosql.plugin.base.security;

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.prestosql.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.prestosql.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFileBasedAccessControlConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(FileBasedAccessControlConfig.class)
                .setConfigFile(null)
                .setRefreshPeriod(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put(SECURITY_CONFIG_FILE, "/test.json")
                .put(SECURITY_REFRESH_PERIOD, "1s")
                .build();

        FileBasedAccessControlConfig expected = new FileBasedAccessControlConfig()
                .setConfigFile("/test.json")
                .setRefreshPeriod(new Duration(1, TimeUnit.SECONDS));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertThatThrownBy(() -> newInstance(ImmutableMap.of(SECURITY_REFRESH_PERIOD, "1ms")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("security.config-file: may not be null ");

        assertThatThrownBy(() -> newInstance(ImmutableMap.of(
                SECURITY_CONFIG_FILE, "/test.json",
                SECURITY_REFRESH_PERIOD, "1us")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid configuration property security.refresh-period");

        newInstance(ImmutableMap.of(SECURITY_CONFIG_FILE, "/test.json"));
    }

    private static FileBasedAccessControlConfig newInstance(Map<String, String> properties)
    {
        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        return configurationFactory.build(FileBasedAccessControlConfig.class);
    }
}
