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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestClickHouseConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ClickHouseConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setUserCredentialName(null)
                .setPasswordCredentialName(null)
                .setCaseInsensitiveNameMatching(false)
                .setAllowDropTable(false)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, MINUTES))
                .setMapStringAsVarchar(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("clickhouse.connection-url", "jdbc:h2:mem:config")
                .put("clickhouse.connection-user", "user")
                .put("clickhouse.connection-password", "password")
                .put("clickhouse.user-credential-name", "foo")
                .put("clickhouse.password-credential-name", "bar")
                .put("clickhouse.case-insensitive-name-matching", "true")
                .put("clickhouse.case-insensitive-name-matching.cache-ttl", "1s")
                .put("clickhouse.map-string-as-varchar", "true")
                .put("clickhouse.allow-drop-table", "true")
                .build();

        ClickHouseConfig expected = new ClickHouseConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setConnectionUser("user")
                .setConnectionPassword("password")
                .setUserCredentialName("foo")
                .setPasswordCredentialName("bar")
                .setCaseInsensitiveNameMatching(true)
                .setAllowDropTable(true)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, SECONDS))
                .setMapStringAsVarchar(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
