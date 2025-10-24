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
import com.facebook.airlift.units.Duration;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestClickHouseConfig
{
    private static final String connectionUrl = "clickhouse.connection-url";
    private static final String connectionUser = "clickhouse.connection-user";
    private static final String connectionPassword = "clickhouse.connection-password";
    private static final String userCredential = "clickhouse.user-credential";
    private static final String passwordCredential = "clickhouse.password-credential";
    private static final String caseInsensitive = "clickhouse.case-insensitive";
    private static final String remoteNameCacheTtl = "clickhouse.remote-name-cache-ttl";
    private static final String mapStringAsVarchar = "clickhouse.map-string-as-varchar";
    private static final String allowDropTable = "clickhouse.allow-drop-table";
    private static final String commitBatchSize = "clickhouse.commitBatchSize";
    private static final String caseSensitiveNameMatching = "case-sensitive-name-matching";

    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ClickHouseConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setUserCredential(null)
                .setPasswordCredential(null)
                .setCaseInsensitiveNameMatching(false)
                .setAllowDropTable(false)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, MINUTES))
                .setMapStringAsVarchar(false)
                .setCommitBatchSize(0)
                .setCaseSensitiveNameMatching(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put(connectionUrl, "jdbc:h2:mem:config")
                .put(connectionUser, "user")
                .put(connectionPassword, "password")
                .put(userCredential, "foo")
                .put(passwordCredential, "bar")
                .put(caseInsensitive, "true")
                .put(remoteNameCacheTtl, "1s")
                .put(mapStringAsVarchar, "true")
                .put(allowDropTable, "true")
                .put(commitBatchSize, "1000")
                .put(caseSensitiveNameMatching, "true")
                .build();

        ClickHouseConfig expected = new ClickHouseConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setConnectionUser("user")
                .setConnectionPassword("password")
                .setUserCredential("foo")
                .setPasswordCredential("bar")
                .setCaseInsensitiveNameMatching(true)
                .setAllowDropTable(true)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, SECONDS))
                .setMapStringAsVarchar(true)
                .setCaseSensitiveNameMatching(true)
                .setCommitBatchSize(1000);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
