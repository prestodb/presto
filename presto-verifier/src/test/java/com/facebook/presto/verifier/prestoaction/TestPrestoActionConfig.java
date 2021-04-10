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
package com.facebook.presto.verifier.prestoaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestPrestoActionConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(PrestoActionConfig.class)
                .setHosts(null)
                .setJdbcPort(0)
                .setHttpPort(null)
                .setJdbcUrlParameters(null)
                .setApplicationName("verifier-test")
                .setRemoveMemoryRelatedSessionProperties(true)
                .setQueryTimeout(new Duration(60, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hosts", "proxy.presto.fbinfra.net")
                .put("jdbc-port", "7778")
                .put("http-port", "7777")
                .put("jdbc-url-parameters", "{\"SSL\": false}")
                .put("query-timeout", "2h")
                .put("application-name", "verifier")
                .put("remove-memory-related-session-properties", "false")
                .build();
        PrestoActionConfig expected = new PrestoActionConfig()
                .setHosts("proxy.presto.fbinfra.net")
                .setJdbcPort(7778)
                .setHttpPort(7777)
                .setJdbcUrlParameters("{\"SSL\": false}")
                .setQueryTimeout(new Duration(2, HOURS))
                .setApplicationName("verifier")
                .setRemoveMemoryRelatedSessionProperties(false);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testJdbcUrl()
    {
        PrestoActionConfig config = new PrestoActionConfig()
                .setHosts("proxy.presto.fbinfra.net,proxy2.presto.fbinfra.net")
                .setJdbcPort(7778)
                .setJdbcUrlParameters("{\"SSL\": true, \"SSLTrustStorePath\": \"trust-store\", \"SSLKeyStorePath\": \"key-store\"}")
                .setQueryTimeout(new Duration(60, MINUTES));
        assertEquals(
                config.getJdbcUrls(),
                ImmutableList.of(
                        "jdbc:presto://proxy.presto.fbinfra.net:7778?SSL=true&SSLTrustStorePath=trust-store&SSLKeyStorePath=key-store",
                        "jdbc:presto://proxy2.presto.fbinfra.net:7778?SSL=true&SSLTrustStorePath=trust-store&SSLKeyStorePath=key-store"));
    }

    @Test
    public void testHttpUri()
    {
        PrestoActionConfig config = new PrestoActionConfig()
                .setHosts("proxy.presto.fbinfra.net,proxy2.presto.fbinfra.net")
                .setJdbcPort(7778)
                .setHttpPort(7777)
                .setQueryTimeout(new Duration(60, MINUTES));
        assertEquals(
                config.getHttpUris("/v1/node"),
                ImmutableList.of(
                        URI.create("http://proxy.presto.fbinfra.net:7777/v1/node"),
                        URI.create("http://proxy2.presto.fbinfra.net:7777/v1/node")));
    }
}
