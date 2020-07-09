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

public class TestPrestoClusterConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(PrestoClusterConfig.class)
                .setHost(null)
                .setJdbcPort(0)
                .setHttpPort(null)
                .setJdbcUrlParameters(null)
                .setQueryTimeout(new Duration(60, MINUTES))
                .setMetadataTimeout(new Duration(3, MINUTES))
                .setChecksumTimeout(new Duration(30, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("host", "proxy.presto.fbinfra.net")
                .put("jdbc-port", "7778")
                .put("http-port", "7777")
                .put("jdbc-url-parameters", "{\"SSL\": false}")
                .put("query-timeout", "2h")
                .put("metadata-timeout", "1h")
                .put("checksum-timeout", "3h")
                .build();
        PrestoClusterConfig expected = new PrestoClusterConfig()
                .setHost("proxy.presto.fbinfra.net")
                .setJdbcPort(7778)
                .setHttpPort(7777)
                .setJdbcUrlParameters("{\"SSL\": false}")
                .setQueryTimeout(new Duration(2, HOURS))
                .setMetadataTimeout(new Duration(1, HOURS))
                .setChecksumTimeout(new Duration(3, HOURS));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testJdbcUrl()
    {
        PrestoClusterConfig config = new PrestoClusterConfig()
                .setHost("proxy.presto.fbinfra.net")
                .setJdbcPort(7778)
                .setJdbcUrlParameters("{\"SSL\": true, \"SSLTrustStorePath\": \"trust-store\", \"SSLKeyStorePath\": \"key-store\"}")
                .setQueryTimeout(new Duration(60, MINUTES));
        assertEquals(config.getJdbcUrl(), "jdbc:presto://proxy.presto.fbinfra.net:7778?SSL=true&SSLTrustStorePath=trust-store&SSLKeyStorePath=key-store");
    }

    @Test
    public void testHttpUri()
    {
        PrestoClusterConfig config = new PrestoClusterConfig()
                .setHost("proxy.presto.fbinfra.net")
                .setJdbcPort(7778)
                .setHttpPort(7777)
                .setQueryTimeout(new Duration(60, MINUTES));
        assertEquals(config.getHttpUri("/v1/node"), URI.create("http://proxy.presto.fbinfra.net:7777/v1/node"));
    }
}
