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
package com.facebook.presto.plugin.prometheus;

import com.facebook.airlift.units.Duration;
import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPrometheusConnectorConfig
{
    @Test
    public void testDefaults()
            throws URISyntaxException
    {
        assertRecordedDefaults(recordDefaults(PrometheusConnectorConfig.class)
                .setPrometheusURI(new URI("http://localhost:9090"))
                .setQueryChunkSizeDuration(Duration.valueOf("10m"))
                .setMaxQueryRangeDuration(Duration.valueOf("1h"))
                .setCacheDuration(Duration.valueOf("30s"))
                .setBearerTokenFile(null)
                .setBasicAuthUser(null)
                .setBasicAuthPassword(null)
                .setTlsEnabled(false)
                .setTruststorePassword(null)
                .setVerifyHostName(false)
                .setTrustStorePath(null)
                .setCaseSensitiveNameMatchingEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("prometheus.uri", "file://test.json")
                .put("prometheus.query-chunk-duration", "365d")
                .put("prometheus.max-query-duration", "1095d")
                .put("prometheus.cache-ttl", "60s")
                .put("prometheus.bearer-token-file", "/tmp/bearer_token.txt")
                .put("prometheus.auth.user", "admin")
                .put("prometheus.auth.password", "secret")
                .put("prometheus.tls.enabled", "true")
                .put("prometheus.tls.truststore-password", "password")
                .put("prometheus.tls.truststore-path", "/tmp/path/truststore")
                .put("verify-host-name", "true")
                .put("case-sensitive-name-matching", "true")
                .build();

        URI uri = URI.create("file://test.json");
        PrometheusConnectorConfig expected = new PrometheusConnectorConfig();
        expected.setPrometheusURI(uri);
        expected.setQueryChunkSizeDuration(Duration.valueOf("365d"));
        expected.setMaxQueryRangeDuration(Duration.valueOf("1095d"));
        expected.setCacheDuration(Duration.valueOf("60s"));
        expected.setBearerTokenFile(new File("/tmp/bearer_token.txt"));
        expected.setBasicAuthUser("admin");
        expected.setBasicAuthPassword("secret");
        expected.setTlsEnabled(true);
        expected.setTruststorePassword("password");
        expected.setTrustStorePath("/tmp/path/truststore");
        expected.setVerifyHostName(true);
        expected.setCaseSensitiveNameMatchingEnabled(true);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testFailOnDurationLessThanQueryChunkConfig()
            throws Exception
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(new URI("http://doesnotmatter.example.com:"));
        config.setQueryChunkSizeDuration(Duration.valueOf("21d"));
        config.setMaxQueryRangeDuration(Duration.valueOf("1d"));
        config.setCacheDuration(Duration.valueOf("30s"));
        assertThatThrownBy(config::checkConfig)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("prometheus.max-query-duration must be greater than prometheus.query-chunk-duration");
    }

    @Test
    public void testFailOnBearerAndBasicAuthTogether()
            throws Exception
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(new URI("http://doesnotmatter.example.com:"));
        config.setQueryChunkSizeDuration(Duration.valueOf("10m"));
        config.setMaxQueryRangeDuration(Duration.valueOf("1h"));
        config.setCacheDuration(Duration.valueOf("30s"));
        config.setBearerTokenFile(new File("/tmp/token.txt"));
        config.setBasicAuthUser("admin");
        config.setBasicAuthPassword("secret");
        assertThatThrownBy(config::checkConfig)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("prometheus.bearer-token-file and prometheus.auth.user/prometheus.auth.password cannot be used together");
    }

    @Test
    public void testFailOnBasicAuthUserWithoutPassword()
            throws Exception
    {
        PrometheusConnectorConfig config = basicAuthConfig();
        config.setBasicAuthUser("admin");
        // password intentionally omitted
        assertThatThrownBy(config::checkConfig)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Both prometheus.auth.user and prometheus.auth.password must be set for Basic Auth");
    }

    @Test
    public void testFailOnBasicAuthPasswordWithoutUser()
            throws Exception
    {
        PrometheusConnectorConfig config = basicAuthConfig();
        config.setBasicAuthPassword("secret");
        // user intentionally omitted
        assertThatThrownBy(config::checkConfig)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Both prometheus.auth.user and prometheus.auth.password must be set for Basic Auth");
    }

    @Test
    public void testFailOnColonInBasicAuthUser()
            throws Exception
    {
        PrometheusConnectorConfig config = basicAuthConfig();
        config.setBasicAuthUser("admin:extra");
        config.setBasicAuthPassword("secret");
        assertThatThrownBy(config::checkConfig)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Illegal character ':' found in prometheus.auth.user");
    }

    private static PrometheusConnectorConfig basicAuthConfig()
            throws Exception
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(new URI("http://doesnotmatter.example.com:"));
        config.setQueryChunkSizeDuration(Duration.valueOf("10m"));
        config.setMaxQueryRangeDuration(Duration.valueOf("1h"));
        config.setCacheDuration(Duration.valueOf("30s"));
        return config;
    }
}
