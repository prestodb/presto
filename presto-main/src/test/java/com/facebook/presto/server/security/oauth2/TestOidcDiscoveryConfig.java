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
package com.facebook.presto.server.security.oauth2;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestOidcDiscoveryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OidcDiscoveryConfig.class)
                .setDiscoveryTimeout(new Duration(30, SECONDS))
                .setUserinfoEndpointEnabled(true)
                .setAccessTokenIssuer(null)
                .setAuthUrl(null)
                .setTokenUrl(null)
                .setJwksUrl(null)
                .setUserinfoUrl(null));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("http-server.authentication.oauth2.oidc.discovery.timeout", "1m")
                .put("http-server.authentication.oauth2.oidc.use-userinfo-endpoint", "false")
                .put("http-server.authentication.oauth2.access-token-issuer", "https://issuer.com/at")
                .put("http-server.authentication.oauth2.auth-url", "https://issuer.com/auth")
                .put("http-server.authentication.oauth2.token-url", "https://issuer.com/token")
                .put("http-server.authentication.oauth2.jwks-url", "https://issuer.com/jwks.json")
                .put("http-server.authentication.oauth2.userinfo-url", "https://issuer.com/user")
                .build();

        OidcDiscoveryConfig expected = new OidcDiscoveryConfig()
                .setDiscoveryTimeout(new Duration(1, MINUTES))
                .setUserinfoEndpointEnabled(false)
                .setAccessTokenIssuer("https://issuer.com/at")
                .setAuthUrl("https://issuer.com/auth")
                .setTokenUrl("https://issuer.com/token")
                .setJwksUrl("https://issuer.com/jwks.json")
                .setUserinfoUrl("https://issuer.com/user");

        assertFullMapping(properties, expected);
    }
}
