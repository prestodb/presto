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
import org.testng.annotations.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.security.NoSuchAlgorithmException;
import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.airlift.units.Duration.succinctDuration;
import static io.jsonwebtoken.io.Encoders.BASE64;
import static java.util.concurrent.TimeUnit.HOURS;

public class TestRefreshTokensConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RefreshTokensConfig.class)
                .setTokenExpiration(succinctDuration(1, HOURS))
                .setIssuer("Presto_coordinator")
                .setAudience("Presto_coordinator")
                .setSecretKey(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        String encodedBase64SecretKey = BASE64.encode(generateKey());

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("http-server.authentication.oauth2.refresh-tokens.issued-token.timeout", "24h")
                .put("http-server.authentication.oauth2.refresh-tokens.issued-token.issuer", "issuer")
                .put("http-server.authentication.oauth2.refresh-tokens.issued-token.audience", "audience")
                .put("http-server.authentication.oauth2.refresh-tokens.secret-key", encodedBase64SecretKey)
                .build();

        RefreshTokensConfig expected = new RefreshTokensConfig()
                .setTokenExpiration(succinctDuration(24, HOURS))
                .setIssuer("issuer")
                .setAudience("audience")
                .setSecretKey(encodedBase64SecretKey);

        assertFullMapping(properties, expected);
    }

    private byte[] generateKey()
            throws NoSuchAlgorithmException
    {
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        SecretKey key = generator.generateKey();
        return key.getEncoded();
    }
}
