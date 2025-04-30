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
import com.google.common.io.Resources;

public class TestOAuth2AuthenticationFilterWithJwt
        extends BaseOAuth2AuthenticationFilterTest
{
    @Override
    protected ImmutableMap<String, String> getOAuth2Config(String idpUrl)
    {
        return ImmutableMap.<String, String>builder()
                .put("http-server.authentication.allow-forwarded-https", "true")
                .put("http-server.authentication.type", "OAUTH2")
                .put("http-server.authentication.oauth2.issuer", "https://localhost:4444/")
                .put("http-server.authentication.oauth2.auth-url", idpUrl + "/oauth2/auth")
                .put("http-server.authentication.oauth2.token-url", idpUrl + "/oauth2/token")
                .put("http-server.authentication.oauth2.jwks-url", idpUrl + "/.well-known/jwks.json")
                .put("http-server.authentication.oauth2.client-id", PRESTO_CLIENT_ID)
                .put("http-server.authentication.oauth2.client-secret", PRESTO_CLIENT_SECRET)
                .put("http-server.authentication.oauth2.additional-audiences", TRUSTED_CLIENT_ID)
                .put("http-server.authentication.oauth2.max-clock-skew", "0s")
                .put("http-server.authentication.oauth2.user-mapping.pattern", "(.*)(@.*)?")
                .put("http-server.authentication.oauth2.oidc.discovery", "false")
                .put("oauth2-jwk.http-client.trust-store-path", Resources.getResource("cert/localhost.pem").getPath())
                .build();
    }

    @Override
    protected TestingHydraIdentityProvider getHydraIdp()
            throws Exception
    {
        TestingHydraIdentityProvider hydraIdP = new TestingHydraIdentityProvider(TTL_ACCESS_TOKEN_IN_SECONDS, true, false);
        hydraIdP.start();

        return hydraIdP;
    }
}
