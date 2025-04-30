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

import com.facebook.airlift.log.Level;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Key;
import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

import static com.facebook.airlift.testing.Assertions.assertLessThan;
import static com.facebook.presto.client.OkHttpUtil.setupInsecureSsl;
import static com.facebook.presto.server.security.oauth2.OAuthWebUiCookie.OAUTH2_COOKIE;
import static com.facebook.presto.server.security.oauth2.TokenEndpointAuthMethod.CLIENT_SECRET_BASIC;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static io.airlift.units.Duration.nanosSince;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDualAuthenticationFilterWithOAuth
{
    protected static final Duration TTL_ACCESS_TOKEN_IN_SECONDS = Duration.ofSeconds(5);
    protected static final String PRESTO_CLIENT_ID = "presto-client";
    protected static final String PRESTO_CLIENT_SECRET = "presto-secret";
    private static final String PRESTO_AUDIENCE = PRESTO_CLIENT_ID;
    private static final String ADDITIONAL_AUDIENCE = "https://external-service.com";
    protected static final String TRUSTED_CLIENT_ID = "trusted-client";
    protected static final String TRUSTED_CLIENT_SECRET = "trusted-secret";
    private static final String UNTRUSTED_CLIENT_ID = "untrusted-client";
    private static final String UNTRUSTED_CLIENT_SECRET = "untrusted-secret";
    private static final String UNTRUSTED_CLIENT_AUDIENCE = "https://untrusted.com";

    private final Logging logging = Logging.initialize();
    protected final OkHttpClient httpClient;
    protected TestingHydraIdentityProvider hydraIdP;

    private TestingPrestoServer server;

    private SimpleProxyServer simpleProxy;
    private URI uiUri;

    private URI proxyURI;

    protected TestDualAuthenticationFilterWithOAuth()
    {
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        setupInsecureSsl(httpClientBuilder);
        httpClientBuilder.followRedirects(false);
        httpClient = httpClientBuilder.build();
    }

    static void waitForNodeRefresh(TestingPrestoServer server)
            throws InterruptedException
    {
        long start = System.nanoTime();
        while (server.refreshNodes().getActiveNodes().size() < 1) {
            assertLessThan(nanosSince(start), new io.airlift.units.Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }
    }

    protected ImmutableMap<String, String> getConfig(String idpUrl)
    {
        return ImmutableMap.<String, String>builder()
                .put("http-server.authentication.allow-forwarded-https", "true")
                .put("http-server.authentication.type", "OAUTH2,PASSWORD")
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

    @BeforeClass
    public void setup()
            throws Exception
    {
        logging.setLevel(OAuth2Service.class.getName(), Level.DEBUG);
        hydraIdP = new TestingHydraIdentityProvider(TTL_ACCESS_TOKEN_IN_SECONDS, true, false);
        hydraIdP.start();
        String idpUrl = "https://localhost:" + hydraIdP.getAuthPort();
        server = new TestingPrestoServer(getConfig(idpUrl));
        server.getInstance(Key.get(OAuth2Client.class)).load();
        waitForNodeRefresh(server);
        // Due to problems with the Presto OSS project related to the AuthenticationFilter we have to run Presto behind a Proxy and terminate SSL at the proxy.
        simpleProxy = new SimpleProxyServer(server.getHttpBaseUrl());
        MILLISECONDS.sleep(1000);
        proxyURI = simpleProxy.getHttpsBaseUrl();
        uiUri = proxyURI.resolve("/");

        hydraIdP.createClient(
                PRESTO_CLIENT_ID,
                PRESTO_CLIENT_SECRET,
                CLIENT_SECRET_BASIC,
                ImmutableList.of(PRESTO_AUDIENCE, ADDITIONAL_AUDIENCE),
                simpleProxy.getHttpsBaseUrl() + "/oauth2/callback");
    }

    @Test
    public void testExpiredOAuthToken()
            throws Exception
    {
        String token = hydraIdP.getToken(PRESTO_CLIENT_ID, PRESTO_CLIENT_SECRET, ImmutableList.of(PRESTO_AUDIENCE));
        assertUICallWithCookie(token);
        Thread.sleep(TTL_ACCESS_TOKEN_IN_SECONDS.plusSeconds(1).toMillis()); // wait for the token expiration = ttl of access token + 1 sec
        try (Response response = httpClientWithOAuth2Cookie(token, false).newCall(apiCall().build()).execute()) {
            assertUnauthorizedOAuthOnlyHeaders(response);
        }
    }

    @Test
    public void testNoAuth()
            throws Exception
    {
        try (Response response = httpClient
                .newCall(apiCall().build())
                .execute()) {
            assertAllUnauthorizedHeaders(response);
        }
    }

    @Test
    public void testInvalidBasicAuth()
            throws Exception
    {
        String userPass = "test:password";
        String basicAuth = "Basic " + Base64.getEncoder().encodeToString(userPass.getBytes());
        try (Response response = httpClient
                .newCall(apiCall().addHeader(AUTHORIZATION, basicAuth).build())
                .execute()) {
            assertAllUnauthorizedHeaders(response);
        }
    }

    private Request.Builder apiCall()
    {
        return new Request.Builder()
                .url(proxyURI.resolve("/v1/cluster").toString())
                .get();
    }

    private void assertUnauthorizedOAuthOnlyHeaders(Response response)
            throws IOException
    {
        String redirectServer = "x_redirect_server=\"" + proxyURI.resolve("/oauth2/token/initiate/");
        String tokenServer = "x_token_server=\"" + proxyURI.resolve("/oauth2/token/");
        assertUnauthorizedResponse(response);
        List<String> headers = response.headers(WWW_AUTHENTICATE);
        assertThat(headers.size()).isEqualTo(1);
        assertThat(headers.get(0)).contains(tokenServer, redirectServer);
    }

    private void assertAllUnauthorizedHeaders(Response response)
            throws IOException
    {
        String redirectServer = "x_redirect_server=\"" + proxyURI.resolve("/oauth2/token/initiate/").toString();
        String tokenServer = "x_token_server=\"" + proxyURI.resolve("/oauth2/token/");
        assertUnauthorizedResponse(response);
        List<String> headers = response.headers(WWW_AUTHENTICATE);
        assertThat(headers.size()).isEqualTo(2);
        assertThat(headers.stream().allMatch(h ->
                        h.contains("Basic realm=\"Presto\"") ||
                                (h.contains(redirectServer) && h.contains(tokenServer))
                )).isTrue();
    }

    private void assertUICallWithCookie(String cookieValue)
            throws IOException
    {
        OkHttpClient httpClient = httpClientWithOAuth2Cookie(cookieValue, true);
        // pass access token in Presto UI cookie
        try (Response response = httpClient.newCall(apiCall().build())
                .execute()) {
            assertThat(response.code()).isEqualTo(OK.getStatusCode());
        }
    }

    private OkHttpClient httpClientWithOAuth2Cookie(String cookieValue, boolean followRedirects)
    {
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        setupInsecureSsl(httpClientBuilder);
        httpClientBuilder.followRedirects(followRedirects);
        httpClientBuilder.cookieJar(new CookieJar()
        {
            @Override
            public void saveFromResponse(HttpUrl url, List<Cookie> cookies)
            {
            }

            @Override
            public List<Cookie> loadForRequest(HttpUrl url)
            {
                return ImmutableList.of(new Cookie.Builder()
                        .domain(proxyURI.getHost())
                        .path("/")
                        .name(OAUTH2_COOKIE)
                        .value(cookieValue)
                        .secure()
                        .build());
            }
        });
        return httpClientBuilder.build();
    }

    private void assertUnauthorizedResponse(Response response)
            throws IOException
    {
        assertThat(response.code()).isEqualTo(UNAUTHORIZED.getStatusCode());
        assertThat(response.body()).isNotNull();
        // NOTE that our errors come in looking like an HTML page since we don't do anything special on the server side so it just is like that.
        assertThat(response.body().string()).contains("Invalid Credentials");
    }
}
