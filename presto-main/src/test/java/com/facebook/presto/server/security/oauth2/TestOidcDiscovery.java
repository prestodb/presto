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

import com.facebook.airlift.http.server.Authenticator;
import com.facebook.airlift.http.server.HttpServerConfig;
import com.facebook.airlift.http.server.HttpServerInfo;
import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.server.security.oauth2.OAuth2ServerConfigProvider.OAuth2ServerConfig;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.http.client.HttpStatus.TOO_MANY_REQUESTS;
import static com.facebook.presto.server.security.oauth2.BaseOAuth2AuthenticationFilterTest.PRESTO_CLIENT_ID;
import static com.facebook.presto.server.security.oauth2.BaseOAuth2AuthenticationFilterTest.PRESTO_CLIENT_SECRET;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.io.File.createTempFile;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOidcDiscovery
{
    @Test(dataProvider = "staticConfiguration")
    public void testStaticConfiguration(Optional<String> accessTokenPath, Optional<String> userinfoPath)
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(ImmutableMap.of("/jwks.json", "jwk/jwk-public.json"))) {
            URI issuer = metadataServer.getBaseUrl();
            Optional<URI> accessTokenIssuer = accessTokenPath.map(issuer::resolve);
            Optional<URI> userinfoUrl = userinfoPath.map(issuer::resolve);
            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                    .put("http-server.authentication.oauth2.oidc.discovery", "false")
                    .put("http-server.authentication.oauth2.auth-url", issuer.resolve("/connect/authorize").toString())
                    .put("http-server.authentication.oauth2.token-url", issuer.resolve("/connect/token").toString())
                    .put("http-server.authentication.oauth2.jwks-url", issuer.resolve("/jwks.json").toString())
                    .put("configuration-based-authorizer.role-regex-map.file-path", createTempFile("regex-map", null).getAbsolutePath().toString());
            accessTokenIssuer.map(URI::toString).ifPresent(uri -> properties.put("http-server.authentication.oauth2.access-token-issuer", uri));
            userinfoUrl.map(URI::toString).ifPresent(uri -> properties.put("http-server.authentication.oauth2.userinfo-url", uri));
            try (TestingPrestoServer server = createServer(properties.build())) {
                assertConfiguration(server, issuer, accessTokenIssuer.map(issuer::resolve), userinfoUrl.map(issuer::resolve));
            }
        }
    }

    @DataProvider(name = "staticConfiguration")
    public static Object[][] staticConfiguration()
    {
        return new Object[][] {
                {Optional.empty(), Optional.empty()},
                {Optional.of("/access-token-issuer"), Optional.of("/userinfo")},
        };
    }

    @Test(dataProvider = "oidcDiscovery")
    public void testOidcDiscovery(String configuration, Optional<String> accessTokenIssuer, Optional<String> userinfoUrl)
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(
                ImmutableMap.<String, String>builder()
                        .put("/.well-known/openid-configuration", "oidc/" + configuration)
                        .put("/jwks.json", "jwk/jwk-public.json")
                        .build());
                TestingPrestoServer server = createServer(
                        ImmutableMap.<String, String>builder()
                                .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                                .put("http-server.authentication.oauth2.oidc.discovery", "true")
                                .put("configuration-based-authorizer.role-regex-map.file-path", createTempFile("regex-map", null).getAbsolutePath().toString())
                                .build())) {
            URI issuer = metadataServer.getBaseUrl();
            assertConfiguration(server, issuer, accessTokenIssuer.map(issuer::resolve), userinfoUrl.map(issuer::resolve));
        }
    }

    @DataProvider(name = "oidcDiscovery")
    public static Object[][] oidcDiscovery()
    {
        return new Object[][] {
                {"openid-configuration.json", Optional.empty(), Optional.of("/connect/userinfo")},
                {"openid-configuration-without-userinfo.json", Optional.empty(), Optional.empty()},
                {"openid-configuration-with-access-token-issuer.json", Optional.of("http://access-token-issuer.com/adfs/services/trust"), Optional.of("/connect/userinfo")},
        };
    }

    @Test
    public void testIssuerCheck()
    {
        assertThatThrownBy(() -> {
            try (MetadataServer metadataServer = new MetadataServer(
                    ImmutableMap.<String, String>builder()
                            .put("/.well-known/openid-configuration", "oidc/openid-configuration-invalid-issuer.json")
                            .put("/jwks.json", "jwk/jwk-public.json")
                            .build());
                    TestingPrestoServer server = createServer(
                            ImmutableMap.<String, String>builder()
                                    .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                                    .put("http-server.authentication.oauth2.oidc.discovery", "true")
                                    .put("configuration-based-authorizer.role-regex-map.file-path", createTempFile("regex-map", null).getAbsolutePath().toString())
                                    .build())) {
                // should throw an exception
                server.getInstance(Key.get(OAuth2ServerConfigProvider.class)).get();
            }
        }).hasMessageContaining(
                "Invalid response from OpenID Metadata endpoint. " +
                        "The value of the \"issuer\" claim in Metadata document different than the Issuer URL used for the Configuration Request.");
    }

    @Test
    public void testStopOnClientError()
    {
        assertThatThrownBy(() -> {
            try (MetadataServer metadataServer = new MetadataServer(ImmutableMap.of());
                    TestingPrestoServer server = createServer(
                            ImmutableMap.<String, String>builder()
                                    .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                                    .put("http-server.authentication.oauth2.oidc.discovery", "true")
                                    .put("configuration-based-authorizer.role-regex-map.file-path", createTempFile("regex-map", null).getAbsolutePath().toString())
                                    .build())) {
                // should throw an exception
                server.getInstance(Key.get(OAuth2ServerConfigProvider.class)).get();
            }
        }).hasMessageContaining("Invalid response from OpenID Metadata endpoint. Expected response code to be 200, but was 404");
    }

    @Test
    public void testOidcDiscoveryRetrying()
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(new MetadataServletWithStartup(
                ImmutableMap.<String, String>builder()
                        .put("/.well-known/openid-configuration", "oidc/openid-configuration.json")
                        .put("/jwks.json", "jwk/jwk-public.json")
                        .build(), 5));
                TestingPrestoServer server = createServer(
                        ImmutableMap.<String, String>builder()
                                .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                                .put("http-server.authentication.oauth2.oidc.discovery", "true")
                                .put("http-server.authentication.oauth2.oidc.discovery.timeout", "10s")
                                .put("configuration-based-authorizer.role-regex-map.file-path", createTempFile("regex-map", null).getAbsolutePath().toString())
                                .build())) {
            URI issuer = metadataServer.getBaseUrl();
            assertConfiguration(server, issuer, Optional.empty(), Optional.of(issuer.resolve("/connect/userinfo")));
        }
    }

    @Test
    public void testOidcDiscoveryTimesOut()
    {
        assertThatThrownBy(() -> {
            try (MetadataServer metadataServer = new MetadataServer(new MetadataServletWithStartup(
                    ImmutableMap.<String, String>builder()
                            .put("/.well-known/openid-configuration", "oidc/openid-configuration.json")
                            .put("/jwks.json", "jwk/jwk-public.json")
                            .build(), 10));
                    TestingPrestoServer server = createServer(
                            ImmutableMap.<String, String>builder()
                                    .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                                    .put("http-server.authentication.oauth2.oidc.discovery", "true")
                                    .put("http-server.authentication.oauth2.oidc.discovery.timeout", "5s")
                                    .put("configuration-based-authorizer.role-regex-map.file-path", createTempFile("regex-map", null).getAbsolutePath().toString())
                                    .build())) {
                // should throw an exception
                server.getInstance(Key.get(OAuth2ServerConfigProvider.class)).get();
            }
        }).hasMessageContaining("Invalid response from OpenID Metadata endpoint: 429");
    }

    @Test
    public void testIgnoringUserinfoUrl()
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(
                ImmutableMap.<String, String>builder()
                        .put("/.well-known/openid-configuration", "oidc/openid-configuration.json")
                        .put("/jwks.json", "jwk/jwk-public.json")
                        .build());
                TestingPrestoServer server = createServer(
                        ImmutableMap.<String, String>builder()
                                .put("http-server.authentication.oauth2.issuer", metadataServer.getBaseUrl().toString())
                                .put("http-server.authentication.oauth2.oidc.discovery", "true")
                                .put("http-server.authentication.oauth2.oidc.use-userinfo-endpoint", "false")
                                .put("configuration-based-authorizer.role-regex-map.file-path", createTempFile("regex-map", null).getAbsolutePath().toString())
                                .build())) {
            URI issuer = metadataServer.getBaseUrl();
            assertConfiguration(server, issuer, Optional.empty(), Optional.empty());
        }
    }

    @Test
    public void testBackwardCompatibility()
            throws Exception
    {
        try (MetadataServer metadataServer = new MetadataServer(
                ImmutableMap.<String, String>builder()
                        .put("/.well-known/openid-configuration", "oidc/openid-configuration-with-access-token-issuer.json")
                        .put("/jwks.json", "jwk/jwk-public.json")
                        .build())) {
            URI issuer = metadataServer.getBaseUrl();
            URI authUrl = issuer.resolve("/custom-authorize");
            URI tokenUrl = issuer.resolve("/custom-token");
            URI jwksUrl = issuer.resolve("/custom-jwks.json");
            String accessTokenIssuer = issuer.resolve("/custom-access-token-issuer").toString();
            URI userinfoUrl = issuer.resolve("/custom-userinfo-url");
            try (TestingPrestoServer server = createServer(
                    ImmutableMap.<String, String>builder()
                            .put("http-server.authentication.oauth2.issuer", issuer.toString())
                            .put("http-server.authentication.oauth2.oidc.discovery", "true")
                            .put("http-server.authentication.oauth2.auth-url", authUrl.toString())
                            .put("http-server.authentication.oauth2.token-url", tokenUrl.toString())
                            .put("http-server.authentication.oauth2.jwks-url", jwksUrl.toString())
                            .put("http-server.authentication.oauth2.access-token-issuer", accessTokenIssuer)
                            .put("http-server.authentication.oauth2.userinfo-url", userinfoUrl.toString())
                            .put("configuration-based-authorizer.role-regex-map.file-path", createTempFile("regex-map", null).getAbsolutePath().toString())
                            .build())) {
                assertComponents(server);
                OAuth2ServerConfig config = server.getInstance(Key.get(OAuth2ServerConfigProvider.class)).get();
                assertThat(config.getAccessTokenIssuer()).isEqualTo(Optional.of(accessTokenIssuer));
                assertThat(config.getAuthUrl()).isEqualTo(authUrl);
                assertThat(config.getTokenUrl()).isEqualTo(tokenUrl);
                assertThat(config.getJwksUrl()).isEqualTo(jwksUrl);
                assertThat(config.getUserinfoUrl()).isEqualTo(Optional.of(userinfoUrl));
            }
        }
    }

    private static void assertConfiguration(TestingPrestoServer server, URI issuer, Optional<URI> accessTokenIssuer, Optional<URI> userinfoUrl)
    {
        assertComponents(server);
        OAuth2ServerConfig config = server.getInstance(Key.get(OAuth2ServerConfigProvider.class)).get();
        assertThat(config.getAccessTokenIssuer()).isEqualTo(accessTokenIssuer.map(URI::toString));
        assertThat(config.getAuthUrl()).isEqualTo(issuer.resolve("/connect/authorize"));
        assertThat(config.getTokenUrl()).isEqualTo(issuer.resolve("/connect/token"));
        assertThat(config.getJwksUrl()).isEqualTo(issuer.resolve("/jwks.json"));
        assertThat(config.getUserinfoUrl()).isEqualTo(userinfoUrl);
    }

    private static void assertComponents(TestingPrestoServer server)
    {
        List<Authenticator> authenticators = server.getInstance(Key.get(new TypeLiteral<List<Authenticator>>() {}));
        assertThat(authenticators).hasSize(1);
        assertThat(authenticators.get(0)).isInstanceOf(OAuth2Authenticator.class);
//        assertThat(server.getInstance(Key.get(WebUiAuthenticationFilter.class))).isInstanceOf(OAuth2WebUiAuthenticationFilter.class);
        // does not throw an exception
        server.getInstance(Key.get(OAuth2Client.class)).load();
    }

    private static TestingPrestoServer createServer(Map<String, String> configuration)
            throws Exception
    {
        ImmutableMap config = ImmutableMap.<String, String>builder()
                .put("http-server.authentication.allow-forwarded-https", "true")
                .put("http-server.authentication.type", "OAUTH2")
                .put("http-server.authentication.oauth2.client-id", PRESTO_CLIENT_ID)
                .put("http-server.authentication.oauth2.client-secret", PRESTO_CLIENT_SECRET)
                .putAll(configuration)
                .build();

        return new TestingPrestoServer(config);
    }

    public static class MetadataServer
            implements AutoCloseable
    {
        private final TestingHttpServer httpServer;

        public MetadataServer(Map<String, String> responseMapping)
                throws Exception
        {
            this(new MetadataServlet(responseMapping));
        }

        public MetadataServer(HttpServlet servlet)
                throws Exception
        {
            NodeInfo nodeInfo = new NodeInfo("test");
            HttpServerConfig config = new HttpServerConfig().setHttpPort(0);
            HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);
            httpServer = new TestingHttpServer(httpServerInfo, nodeInfo, config, servlet, ImmutableMap.of(), ImmutableMap.of(), Optional.empty());
            httpServer.start();
        }

        public URI getBaseUrl()
        {
            return httpServer.getBaseUrl();
        }

        @Override
        public void close()
                throws Exception
        {
            httpServer.stop();
        }
    }

    public static class MetadataServlet
            extends HttpServlet
    {
        private final Map<String, String> responseMapping;

        public MetadataServlet(Map<String, String> responseMapping)
        {
            this.responseMapping = requireNonNull(responseMapping, "responseMapping is null");
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            String fileName = responseMapping.get(request.getPathInfo());
            if (fileName == null) {
                response.setStatus(404);
                return;
            }
            response.setHeader(CONTENT_TYPE, APPLICATION_JSON);
            String body = Resources.toString(Resources.getResource(fileName), UTF_8);
            body = body.replaceAll("https://issuer.com", request.getRequestURL().toString().replace("/.well-known/openid-configuration", ""));
            response.getWriter().write(body);
        }
    }

    public static class MetadataServletWithStartup
            extends MetadataServlet
    {
        private final Instant startTime;

        public MetadataServletWithStartup(Map<String, String> responseMapping, int startupInSeconds)
        {
            super(responseMapping);
            startTime = Instant.now().plusSeconds(startupInSeconds);
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            if (Instant.now().isBefore(startTime)) {
                response.setStatus(TOO_MANY_REQUESTS.code());
                return;
            }
            super.doGet(request, response);
        }
    }
}
