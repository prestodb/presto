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
package com.facebook.presto.client;

import com.facebook.airlift.json.JsonCodec;
import com.google.common.collect.ImmutableSet;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.tls.HandshakeCertificates;
import okhttp3.tls.HeldCertificate;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.OkHttpUtil.basicAuth;
import static com.facebook.presto.client.OkHttpUtil.isTrustedRedirectTarget;
import static com.facebook.presto.client.OkHttpUtil.normalizeTrustedRedirectHosts;
import static com.facebook.presto.client.OkHttpUtil.tokenAuth;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.LOCATION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestOkHttpUtilRedirectAuth
{
    @DataProvider
    public Object[][] redirectTargets()
    {
        String router = "https://router.example.com/v1/statement";
        return new Object[][] {
                // same host and port — trusted
                {router, "https://router.example.com/v1/statement", true},
                {"https://router.example.com:8080/v1/statement", "https://router.example.com:8080/v1/statement", true},
                // sibling host under the same registrable domain — NOT trusted (independently controlled service)
                {router, "https://engine-1.example.com/v1/statement", false},
                {"https://router.lakehouse.dev.ibmappdomain.cloud/v1/statement", "https://engine.lakehouse.dev.ibmappdomain.cloud/v1/statement", false},
                // different domain
                {router, "https://evil.com/v1/statement", false},
                {router, "https://example.com.evil.com/v1/statement", false},
                {router, "https://notexample.com/v1/statement", false},
                // https -> http downgrade
                {router, "http://router.example.com/v1/statement", false},
                // same host, different port — NOT trusted
                {"https://router.example.com:8080/v1/statement", "https://router.example.com:9090/v1/statement", false},
                // IP addresses must match host and port exactly
                {"https://10.0.0.1/v1/statement", "https://10.0.0.2/v1/statement", false},
                {"https://10.0.0.1/v1/statement", "https://10.0.0.1/v1/statement", true},
                {"https://10.0.0.1:8080/v1/statement", "https://10.0.0.1:9090/v1/statement", false},
                {"https://localhost/v1/statement", "https://otherhost/v1/statement", false},
                // shared hosting suffixes on the public suffix list
                {"https://alice.github.io/v1/statement", "https://bob.github.io/v1/statement", false},
        };
    }

    @Test(dataProvider = "redirectTargets")
    public void testIsTrustedRedirectTarget(String origin, String target, boolean expected)
    {
        assertEquals(isTrustedRedirectTarget(HttpUrl.get(origin), HttpUrl.get(target)), expected);
    }

    @DataProvider
    public Object[][] optedInRedirectTargets()
    {
        String router = "https://router.example.com/v1/statement";
        return new Object[][] {
                // listed host, any port
                {router, "https://engine-1.example.com/v1/statement", ImmutableSet.of("engine-1.example.com"), true},
                {router, "https://engine-1.example.com:8443/v1/statement", ImmutableSet.of("engine-1.example.com"), true},
                // host matching is case insensitive
                {router, "https://ENGINE-1.example.com/v1/statement", ImmutableSet.of("Engine-1.Example.com"), true},
                // listed host:port
                {router, "https://engine-1.example.com:8443/v1/statement", ImmutableSet.of("engine-1.example.com:8443"), true},
                {router, "https://engine-1.example.com/v1/statement", ImmutableSet.of("engine-1.example.com:443"), true},
                // listed host:port, but a different port
                {router, "https://engine-1.example.com:9443/v1/statement", ImmutableSet.of("engine-1.example.com:8443"), false},
                // unconfigured destinations stay rejected: other host, sibling of the listed host, parent domain
                {router, "https://engine-2.example.com/v1/statement", ImmutableSet.of("engine-1.example.com"), false},
                {router, "https://evil.com/v1/statement", ImmutableSet.of("engine-1.example.com"), false},
                {router, "https://x.engine-1.example.com/v1/statement", ImmutableSet.of("engine-1.example.com"), false},
                {router, "https://engine-1.example.com.evil.com/v1/statement", ImmutableSet.of("engine-1.example.com"), false},
                {router, "https://engine-1.example.com/v1/statement", ImmutableSet.of(), false},
                // HTTPS -> HTTP downgrade is rejected even for a listed host
                {router, "http://engine-1.example.com/v1/statement", ImmutableSet.of("engine-1.example.com"), false},
                // an HTTP origin never gets to forward credentials to another host
                {"http://router.example.com/v1/statement", "https://engine-1.example.com/v1/statement", ImmutableSet.of("engine-1.example.com"), false},
        };
    }

    @Test(dataProvider = "optedInRedirectTargets")
    public void testIsTrustedRedirectTargetWithOptIn(String origin, String target, Set<String> trustedHosts, boolean expected)
    {
        assertEquals(isTrustedRedirectTarget(HttpUrl.get(origin), HttpUrl.get(target), normalizeTrustedRedirectHosts(trustedHosts)), expected);
    }

    @Test
    public void testNormalizeTrustedRedirectHosts()
    {
        assertEquals(normalizeTrustedRedirectHosts(ImmutableSet.of(" Engine-1.Example.com ", "engine-2.example.com:8443", "[::1]:8443")),
                ImmutableSet.of("engine-1.example.com", "engine-2.example.com:8443", "::1:8443"));
        assertEquals(normalizeTrustedRedirectHosts(ImmutableSet.of()), ImmutableSet.of());
    }

    @DataProvider
    public Object[][] invalidTrustedRedirectHosts()
    {
        return new Object[][] {
                {""},
                {"   "},
                {"*.example.com"},
                {"*"},
                {"https://engine-1.example.com"},
                {"engine-1.example.com/path"},
                {"user@engine-1.example.com"},
                {"engine-1.example.com:notaport"},
                {":8443"},
        };
    }

    @Test(dataProvider = "invalidTrustedRedirectHosts", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid trusted redirect host .*")
    public void testInvalidTrustedRedirectHosts(String host)
    {
        normalizeTrustedRedirectHosts(ImmutableSet.of(host));
    }

    /**
     * Router to engine redirect across two different hosts over real TLS. Without the opt-in the
     * engine gets no Authorization header (and would answer 401); with the engine listed in
     * trustedRedirectHosts the credentials reach it. The redirect is cross-host because the router
     * is addressed as "localhost" and the engine as "127.0.0.1".
     */
    @Test
    public void testCredentialsSentToOptedInHostOverHttps()
            throws Exception
    {
        assertCrossHostHttpsRedirect(true, ImmutableSet.of("127.0.0.1"), "Basic dTpw");
        assertCrossHostHttpsRedirect(false, ImmutableSet.of("127.0.0.1"), "Bearer token");
        assertCrossHostHttpsRedirect(true, ImmutableSet.of("127.0.0.1:__ENGINE_PORT__"), "Basic dTpw");
    }

    @Test
    public void testCredentialsNotSentToUnconfiguredHostOverHttps()
            throws Exception
    {
        assertCrossHostHttpsRedirect(true, ImmutableSet.of(), null);
        assertCrossHostHttpsRedirect(false, ImmutableSet.of(), null);
        // a different host is listed, but not the one being redirected to
        assertCrossHostHttpsRedirect(true, ImmutableSet.of("engine.example.com"), null);
        // right host, wrong port
        assertCrossHostHttpsRedirect(true, ImmutableSet.of("127.0.0.1:1"), null);
    }

    private static void assertCrossHostHttpsRedirect(boolean basic, Set<String> trustedHosts, String expectedEngineAuthorization)
            throws Exception
    {
        HeldCertificate certificate = new HeldCertificate.Builder()
                .addSubjectAlternativeName("localhost")
                .addSubjectAlternativeName("127.0.0.1")
                .build();
        HandshakeCertificates serverCertificates = new HandshakeCertificates.Builder()
                .heldCertificate(certificate)
                .build();
        HandshakeCertificates clientCertificates = new HandshakeCertificates.Builder()
                .addTrustedCertificate(certificate.certificate())
                .build();

        try (MockWebServer router = new MockWebServer(); MockWebServer engine = new MockWebServer()) {
            router.useHttps(serverCertificates.sslSocketFactory());
            engine.useHttps(serverCertificates.sslSocketFactory());
            router.start();
            engine.start();

            // allow tests to refer to the engine's dynamically assigned port
            Set<String> hosts = trustedHosts.stream()
                    .map(host -> host.replace("__ENGINE_PORT__", String.valueOf(engine.getPort())))
                    .collect(toImmutableSet());

            router.enqueue(new MockResponse.Builder()
                    .code(307)
                    .addHeader(LOCATION, "https://127.0.0.1:" + engine.getPort() + "/v1/statement")
                    .build());
            engine.enqueue(new MockResponse.Builder().code(200).build());

            OkHttpClient client = new OkHttpClient.Builder()
                    .sslSocketFactory(clientCertificates.sslSocketFactory(), clientCertificates.trustManager())
                    .addNetworkInterceptor(basic ? basicAuth("u", "p", hosts) : tokenAuth("token", hosts))
                    .build();
            Request request = new Request.Builder()
                    .url("https://localhost:" + router.getPort() + "/v1/statement")
                    .post(RequestBody.create("select 1", MediaType.get("text/plain")))
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), 200);
            }

            assertEquals(router.takeRequest().getHeaders().get(AUTHORIZATION), basic ? "Basic dTpw" : "Bearer token");
            assertEquals(engine.takeRequest().getHeaders().get(AUTHORIZATION), expectedEngineAuthorization);
        }
    }

    @Test
    public void testBasicAuthSentOnSameHostAndPortRedirect()
            throws Exception
    {
        assertAuthorizationAfterSameServerRedirect(true, "Basic dTpw");
    }

    @Test
    public void testBasicAuthNotSentToDifferentHostOnRedirect()
            throws Exception
    {
        // "localhost" -> "127.0.0.1": different hosts
        assertAuthorizationAfterCrossServerRedirect(true, "localhost", "127.0.0.1", null);
    }

    @Test
    public void testTokenAuthNotSentToDifferentHostOnRedirect()
            throws Exception
    {
        assertAuthorizationAfterCrossServerRedirect(false, "localhost", "127.0.0.1", null);
    }

    @Test
    public void testTokenAuthSentOnSameHostAndPortRedirect()
            throws Exception
    {
        assertAuthorizationAfterSameServerRedirect(false, "Bearer token");
    }

    @Test
    public void testBasicAuthNotSentToDifferentPortOnRedirect()
            throws Exception
    {
        // Same host but different port — must NOT receive credentials
        assertAuthorizationAfterCrossServerRedirect(true, "localhost", "localhost", null);
    }

    /**
     * Redirect that stays on the same host and port (single MockWebServer handles both hops).
     * The server serves a 307 to its own address with a different path, then a 200.
     */
    private static void assertAuthorizationAfterSameServerRedirect(boolean basic, String expectedAuthorization)
            throws Exception
    {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new MockResponse.Builder()
                    .code(307)
                    .addHeader(LOCATION, "http://localhost:" + server.getPort() + "/v1/statement/redirected")
                    .build());
            server.enqueue(new MockResponse.Builder().code(200).build());

            OkHttpClient client = new OkHttpClient.Builder()
                    .addNetworkInterceptor(basic ? basicAuth("u", "p") : tokenAuth("token"))
                    .build();
            Request request = new Request.Builder()
                    .url("http://localhost:" + server.getPort() + "/v1/statement")
                    .post(RequestBody.create("select 1", MediaType.get("text/plain")))
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), 200);
            }

            // First request (original)
            assertEquals(server.takeRequest().getHeaders().get(AUTHORIZATION), basic ? "Basic dTpw" : "Bearer token");
            // Second request (same host+port redirect) — must also carry credentials
            assertEquals(server.takeRequest().getHeaders().get(AUTHORIZATION), expectedAuthorization);
        }
    }

    /**
     * Redirect that crosses to a different server (different host or port).
     * Credentials must NOT be forwarded to the second server when the redirect target is untrusted.
     */
    private static void assertAuthorizationAfterCrossServerRedirect(boolean basic, String routerHost, String clusterHost, String expectedClusterAuthorization)
            throws Exception
    {
        try (MockWebServer router = new MockWebServer(); MockWebServer cluster = new MockWebServer()) {
            router.start();
            cluster.start();
            router.enqueue(new MockResponse.Builder()
                    .code(307)
                    .addHeader(LOCATION, "http://" + clusterHost + ":" + cluster.getPort() + "/v1/statement")
                    .build());
            cluster.enqueue(new MockResponse.Builder().code(200).build());

            OkHttpClient client = new OkHttpClient.Builder()
                    .addNetworkInterceptor(basic ? basicAuth("u", "p") : tokenAuth("token"))
                    .build();
            Request request = new Request.Builder()
                    .url("http://" + routerHost + ":" + router.getPort() + "/v1/statement")
                    .post(RequestBody.create("select 1", MediaType.get("text/plain")))
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), 200);
            }

            // Router always gets credentials
            assertEquals(router.takeRequest().getHeaders().get(AUTHORIZATION), basic ? "Basic dTpw" : "Bearer token");
            // Cluster only gets credentials if the redirect target is trusted
            String clusterAuthorization = cluster.takeRequest().getHeaders().get(AUTHORIZATION);
            if (expectedClusterAuthorization == null) {
                assertNull(clusterAuthorization);
            }
            else {
                assertEquals(clusterAuthorization, expectedClusterAuthorization);
            }
        }
    }

    /**
     * Goes through JsonResponse.execute, the entry point used by StatementClientV1. For a regular
     * (non one-shot) POST body OkHttp follows the 307 itself within the single call, so
     * chain.call().request() remains the origin.
     *
     * Untrusted target (different host): credentials must NOT be forwarded.
     */
    @Test
    public void testBasicAuthNotSentViaJsonResponseToDifferentHost()
            throws Exception
    {
        try (MockWebServer router = new MockWebServer(); MockWebServer cluster = new MockWebServer()) {
            router.start();
            cluster.start();
            // Router returns 307 to a different host (localhost -> 127.0.0.1)
            router.enqueue(new MockResponse.Builder()
                    .code(307)
                    .addHeader(LOCATION, "http://127.0.0.1:" + cluster.getPort() + "/v1/statement")
                    .body("{}")
                    .build());
            cluster.enqueue(new MockResponse.Builder().code(200).body("{}").build());

            OkHttpClient client = new OkHttpClient.Builder()
                    .addNetworkInterceptor(basicAuth("u", "p"))
                    .followRedirects(true)
                    .build();
            Request request = new Request.Builder()
                    .url("http://localhost:" + router.getPort() + "/v1/statement")
                    .post(RequestBody.create("select 1", MediaType.get("text/plain")))
                    .build();

            JsonCodec<String> codec = jsonCodec(String.class);
            JsonResponse.execute(codec, client, request);

            // Router must receive credentials
            assertEquals(router.takeRequest().getHeaders().get(AUTHORIZATION), "Basic dTpw");
            // Cluster must NOT receive credentials (different host)
            assertNull(cluster.takeRequest().getHeaders().get(AUTHORIZATION));
        }
    }

    /**
     * Goes through JsonResponse.execute. Trusted target (same host and port): credentials MUST be forwarded.
     */
    @Test
    public void testBasicAuthSentViaJsonResponseToSameHostAndPort()
            throws Exception
    {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            // Redirect to the same host+port with a different path
            server.enqueue(new MockResponse.Builder()
                    .code(307)
                    .addHeader(LOCATION, "http://localhost:" + server.getPort() + "/v1/statement/redirected")
                    .body("{}")
                    .build());
            server.enqueue(new MockResponse.Builder().code(200).body("{}").build());

            OkHttpClient client = new OkHttpClient.Builder()
                    .addNetworkInterceptor(basicAuth("u", "p"))
                    .followRedirects(true)
                    .build();
            Request request = new Request.Builder()
                    .url("http://localhost:" + server.getPort() + "/v1/statement")
                    .post(RequestBody.create("select 1", MediaType.get("text/plain")))
                    .build();

            JsonCodec<String> codec = jsonCodec(String.class);
            JsonResponse.execute(codec, client, request);

            // Both the original and the redirected request must carry credentials
            assertEquals(server.takeRequest().getHeaders().get(AUTHORIZATION), "Basic dTpw");
            assertEquals(server.takeRequest().getHeaders().get(AUTHORIZATION), "Basic dTpw");
        }
    }

    /**
     * Goes through JsonResponse.execute. Same host but different port: credentials must NOT be forwarded.
     */
    @Test
    public void testBasicAuthNotSentViaJsonResponseToDifferentPort()
            throws Exception
    {
        try (MockWebServer router = new MockWebServer(); MockWebServer cluster = new MockWebServer()) {
            router.start();
            cluster.start();
            // Redirect to same host (localhost) but different port
            router.enqueue(new MockResponse.Builder()
                    .code(307)
                    .addHeader(LOCATION, "http://localhost:" + cluster.getPort() + "/v1/statement")
                    .body("{}")
                    .build());
            cluster.enqueue(new MockResponse.Builder().code(200).body("{}").build());

            OkHttpClient client = new OkHttpClient.Builder()
                    .addNetworkInterceptor(basicAuth("u", "p"))
                    .followRedirects(true)
                    .build();
            Request request = new Request.Builder()
                    .url("http://localhost:" + router.getPort() + "/v1/statement")
                    .post(RequestBody.create("select 1", MediaType.get("text/plain")))
                    .build();

            JsonCodec<String> codec = jsonCodec(String.class);
            JsonResponse.execute(codec, client, request);

            // Router must receive credentials
            assertEquals(router.takeRequest().getHeaders().get(AUTHORIZATION), "Basic dTpw");
            // Cluster must NOT receive credentials (different port)
            assertNull(cluster.takeRequest().getHeaders().get(AUTHORIZATION));
        }
    }

    @Test
    public void testBasicAuthDoesNotOverwriteAuthenticatorHeaderOnRetry()
            throws Exception
    {
        assertAuthenticatorHeaderPreservedAfter401(basicAuth("u", "p"), "Basic dTpw");
    }

    @Test
    public void testTokenAuthDoesNotOverwriteAuthenticatorHeaderOnRetry()
            throws Exception
    {
        assertAuthenticatorHeaderPreservedAfter401(tokenAuth("token"), "Bearer token");
    }

    /**
     * Configured credentials are sent on the first request. When the server answers 401 and an
     * Authenticator (as ExternalAuthenticator does) supplies a replacement Authorization header,
     * the retry must carry the Authenticator's header, not the configured credentials again.
     */
    private static void assertAuthenticatorHeaderPreservedAfter401(Interceptor authInterceptor, String expectedFirstAuthorization)
            throws Exception
    {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            server.enqueue(new MockResponse.Builder()
                    .code(401)
                    .addHeader("WWW-Authenticate", "Bearer")
                    .build());
            server.enqueue(new MockResponse.Builder().code(200).build());

            OkHttpClient client = new OkHttpClient.Builder()
                    .addNetworkInterceptor(authInterceptor)
                    .authenticator((route, response) -> response.request().newBuilder()
                            .header(AUTHORIZATION, "Bearer external")
                            .build())
                    .build();
            Request request = new Request.Builder()
                    .url("http://localhost:" + server.getPort() + "/v1/statement")
                    .post(RequestBody.create("select 1", MediaType.get("text/plain")))
                    .build();
            try (Response response = client.newCall(request).execute()) {
                assertEquals(response.code(), 200);
            }

            assertEquals(server.takeRequest().getHeaders().get(AUTHORIZATION), expectedFirstAuthorization);
            assertEquals(server.takeRequest().getHeaders().get(AUTHORIZATION), "Bearer external");
        }
    }
}
