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
package com.facebook.presto.tests;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.server.TestingFunctionServer;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.utils.FnServerAuthTestUtils.JWT_SHARED_SECRET;
import static com.facebook.presto.tests.utils.FnServerAuthTestUtils.buildCoordinatorOnlyRunner;
import static com.facebook.presto.tests.utils.FnServerAuthTestUtils.buildCoordinatorOnlyRunnerWithUrl;
import static com.facebook.presto.tests.utils.FnServerAuthTestUtils.certPath;
import static com.facebook.presto.tests.utils.FnServerAuthTestUtils.findUnusedPort;
import static com.facebook.presto.tests.utils.FnServerAuthTestUtils.getCoordinatorMtlsProperties;
import static com.facebook.presto.tests.utils.FnServerAuthTestUtils.getFunctionServerConfigWithAuth;
import static com.facebook.presto.tests.utils.FnServerAuthTestUtils.getFunctionServerMtlsConfig;
import static com.facebook.presto.tests.utils.FnServerAuthTestUtils.getFunctionServerMtlsConfigWithTruststore;
import static com.facebook.presto.tests.utils.FnServerAuthTestUtils.getJwtProperties;
import static com.facebook.presto.tests.utils.FnServerAuthTestUtils.getMtlsPropertiesWithKeystore;
import static com.facebook.presto.tests.utils.FnServerAuthTestUtils.startFunctionServer;
import static java.lang.String.format;
import static org.testng.Assert.fail;

/**
 * Negative-path integration tests for Function Server authentication.
 *
 * <p>Every test is fully self-contained: it starts a {@link TestingFunctionServer}
 * configured with a deliberately bad credential or mismatched scheme, builds a
 * throwaway coordinator-only {@link DistributedQueryRunner} with the corresponding
 * bad client config, and asserts that the probe query fails with the expected error.
 *
 * <p>This class does <em>not</em> extend {@link AbstractTestQueryFramework} because
 * every test manages its own runner — there is no shared cluster. Using the framework
 * would require a dummy {@code createQueryRunner()} that starts an unused server just
 * to satisfy the framework's {@code @BeforeClass}.
 *
 * <p>Coordinator-only runners (no workers) are sufficient because all rejections occur
 * at the coordinator → function-server boundary, either during function namespace
 * registration (cert/JWT failures) or at connection time (scheme mismatches), before
 * any worker is scheduled.
 *
 * <p>Covered scenarios:
 * <ul>
 *   <li>No client certificate → HTTP 401 from {@code CertificateAuthenticator}
 *   <li>Certificate signed by an untrusted CA → TLS handshake failure
 *   <li>Expired client certificate → TLS handshake failure
 *   <li>JWT token signed with the wrong shared secret → HTTP 401
 *   <li>{@code https://} URL against an HTTP-only function server → connection error
 *   <li>{@code http://} URL against an HTTPS-only function server → protocol error
 * </ul>
 */
public class TestFnServerAuthNegative
{
    private static final Logger log = Logger.get(TestFnServerAuthNegative.class);
    private static final String PROBE_QUERY = "SELECT rest.default.abs(-123)";
    private static final String STORE_PASS = "changeit";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .setSystemProperty("remote_functions_enabled", "true")
            .build();

    // =========================================================================
    // Negative — Certificate tests
    // =========================================================================

    /**
     * The coordinator presents no client certificate to the function server.
     *
     * <p>The function server requires mTLS. Airlift uses {@code setWantClientAuth(true)}
     * so the TLS handshake itself succeeds at the network layer (a missing cert is not a
     * fatal TLS alert), but the application-layer {@code CertificateAuthenticator}
     * returns HTTP 401, which the REST namespace manager surfaces as a query failure.
     */
    @Test
    public void testNoCertificateIsRejected()
            throws Exception
    {
        log.info("TEST: no client certificate → expect 401 from CertificateAuthenticator");

        int fnPort = findUnusedPort();
        TestingFunctionServer fnServer = startFunctionServer(getFunctionServerMtlsConfig(fnPort));
        // The coordinator needs its own keystore to start its HTTPS listener, but we
        // deliberately omit internal-communication.https.keystore.path / .key so the
        // outbound HTTP client sends no client certificate to the function server.
        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .put("http-server.http.enabled", "true")
                .put("http-server.http.port", "0")
                .put("http-server.https.enabled", "true")
                .put("http-server.https.port", "0")
                .put("http-server.https.keystore.path", certPath("coordinator/coordinator-keystore.jks"))
                .put("http-server.https.keystore.key", STORE_PASS)
                .put("http-server.https.truststore.path", certPath("truststore.jks"))
                .put("http-server.https.truststore.key", STORE_PASS)
                .put("internal-communication.https.required", "true")
                // No keystore for outbound requests → no client certificate presented.
                .put("internal-communication.https.trust-store-path", certPath("truststore.jks"))
                .put("internal-communication.https.trust-store-password", STORE_PASS)
                .put("list-built-in-functions-only", "false")
                .put("node-scheduler.include-coordinator", "true")
                .build();

        DistributedQueryRunner runner = buildCoordinatorOnlyRunner(coordinatorProperties, fnServer);
        try {
            assertFails(runner, ".*401.*|.*Unexpected response.*");
        }
        finally {
            runner.close();
        }
    }

    /**
     * The coordinator presents a certificate whose issuer is absent from the function
     * server's truststore.
     *
     * <p>{@code function-server/invalid-keystore.jks} was signed by a throwaway CA that
     * is not in the function server's {@code truststore.jks}. Airlift configures the TLS
     * listener with {@code setWantClientAuth(true)} (not NEED), so the handshake itself
     * completes; the application-layer {@code CertificateAuthenticator} then rejects the
     * untrusted certificate and returns HTTP 401.
     */
    @Test
    public void testUntrustedCaCertificateIsRejected()
            throws Exception
    {
        log.info("TEST: certificate from untrusted CA → expect 401 from CertificateAuthenticator");

        int fnPort = findUnusedPort();
        TestingFunctionServer fnServer = startFunctionServer(getFunctionServerMtlsConfig(fnPort));
        // Coordinator uses the invalid keystore (different, untrusted CA) but still
        // trusts the function server's certificate via the shared truststore.
        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .putAll(getMtlsPropertiesWithKeystore("function-server/invalid-keystore.jks"))
                .put("list-built-in-functions-only", "false")
                .put("node-scheduler.include-coordinator", "true")
                .build();

        DistributedQueryRunner runner = buildCoordinatorOnlyRunner(coordinatorProperties, fnServer);
        try {
            assertFails(runner, ".*SSL.*|.*certificate.*|.*handshake.*|.*REST_SERVER_ERROR.*|.*Unexpected.*|.*401.*");
        }
        finally {
            runner.close();
        }
    }

    /**
     * The coordinator presents a certificate signed by a trusted CA but with an expired
     * validity window.
     *
     * <p>The function server's truststore ({@code expired-truststore.jks}) contains the
     * expired test CA so the issuer is recognised; the only rejection reason is the
     * validity period having elapsed.
     */
    @Test
    public void testExpiredCertificateIsRejected()
            throws Exception
    {
        log.info("TEST: expired client certificate → expect TLS handshake failure");

        int fnPort = findUnusedPort();
        // Function server trusts the expired CA — failure is purely due to cert expiry.
        TestingFunctionServer fnServer = startFunctionServer(
                getFunctionServerMtlsConfigWithTruststore(fnPort, certPath("expired-truststore.jks"), STORE_PASS));
        // Coordinator presents the expired leaf cert.
        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .putAll(getMtlsPropertiesWithKeystore("function-server/expired-keystore.jks"))
                .put("list-built-in-functions-only", "false")
                .put("node-scheduler.include-coordinator", "true")
                .build();

        DistributedQueryRunner runner = buildCoordinatorOnlyRunner(coordinatorProperties, fnServer);
        try {
            assertFails(runner, ".*expired.*|.*SSL.*|.*certificate.*|.*handshake.*|.*REST_SERVER_ERROR.*|.*Unexpected.*");
        }
        finally {
            runner.close();
        }
    }

    // =========================================================================
    // Negative — JWT tests
    // =========================================================================

    /**
     * JWT is enabled on the function server but the coordinator signs tokens with a
     * different shared secret.
     *
     * <p>{@code InternalAuthenticationFilter} on the function server rejects the
     * mismatched token with HTTP 401, which the REST namespace manager surfaces as a
     * query failure.
     */
    @Test
    public void testJwtWrongSecretIsRejected()
            throws Exception
    {
        log.info("TEST: JWT signed with wrong secret → expect 401");

        int fnPort = findUnusedPort();
        // Function server expects tokens signed with JWT_SHARED_SECRET.
        TestingFunctionServer fnServer = startFunctionServer(
                getFunctionServerConfigWithAuth(fnPort, JWT_SHARED_SECRET, true));
        // Coordinator generates tokens signed with a different secret.
        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .putAll(getCoordinatorMtlsProperties())
                .putAll(getJwtProperties("wrong-secret"))
                .put("node-scheduler.include-coordinator", "true")
                .build();

        DistributedQueryRunner runner = buildCoordinatorOnlyRunner(coordinatorProperties, fnServer);
        try {
            assertFails(runner, ".*401.*|.*Unexpected response.*|.*REST_SERVER_BAD_RESPONSE.*");
        }
        finally {
            runner.close();
        }
    }

    // =========================================================================
    // Negative — Scheme-mismatch tests
    // =========================================================================

    /**
     * The function namespace URL uses {@code https://} but the function server only
     * listens on plain HTTP. The TLS ClientHello arrives on a plain-text socket,
     * causing an immediate connection error.
     */
    @Test
    public void testHttpsUrlAgainstHttpOnlyFunctionServerFails()
            throws Exception
    {
        log.info("TEST: https:// URL against http-only function server → expect connection error");

        int fnPort = findUnusedPort();
        // Plain-HTTP function server — no TLS at all.
        TestingFunctionServer fnServer = startFunctionServer(ImmutableMap.of(
                "http-server.http.enabled", "true",
                "http-server.http.port", String.valueOf(fnPort),
                "http-server.https.enabled", "false"));
        // Valid coordinator mTLS config. Namespace URL deliberately uses https:// even
        // though the server only speaks http:// — that is the mismatch under test.
        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .putAll(getCoordinatorMtlsProperties())
                .put("node-scheduler.include-coordinator", "true")
                .build();
        DistributedQueryRunner runner = buildCoordinatorOnlyRunnerWithUrl(
                coordinatorProperties, fnServer, "https://127.0.0.1:" + fnPort);
        try {
            assertFails(runner,
                    ".*REST_SERVER_ERROR.*|.*REST_SERVER_CONNECT_ERROR.*|.*Failed to connect.*|.*Connection.*|.*Unexpected.*|.*SSLHandshakeException.*|.*plaintext connection.*");
        }
        finally {
            runner.close();
        }
    }

    /**
     * The function namespace URL uses {@code http://} but the function server only
     * listens on HTTPS/TLS. Plain HTTP bytes sent to a TLS port produce a protocol error.
     */
    @Test
    public void testHttpUrlAgainstHttpsOnlyFunctionServerFails()
            throws Exception
    {
        log.info("TEST: http:// URL against https-only function server → expect protocol error");

        int fnPort = findUnusedPort();
        // HTTPS-only function server.
        TestingFunctionServer fnServer = startFunctionServer(getFunctionServerMtlsConfig(fnPort));
        // Valid coordinator mTLS config. Namespace URL deliberately uses http:// even
        // though the server only speaks https:// — that is the mismatch under test.
        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .putAll(getCoordinatorMtlsProperties())
                .put("node-scheduler.include-coordinator", "true")
                .build();
        DistributedQueryRunner runner = buildCoordinatorOnlyRunnerWithUrl(
                coordinatorProperties, fnServer, "http://127.0.0.1:" + fnPort);
        try {
            assertFails(runner,
                    ".*REST_SERVER_ERROR.*|.*REST_SERVER_CONNECT_ERROR.*|.*REST_SERVER_BAD_RESPONSE.*|.*Unexpected.*|.*HTTP protocol violation.*|.*bad response.*|.*Illegal character.*");
        }
        finally {
            runner.close();
        }
    }

    // =========================================================================
    // Helper
    // =========================================================================

    /**
     * Executes {@link #PROBE_QUERY} on the given runner and asserts that it throws a
     * {@link RuntimeException} whose message matches {@code expectedRegex}.
     */
    private void assertFails(DistributedQueryRunner runner, String expectedRegex)
    {
        try {
            runner.execute(SESSION, PROBE_QUERY);
            fail(format("Expected query to fail but it succeeded: %s", PROBE_QUERY));
        }
        catch (RuntimeException ex) {
            String message = ex.getMessage() == null ? "" : ex.getMessage();
            if (!message.matches(expectedRegex)) {
                fail(format("Query failed with unexpected message.%nExpected pattern : %s%nActual message   : %s",
                        expectedRegex, message));
            }
        }
    }
}
