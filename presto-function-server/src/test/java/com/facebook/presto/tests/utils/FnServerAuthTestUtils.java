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
package com.facebook.presto.tests.utils;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.rest.RestBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.server.TestingFunctionServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

/**
 * Utilities for mTLS + JWT testing with Function Server.
 */
public class FnServerAuthTestUtils
{
    private static final Logger log = Logger.get(FnServerAuthTestUtils.class);

    /** Cache of already-extracted cert files: resource path → host temp file path. */
    private static final ConcurrentHashMap<String, String> CERT_CACHE = new ConcurrentHashMap<>();

    public static final String JWT_SHARED_SECRET = "supersecret";
    public static final String JWT_WRONG_SECRET = "wrongsecret";

    private static final Map<String, String> TPCH_PROPERTIES = ImmutableMap.of("tpch.column-naming", "standard");

    private FnServerAuthTestUtils() {}

    public static String convertToLocalhostUri(String uri)
    {
        return uri.replaceFirst("://\\d+\\.\\d+\\.\\d+\\.\\d+:", "://127.0.0.1:");
    }

    private static int findUnusedPort()
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to find unused port", e);
        }
    }

    /**
     * Resolves the path to a certificate resource regardless of which module's working
     * directory the test is running from. The certs are packaged into the presto-function-server
     * test-jar under "certs/", so they are always findable via the classloader.
     */
    private static String certPath(String relativePath)
    {
        String resourceName = "certs/" + relativePath;
        return CERT_CACHE.computeIfAbsent(resourceName, FnServerAuthTestUtils::extractCertResource);
    }

    private static String extractCertResource(String resourceName)
    {
        URL resource = FnServerAuthTestUtils.class.getClassLoader().getResource(resourceName);
        if (resource == null) {
            throw new IllegalStateException("Certificate resource not found on classpath: " + resourceName);
        }
        // If the resource is a plain file (not inside a JAR), return its path directly.
        if ("file".equals(resource.getProtocol())) {
            return Paths.get(resource.getFile()).toAbsolutePath().toString();
        }
        // Resource is inside a JAR — extract it to a temp file so the JVM can open it.
        try {
            String fileName = Paths.get(resourceName).getFileName().toString();
            Path temp = Files.createTempFile("presto-cert-", "-" + fileName);
            temp.toFile().deleteOnExit();
            try (InputStream in = FnServerAuthTestUtils.class.getClassLoader().getResourceAsStream(resourceName)) {
                if (in == null) {
                    throw new IllegalStateException("Could not open resource stream for: " + resourceName);
                }
                Files.copy(in, temp, StandardCopyOption.REPLACE_EXISTING);
            }
            log.info("Extracted cert resource %s to temp file %s", resourceName, temp);
            return temp.toAbsolutePath().toString();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to extract cert resource: " + resourceName, e);
        }
    }

    public static Map<String, String> getSharedMtlsProperties()
    {
        return ImmutableMap.<String, String>builder()
                .putAll(getMtlsProperties("worker/worker-keystore.jks"))
                .put("node-scheduler.include-coordinator", "false")
                .build();
    }

    public static Map<String, String> getCoordinatorMtlsProperties()
    {
        return ImmutableMap.<String, String>builder()
                .putAll(getMtlsProperties("coordinator/coordinator-keystore.jks"))
                .put("list-built-in-functions-only", "false")
                .build();
    }

    /**
     * Builds common HTTPS + mTLS properties for a node, parameterised by keystore path
     * relative to the certs resource directory.
     */
    private static Map<String, String> getMtlsProperties(String keystoreRelativePath)
    {
        return ImmutableMap.<String, String>builder()
                .put("http-server.http.enabled", "true") // true for discovery
                .put("http-server.http.port", "0")
                .put("http-server.https.enabled", "true")
                .put("http-server.https.port", "0")

                .put("http-server.https.keystore.path", certPath(keystoreRelativePath))
                .put("http-server.https.keystore.key", "changeit")
                .put("http-server.https.truststore.path", certPath("truststore.jks"))
                .put("http-server.https.truststore.key", "changeit")

                .put("internal-communication.https.required", "true")
                .put("internal-communication.https.keystore.path", certPath(keystoreRelativePath))
                .put("internal-communication.https.keystore.key", "changeit")
                .put("internal-communication.https.trust-store-path", certPath("truststore.jks"))
                .put("internal-communication.https.trust-store-password", "changeit")
                .build();
    }

    public static Map<String, String> getJwtProperties(String sharedSecret)
    {
        return ImmutableMap.<String, String>builder()
                .put("internal-communication.jwt.enabled", "true")
                .put("internal-communication.shared-secret", sharedSecret)
                .build();
    }

    public static Map<String, String> getFunctionServerMtlsConfig(int port)
    {
        return ImmutableMap.<String, String>builder()
                .put("http-server.http.enabled", "false")
                .put("http-server.https.enabled", "true")
                .put("http-server.https.port", String.valueOf(port))
                .put("http-server.https.keystore.path", certPath("function-server/function-server-keystore.jks"))
                .put("http-server.https.keystore.key", "changeit")
                .put("http-server.https.truststore.path", certPath("truststore.jks"))
                .put("http-server.https.truststore.key", "changeit")
                .build();
    }

    public static Map<String, String> getFnServerMtlsConfigWithInvalidCert(int port)
    {
        String invalidKeystorePath = certPath("function-server/invalid-keystore.jks");

        return ImmutableMap.<String, String>builder()
                .put("http-server.http.enabled", "false")
                .put("http-server.https.enabled", "true")
                .put("http-server.https.port", String.valueOf(port))
                .put("http-server.https.keystore.path", invalidKeystorePath)
                .put("http-server.https.keystore.key", "changeit")
                .put("http-server.https.truststore.path", certPath("truststore.jks"))
                .put("http-server.https.truststore.key", "changeit")
                .build();
    }

    public static Map<String, String> getFunctionServerConfigWithAuth(int port, String sharedSecret, boolean includeJwt)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                .putAll(getFunctionServerMtlsConfig(port));
        if (includeJwt) {
            builder.putAll(getJwtProperties(sharedSecret));
        }
        return builder.build();
    }

    public static Map<String, String> getFunctionServerConfigWithInvalidCert(int port, String sharedSecret)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(getFnServerMtlsConfigWithInvalidCert(port))
                .putAll(getJwtProperties(sharedSecret))
                .build();
    }

    /** Java coordinator + Java workers, mTLS + JWT, valid cert. */
    public static DistributedQueryRunner createRunnerWithValidHttpsFnServer()
            throws Exception
    {
        return createHttpsQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(findUnusedPort(), JWT_SHARED_SECRET, true), true);
    }

    /** Java coordinator + Java workers, mTLS + JWT, wrong JWT secret on Function Server. */
    public static DistributedQueryRunner createRunnerWithInvalidJwtSecretOnFnServer()
            throws Exception
    {
        return createHttpsQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(findUnusedPort(), JWT_WRONG_SECRET, true), true);
    }

    /**
     * Java coordinator + Java workers, mTLS + JWT on coordinator/workers,
     * but Function Server has no JWT config at all.
     */
    public static DistributedQueryRunner createRunnerWithNoJwtOnFnServer()
            throws Exception
    {
        return createHttpsQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(findUnusedPort(), JWT_WRONG_SECRET, false), true);
    }

    /** Java coordinator + Java workers, mTLS only (no JWT on any node). */
    public static DistributedQueryRunner createRunnerWithOnlyMtls()
            throws Exception
    {
        return createHttpsQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(findUnusedPort(), JWT_WRONG_SECRET, false), false);
    }

    /** Java coordinator + Java workers, mTLS + JWT, invalid cert on Function Server. */
    public static DistributedQueryRunner createRunnerWithInvalidCertInFnServer()
            throws Exception
    {
        return createHttpsQueryRunnerWithFnServer(
                getFunctionServerConfigWithInvalidCert(findUnusedPort(), JWT_SHARED_SECRET), true);
    }

    /** Java coordinator + native (C++) workers, mTLS + JWT, valid cert. */
    public static DistributedQueryRunner createNativeRunnerWithValidHttpsFnServer()
            throws Exception
    {
        return createHttpsNativeQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(findUnusedPort(), JWT_SHARED_SECRET, true), true);
    }

    /** Java coordinator + native workers, mTLS + JWT, wrong JWT secret on Function Server. */
    public static DistributedQueryRunner createNativeRunnerWithWrongJwtSecretOnFnServer()
            throws Exception
    {
        return createHttpsNativeQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(findUnusedPort(), JWT_WRONG_SECRET, true), true);
    }

    /**
     * Java coordinator + native workers, mTLS + JWT on coordinator/workers,
     * but Function Server has no JWT config at all.
     */
    public static DistributedQueryRunner createNativeRunnerWithNoJwtOnFnServer()
            throws Exception
    {
        return createHttpsNativeQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(findUnusedPort(), JWT_WRONG_SECRET, false), true);
    }

    /** Java coordinator + native workers, mTLS only (no JWT on any node). */
    public static DistributedQueryRunner createNativeRunnerWithOnlyMtls()
            throws Exception
    {
        return createHttpsNativeQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(findUnusedPort(), JWT_WRONG_SECRET, false), false);
    }

    /** Java coordinator + native workers, mTLS + JWT, invalid cert on Function Server. */
    public static DistributedQueryRunner createNativeRunnerWithInvalidCertInFnServer()
            throws Exception
    {
        return createHttpsNativeQueryRunnerWithFnServer(
                getFunctionServerConfigWithInvalidCert(findUnusedPort(), JWT_SHARED_SECRET), true);
    }

    /**
     * Creates a coordinator-only runner (no workers) for testing
     * coordinator → function-server communication directly.
     */
    public static DistributedQueryRunner createCoordinatorOnlyRunnerWithMtlsAndJwt()
            throws Exception
    {
        return createCoordinatorOnlyHttpsRunnerWithFnServer(
                getFunctionServerConfigWithAuth(findUnusedPort(), JWT_SHARED_SECRET, true));
    }

    /**
     * Returns an external worker launcher for a native (C++) worker configured with
     * HTTPS client certificates and optionally JWT.
     */
    public static Optional<BiFunction<Integer, URI, Process>> getHttpsNativeWorkerLauncher(
            String prestoServerPath,
            String functionServerUri,
            boolean includeJwt)
    {
        String workerCertPath = certPath("worker/worker.crt");
        String workerKeyPath = certPath("worker/worker.key");
        String workerCombinedPemPath = certPath("worker/worker-combined.pem");
        String caCertPath = certPath("ca/ca.crt");
        if (!Files.exists(Paths.get(workerCombinedPemPath))) {
            throw new IllegalStateException(
                    "Worker combined PEM file not found at: " + workerCombinedPemPath);
        }
        PrestoNativeQueryRunnerUtils.HttpsClientConfig httpsConfig = PrestoNativeQueryRunnerUtils.HttpsClientConfig.of(
                workerCertPath, workerKeyPath, workerCombinedPemPath, caCertPath);
        if (includeJwt) {
            httpsConfig = httpsConfig.withJwt(JWT_SHARED_SECRET);
        }
        return PrestoNativeQueryRunnerUtils.externalWorkerLauncherBuilder()
                .setPrestoServerPath(prestoServerPath)
                .setCatalogName("tpch")
                .setConnectorName("tpch")
                .setRemoteFunctionServerRestUrl(functionServerUri)
                .setEnableRuntimeMetricsCollection(true)
                .setHttpsClientConfig(httpsConfig)
                .build();
    }

    /**
     * Helper that installs TpchPlugin, creates tpch catalog, installs
     * FunctionNamespaceManagerPlugin, and loads the REST-based function namespace manager.
     */
    private static void setupTpchAndFunctionNamespace(DistributedQueryRunner queryRunner, String functionServerUri)
    {
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", TPCH_PROPERTIES);
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        queryRunner.loadFunctionNamespaceManager(
                RestBasedFunctionNamespaceManagerFactory.NAME,
                "rest",
                ImmutableMap.of(
                        "supported-function-languages", "JAVA",
                        "function-implementation-type", "REST",
                        "rest-based-function-manager.rest.url", functionServerUri));
    }

    private static TestingFunctionServer startFunctionServer(Map<String, String> functionServerConfig)
    {
        TestingFunctionServer functionServer = new TestingFunctionServer(functionServerConfig);
        log.info("Function Server started at: %s", functionServer.getServerUri());
        return functionServer;
    }

    private static Session defaultTpchSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
    }

    private static Map<String, String> buildCoordinatorProperties(boolean includeJwt)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                .putAll(getCoordinatorMtlsProperties());
        if (includeJwt) {
            builder.putAll(getJwtProperties(JWT_SHARED_SECRET));
        }
        return builder.build();
    }

    /**
     * Creates a Java-worker with HTTPS/mTLS and optional JWT.
     *
     * <p>On any failure during construction both the function server and the query runner are
     * closed via {@code closeAllSuppress} before the exception propagates, guaranteeing no
     * port or thread leaks. On success the function server is registered via
     * addCloseAction so that it is stopped automatically when
     * the runner is eventually closed by the test framework.
     */
    private static DistributedQueryRunner createHttpsQueryRunnerWithFnServer(
            Map<String, String> functionServerConfig,
            boolean includeJwt)
            throws Exception
    {
        log.info("Creating HTTPS Java Query Runner");

        ImmutableMap.Builder<String, String> extraPropertiesBuilder = ImmutableMap.<String, String>builder()
                .putAll(getSharedMtlsProperties());
        if (includeJwt) {
            extraPropertiesBuilder.putAll(getJwtProperties(JWT_SHARED_SECRET));
        }
        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .putAll(buildCoordinatorProperties(includeJwt))
                .put("node-scheduler.include-coordinator", "false")
                .build();

        TestingFunctionServer functionServer = startFunctionServer(functionServerConfig);
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(defaultTpchSession())
                    .setNodeCount(2)
                    .setExtraProperties(extraPropertiesBuilder.build())
                    .setCoordinatorProperties(coordinatorProperties)
                    .build();
            setupTpchAndFunctionNamespace(queryRunner, convertToLocalhostUri(functionServer.getServerUri()));
            queryRunner.addCloseAction(functionServer);
            log.info("HTTPS Java query runner created successfully");
            return queryRunner;
        }
        catch (Exception e) {
            // Close whichever resources were successfully opened before re-throwing.
            closeAllSuppress(e, queryRunner, functionServer);
            throw e;
        }
    }

    /**
     * Creates a native-worker {@link DistributedQueryRunner} with HTTPS/mTLS and optional JWT.
     */
    private static DistributedQueryRunner createHttpsNativeQueryRunnerWithFnServer(
            Map<String, String> functionServerConfig,
            boolean includeJwt)
            throws Exception
    {
        log.info("Creating Native Worker HTTPS Query Runner");

        Path prestoServerPath = Paths.get(System.getProperty("PRESTO_SERVER",
                "_build/debug/presto_cpp/main/presto_server")).toAbsolutePath();
        if (!Files.exists(prestoServerPath)) {
            throw new IllegalStateException(format(
                    "Native worker binary at %s not found. " +
                            "Add -DPRESTO_SERVER=<path/to/presto_server> to your JVM arguments.",
                    prestoServerPath));
        }
        log.info("Using PRESTO_SERVER binary at %s", prestoServerPath);

        TestingFunctionServer functionServer = startFunctionServer(functionServerConfig);
        DistributedQueryRunner queryRunner = null;
        try {
            String functionServerUri = convertToLocalhostUri(functionServer.getServerUri());
            queryRunner = DistributedQueryRunner.builder(defaultTpchSession())
                    .setNodeCount(1)
                    .setExtraProperties(NativeQueryRunnerUtils.getNativeWorkerSystemProperties())
                    .setCoordinatorProperties(buildCoordinatorProperties(includeJwt))
                    .setExternalWorkerLauncher(
                            getHttpsNativeWorkerLauncher(prestoServerPath.toString(), functionServerUri, includeJwt))
                    .build();
            setupTpchAndFunctionNamespace(queryRunner, functionServerUri);
            queryRunner.addCloseAction(functionServer);
            log.info("HTTPS Native query runner created successfully");
            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner, functionServer);
            throw e;
        }
    }

    /**
     * Creates a coordinator-only (no workers) {@link DistributedQueryRunner} with HTTPS/mTLS + JWT.
     */
    private static DistributedQueryRunner createCoordinatorOnlyHttpsRunnerWithFnServer(
            Map<String, String> functionServerConfig)
            throws Exception
    {
        log.info("Creating Coordinator-Only mTLS + JWT Query Runner");

        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .putAll(getCoordinatorMtlsProperties())
                .putAll(getJwtProperties(JWT_SHARED_SECRET))
                .put("node-scheduler.include-coordinator", "true")
                .build();

        TestingFunctionServer functionServer = startFunctionServer(functionServerConfig);
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(defaultTpchSession())
                    .setNodeCount(0)
                    .setCoordinatorProperties(coordinatorProperties)
                    .build();
            setupTpchAndFunctionNamespace(queryRunner, convertToLocalhostUri(functionServer.getServerUri()));
            queryRunner.addCloseAction(functionServer);
            log.info("Coordinator-only query runner created successfully");
            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner, functionServer);
            throw e;
        }
    }
}
