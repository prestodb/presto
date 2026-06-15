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
import com.facebook.presto.common.AuthClientConfigs;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.rest.RestBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.server.TestingFunctionServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

/**
 * Utilities for mTLS + JWT testing with Function Server.
 */
public class FnServerAuthTestUtils
{
    private static final Logger log = Logger.get(FnServerAuthTestUtils.class);
    private static final String CERTS_BASE_PATH = "src/test/resources/certs/";

    public static final String JWT_SHARED_SECRET = "supersecret";
    public static final String JWT_WRONG_SECRET = "wrongsecret";

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

    public static Map<String, String> getSharedMtlsProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("http-server.http.enabled", "true") //true for discovery
                .put("http-server.http.port", "0")
                .put("http-server.https.enabled", "true")
                .put("http-server.https.port", "0")

                .put("http-server.https.keystore.path", CERTS_BASE_PATH + "worker/worker-keystore.jks")
                .put("http-server.https.keystore.key", "changeit")

                .put("http-server.https.truststore.path", CERTS_BASE_PATH + "truststore.jks")
                .put("http-server.https.truststore.key", "changeit")

                .put("internal-communication.https.required", "true")
                .put("internal-communication.https.keystore.path", CERTS_BASE_PATH + "worker/worker-keystore.jks")
                .put("internal-communication.https.keystore.key", "changeit")
                .put("internal-communication.https.trust-store-path", CERTS_BASE_PATH + "truststore.jks")
                .put("internal-communication.https.trust-store-password", "changeit")

                .put("node-scheduler.include-coordinator", "false")
                .build();
    }

    /**
     * Creates coordinator-specific mTLS properties.
     */
    public static Map<String, String> getCoordinatorMtlsProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("http-server.http.enabled", "true") //true for discovery
                .put("http-server.http.port", "0")
                .put("http-server.https.enabled", "true")
                .put("http-server.https.port", "0")

                .put("http-server.https.keystore.path", CERTS_BASE_PATH + "coordinator/coordinator-keystore.jks")
                .put("http-server.https.keystore.key", "changeit")
                .put("http-server.https.truststore.path", CERTS_BASE_PATH + "truststore.jks")
                .put("http-server.https.truststore.key", "changeit")

                .put("internal-communication.https.required", "true")
                .put("internal-communication.https.keystore.path", CERTS_BASE_PATH + "coordinator/coordinator-keystore.jks")
                .put("internal-communication.https.keystore.key", "changeit")
                .put("internal-communication.https.trust-store-path", CERTS_BASE_PATH + "truststore.jks")
                .put("internal-communication.https.trust-store-password", "changeit")

                .put("list-built-in-functions-only", "false")
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
                .put("http-server.https.keystore.path", CERTS_BASE_PATH + "function-server/function-server-keystore.jks")
                .put("http-server.https.keystore.key", "changeit")
                .put("http-server.https.truststore.path", CERTS_BASE_PATH + "truststore.jks")
                .put("http-server.https.truststore.key", "changeit")
                .build();
    }

    public static Map<String, String> getFnServerMtlsConfigWithInvalidCert(int port)
    {
        String invalidKeystorePath = CERTS_BASE_PATH + "function-server/invalid-keystore.jks";

        File invalidKeystore = new File(invalidKeystorePath);
        if (!invalidKeystore.exists()) {
            throw new IllegalStateException(
                    "Invalid keystore not found at: " + invalidKeystorePath + "\n" +
                            "Please run the certificate creation commands first!");
        }

        return ImmutableMap.<String, String>builder()
                .put("http-server.http.enabled", "false")
                .put("http-server.https.enabled", "true")
                .put("http-server.https.port", String.valueOf(port))

                .put("http-server.https.keystore.path", invalidKeystorePath)
                .put("http-server.https.keystore.key", "changeit")

                .put("http-server.https.truststore.path", CERTS_BASE_PATH + "truststore.jks")
                .put("http-server.https.truststore.key", "changeit")
                .build();
    }

    public static Map<String, String> getFunctionServerConfigWithAuth(int port, String sharedSecret, boolean includeJwt)
    {
        ImmutableMap.Builder<String, String> fnServerPropsBuilder = ImmutableMap.<String, String>builder()
                .putAll(getFunctionServerMtlsConfig(port));

        if (includeJwt) {
            fnServerPropsBuilder.putAll(getJwtProperties(sharedSecret));
        }

        return fnServerPropsBuilder.build();
    }

    /**
     * Creates Function Server configuration with invalid keystore.
     */
    public static Map<String, String> getFunctionServerConfigWithInvalidCert(int port, String sharedSecret)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(getFnServerMtlsConfigWithInvalidCert(port))
                .putAll(getJwtProperties(sharedSecret))
                .build();
    }

    public static void loadFunctionNamespaceManager(
            DistributedQueryRunner queryRunner,
            String functionServerUri)
    {
        log.info("Loading function namespace manager with URI: %s", functionServerUri);

        // Load on coordinator
        AuthClientConfigs coordAuthConfigs = queryRunner.getCoordinator().getAuthClientConfigs();

        queryRunner.getCoordinator().getMetadata().getFunctionAndTypeManager()
                .loadFunctionNamespaceManager(
                        RestBasedFunctionNamespaceManagerFactory.NAME,
                        "rest",
                        ImmutableMap.of(
                                "supported-function-languages", "JAVA",
                                "function-implementation-type", "REST",
                                "rest-based-function-manager.rest.url", functionServerUri),
                        queryRunner.getCoordinator().getPluginNodeManager(),
                        coordAuthConfigs);

        // Load on workers
        queryRunner.getServers().stream()
                .filter(server -> !server.isCoordinator())
                .forEach(server -> {
                    try {
                        AuthClientConfigs workerAuthConfigs = server.getAuthClientConfigs();

                        server.getMetadata().getFunctionAndTypeManager()
                                .loadFunctionNamespaceManager(
                                        RestBasedFunctionNamespaceManagerFactory.NAME,
                                        "rest",
                                        ImmutableMap.of(
                                                "supported-function-languages", "JAVA",
                                                "function-implementation-type", "REST",
                                                "rest-based-function-manager.rest.url", functionServerUri),
                                        server.getPluginNodeManager(),
                                        workerAuthConfigs);
                    }
                    catch (Exception e) {
                        throw new RuntimeException("Failed to load function namespace manager on worker", e);
                    }
                });

        log.info("Function namespace manager loaded successfully on all nodes");
    }

    public static DistributedQueryRunner createRunnerWithValidHttpsFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createHttpsQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(functionServerPort, JWT_SHARED_SECRET, true),
                true);
    }

    public static DistributedQueryRunner createRunnerWithInvalidJwtSecretOnFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createHttpsQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(functionServerPort, JWT_WRONG_SECRET, true),
                true);
    }

    /**
     * Create QueryRunner with Function Server that has no JWT in its configuration.
     * Note that only the function-server has no JWT. Coordinator and Worker still has JWT included.
     */
    public static DistributedQueryRunner createRunnerWithNoJwtOnFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createHttpsQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(functionServerPort, JWT_WRONG_SECRET, false),
                true);
    }

    /**
     * Create QueryRunner with Function Server, Coordinator, and Worker where all have mTLS-only configuration set.
     */
    public static DistributedQueryRunner createRunnerWithOnlyMtls()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createHttpsQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(functionServerPort, JWT_WRONG_SECRET, false),
                false);
    }

    public static DistributedQueryRunner createRunnerWithInvalidCertInFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createHttpsQueryRunnerWithFnServer(
                getFunctionServerConfigWithInvalidCert(functionServerPort, JWT_SHARED_SECRET), true);
    }

    public static DistributedQueryRunner createNativeRunnerWithValidHttpsFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createHttpsNativeQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(functionServerPort, JWT_SHARED_SECRET, true), true);
    }

    public static DistributedQueryRunner createNativeRunnerWithWrongJwtSecretOnFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createHttpsNativeQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(functionServerPort, JWT_WRONG_SECRET, true), true);
    }

    public static DistributedQueryRunner createNativeRunnerWithNoJwtOnFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createHttpsNativeQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(functionServerPort, JWT_WRONG_SECRET, false), true);
    }

    public static DistributedQueryRunner createNativeRunnerWithOnlyMtls()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createHttpsNativeQueryRunnerWithFnServer(
                getFunctionServerConfigWithAuth(functionServerPort, JWT_WRONG_SECRET, false), false);
    }

    public static DistributedQueryRunner createNativeRunnerWithInvalidCertInFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createHttpsNativeQueryRunnerWithFnServer(
                getFunctionServerConfigWithInvalidCert(functionServerPort, JWT_SHARED_SECRET), true);
    }

    /**
     * Create QueryRunner with coordinator-only execution (no workers) for testing
     * coordinator→function-server communication directly.
     */
    public static DistributedQueryRunner createCoordinatorOnlyRunnerWithMtlsAndJwt()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createCoordinatorOnlyHttpsRunnerWithFnServer(
                getFunctionServerConfigWithAuth(functionServerPort, JWT_SHARED_SECRET, true));
    }

    /**
     * Get external worker launcher for native workers with HTTPS configuration.
     */
    public static Optional<BiFunction<Integer, URI, Process>> getHttpsNativeWorkerLauncher(
            String prestoServerPath,
            String functionServerUri,
            boolean includeJwt)
    {
        return Optional.of((workerIndex, discoveryUri) -> {
            try {
                Path dir = Paths.get("/tmp", "NativeWorkerTests");
                Files.createDirectories(dir);
                Path tempDirectoryPath = Files.createTempDirectory(dir, "worker");
                log.info("Temp directory for Native Worker #%d: %s", workerIndex, tempDirectoryPath.toString());

                String workerCertPath = Paths.get(CERTS_BASE_PATH + "worker/worker.crt").toAbsolutePath().toString();
                String workerKeyPath = Paths.get(CERTS_BASE_PATH + "worker/worker.key").toAbsolutePath().toString();
                String workerCombinedPemPath = Paths.get(CERTS_BASE_PATH + "worker/worker-combined.pem").toAbsolutePath().toString();
                String caCertPath = Paths.get(CERTS_BASE_PATH + "ca/ca.crt").toAbsolutePath().toString();

                if (!Files.exists(Paths.get(workerCombinedPemPath))) {
                    throw new IllegalStateException(
                            "Worker combined PEM file not found at: " + workerCombinedPemPath + "\n" +
                                    "Please run: cat " + workerCertPath + " " + workerKeyPath + " > " + workerCombinedPemPath);
                }

                String configProperties = format(
                                "discovery.uri=%s%n" +
                                "presto.version=testversion%n" +
                                "http-server.http.port=0%n" +
                                "shutdown-onset-sec=1%n" +
                                "runtime-metrics-collection-enabled=true%n" +
                                "remote-function-server.rest.url=%s%n" +
                                "remote-function-server.catalog-name=rest%n" +
                                "http-server.https.enabled=true%n" +
                                "http-server.http2.enabled=false%n" +
                                "http-server.https.port=0%n" +
                                "https-cert-path=%s%n" +
                                "https-key-path=%s%n" +
                                "https-client-cert-key-path=%s%n" +
                                "https-client-ca-file=%s%n",
                        discoveryUri,
                        functionServerUri,
                        workerCertPath,
                        workerKeyPath,
                        workerCombinedPemPath,
                        caCertPath);

                if (includeJwt) {
                    configProperties = format(
                            "%s%n" +
                            "internal-communication.jwt.enabled=true%n" +
                            "internal-communication.shared-secret=%s%n",
                            configProperties,
                            JWT_SHARED_SECRET);
                }

                Files.write(tempDirectoryPath.resolve("config.properties"), configProperties.getBytes());

                Files.write(tempDirectoryPath.resolve("node.properties"),
                        format("node.id=%s%n" +
                                "node.internal-address=127.0.0.1%n" +
                                "node.environment=testing%n" +
                                "node.location=test-location", UUID.randomUUID()).getBytes());

                Path catalogDirectoryPath = tempDirectoryPath.resolve("catalog");
                Files.createDirectory(catalogDirectoryPath);

                Files.write(
                        catalogDirectoryPath.resolve("tpch.properties"),
                        "connector.name=tpch\n".getBytes());

                Path workerLogFile = tempDirectoryPath.resolve("worker." + workerIndex + ".out");
                log.info("Native Worker #%d log file: %s", workerIndex, workerLogFile.toAbsolutePath());

                Process process = new ProcessBuilder(prestoServerPath, "--logtostderr=1", "--v=1")
                        .directory(tempDirectoryPath.toFile())
                        .redirectErrorStream(true)
                        .redirectOutput(ProcessBuilder.Redirect.to(workerLogFile.toFile()))
                        .redirectError(ProcessBuilder.Redirect.to(workerLogFile.toFile()))
                        .start();

                try {
                    Thread.sleep(10000);
                    if (Files.exists(workerLogFile)) {
                        String output = new String(Files.readAllBytes(workerLogFile));
                        log.info("Native Worker #%d output: %s", workerIndex, output);
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                return process;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    /**
     * Creates coordinator-only query runner with HTTPS.
     */
    private static DistributedQueryRunner createCoordinatorOnlyHttpsRunnerWithFnServer(
            Map<String, String> functionServerConfig)
            throws Exception
    {
        log.info("Creating Coordinator-Only mTLS + JWT Query Runner");

        // Start Function Server
        TestingFunctionServer functionServer = new TestingFunctionServer(functionServerConfig);
        String functionServerUri = convertToLocalhostUri(functionServer.getServerUri());
        log.info("Function Server started at: %s", functionServerUri);

        // Create session
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .putAll(getCoordinatorMtlsProperties())
                .putAll(getJwtProperties(JWT_SHARED_SECRET))
                .put("node-scheduler.include-coordinator", "true")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(0)
                .setCoordinatorProperties(coordinatorProperties)
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        Map<String, String> tpchProperties = ImmutableMap.of(
                "tpch.column-naming", "standard");
        queryRunner.createCatalog("tpch", "tpch", tpchProperties);
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        loadFunctionNamespaceManager(queryRunner, functionServerUri);

        log.info("Coordinator-only query runner created successfully");
        return queryRunner;
    }

    /**
     * Creates query runner with HTTPS.
     */
    private static DistributedQueryRunner createHttpsQueryRunnerWithFnServer(Map<String, String> functionServerConfig, boolean includeJwt)
            throws Exception
    {
        log.info("Creating HTTPS Query Runner");

        // Start Function Server with mTLS + JWT
        TestingFunctionServer functionServer = new TestingFunctionServer(functionServerConfig);
        String functionServerUri = convertToLocalhostUri(functionServer.getServerUri());
        log.info("Function Server started at: %s", functionServerUri);

        // Create session
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        ImmutableMap.Builder<String, String> extraPropertiesBuilder = ImmutableMap.<String, String>builder()
                .putAll(getSharedMtlsProperties());

        if (includeJwt) {
            extraPropertiesBuilder.putAll(getJwtProperties(JWT_SHARED_SECRET));
        }

        Map<String, String> extraProperties = extraPropertiesBuilder.build();

        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .putAll(getCoordinatorMtlsProperties())
                .put("node-scheduler.include-coordinator", "false")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(2)
                .setExtraProperties(extraProperties)
                .setCoordinatorProperties(coordinatorProperties)
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        Map<String, String> tpchProperties = ImmutableMap.of(
                "tpch.column-naming", "standard");
        queryRunner.createCatalog("tpch", "tpch", tpchProperties);
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        loadFunctionNamespaceManager(queryRunner, functionServerUri);

        log.info("HTTPS Query runner created successfully");
        return queryRunner;
    }

    /**
     * Creates native query runner with HTTPS.
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
            throw new IllegalStateException(
                    format("Native worker binary at %s not found. " +
                                    "Add -DPRESTO_SERVER=<path/to/presto_server> to your JVM arguments.",
                            prestoServerPath));
        }
        log.info("Using PRESTO_SERVER binary at %s", prestoServerPath);

        // Start Function Server
        TestingFunctionServer functionServer = new TestingFunctionServer(functionServerConfig);
        String functionServerUri = convertToLocalhostUri(functionServer.getServerUri());
        log.info("Function Server started at: %s", functionServerUri);

        // Create session
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        ImmutableMap.Builder<String, String> coordinatorPropertiesBuilder = ImmutableMap.<String, String>builder()
                .putAll(getCoordinatorMtlsProperties());

        if (includeJwt) {
            coordinatorPropertiesBuilder.putAll(getJwtProperties(JWT_SHARED_SECRET));
        }

        Map<String, String> coordinatorProperties = coordinatorPropertiesBuilder.build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .setExtraProperties(NativeQueryRunnerUtils.getNativeWorkerSystemProperties())
                .setCoordinatorProperties(coordinatorProperties)
                .setExternalWorkerLauncher(
                        getHttpsNativeWorkerLauncher(prestoServerPath.toString(), functionServerUri, includeJwt))
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            Map<String, String> tpchProperties = ImmutableMap.of(
                    "tpch.column-naming", "standard");
            queryRunner.createCatalog("tpch", "tpch", tpchProperties);
            queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
            loadFunctionNamespaceManager(queryRunner, functionServerUri);

            log.info("HTTPS Native query runner created successfully");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
