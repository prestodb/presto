/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.facebook.presto.tests.utils;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.AuthClientConfigs;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.rest.RestBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.server.TestingFunctionServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.net.URI;
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

    // JWT Configuration
    public static final String JWT_SHARED_SECRET = "supersecret";
    public static final String JWT_WRONG_SECRET = "wrongsecret";

    /**
     * Converts IP-based URI to localhost for certificate validation.
     */
    public static String convertToLocalhostUri(String uri)
    {
        String localhostUri = uri.replaceFirst("://\\d+\\.\\d+\\.\\d+\\.\\d+:", "://localhost:");
        if (!localhostUri.equals(uri)) {
            log.info("Converted URI: %s -> %s", uri, localhostUri);
        }
        return localhostUri;
    }

    /**
     * Find an unused port for testing.
     */
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
     * Creates shared HTTPS properties for all nodes (coordinator + workers).
     */
    public static Map<String, String> getSharedHttpsMtlsProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("http-server.http.enabled", "false")
//                .put("http-server.http.port", "0") // Ephemeral port
                .put("http-server.https.enabled", "true")
                .put("http-server.https.port", "0") // Ephemeral port

                // Worker HTTPS config (coordinator overrides with its own keystore)
                .put("http-server.https.keystore.path", CERTS_BASE_PATH + "worker/worker-keystore.jks")
                .put("http-server.https.keystore.key", "changeit")

                // Worker mTLS
                .put("http-server.https.truststore.path", CERTS_BASE_PATH + "truststore.jks")
                .put("http-server.https.truststore.key", "changeit")

                // Force internal communication to use HTTPS
                .put("internal-communication.https.required", "true")
                .put("internal-communication.https.keystore.path", CERTS_BASE_PATH + "worker/worker-keystore.jks")
                .put("internal-communication.https.keystore.key", "changeit")
                .put("internal-communication.https.trust-store-path", CERTS_BASE_PATH + "truststore.jks")
                .put("internal-communication.https.trust-store-password", "changeit")

                .put("node-scheduler.include-coordinator", "false")
                .build();
    }

    /**
     * Creates coordinator-specific HTTPS + mTLS properties.
     */
    public static Map<String, String> getCoordinatorHttpsMtlsProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("http-server.https.keystore.path",
                        CERTS_BASE_PATH + "coordinator/coordinator-keystore.jks")
                .put("http-server.https.keystore.key", "changeit")
                .put("http-server.https.truststore.path",
                        CERTS_BASE_PATH + "truststore.jks")
                .put("http-server.https.truststore.key", "changeit")

                .put("internal-communication.https.required", "true")
                .put("internal-communication.https.keystore.path",
                        CERTS_BASE_PATH + "coordinator/coordinator-keystore.jks")
                .put("internal-communication.https.keystore.key", "changeit")
                .put("internal-communication.https.trust-store-path",
                        CERTS_BASE_PATH + "truststore.jks")
                .put("internal-communication.https.trust-store-password", "changeit")

                .put("node-scheduler.include-coordinator", "false")
                .put("list-built-in-functions-only", "false")
                .build();
    }

    /**
     * Creates JWT properties with specified shared secret.
     */
    public static Map<String, String> getJwtProperties(String sharedSecret)
    {
        return ImmutableMap.<String, String>builder()
                .put("internal-communication.jwt.enabled", "true")
                .put("internal-communication.shared-secret", sharedSecret)
                .build();
    }

    /**
     * Creates JWT properties with default shared secret.
     */
    public static Map<String, String> getJwtProperties()
    {
        return getJwtProperties(JWT_SHARED_SECRET);
    }

    /**
     * Creates Function Server mTLS configuration.
     */
    public static Map<String, String> getFunctionServerMtlsConfig(int port)
    {
        return ImmutableMap.<String, String>builder()
                .put("http-server.http.enabled", "false")
                .put("http-server.https.enabled", "true")
                .put("http-server.https.port", String.valueOf(port))
                .put("http-server.https.keystore.path",
                        CERTS_BASE_PATH + "function-server/function-server-keystore.jks")
                .put("http-server.https.keystore.key", "changeit")
                .put("http-server.https.truststore.path",
                        CERTS_BASE_PATH + "truststore.jks")
                .put("http-server.https.truststore.key", "changeit")
                .build();
    }

    /**
     * Get function server configuration with INVALID certificate (not in coordinator/worker truststore).
     * This is used for negative testing.
     */
    public static Map<String, String> getFnServerMtlsConfigWithInvalidCert(int port)
    {
        String invalidKeystorePath = CERTS_BASE_PATH + "invalid-keystore.jks";

        // Verify invalid keystore exists
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

                // Use INVALID keystore (certificate not in coordinator/worker truststore)
                .put("http-server.https.keystore.path", invalidKeystorePath)
                .put("http-server.https.keystore.key", "changeit")

                .put("http-server.https.truststore.path", CERTS_BASE_PATH + "truststore.jks")
                .put("http-server.https.truststore.key", "changeit")
                .build();
    }

    /**
     * Creates Function Server mTLS + JWT configuration.
     */
    public static Map<String, String> getFunctionServerMtlsAndJwtConfig(int port, String sharedSecret)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(getFunctionServerMtlsConfig(port))
                .putAll(getJwtProperties(sharedSecret))
                .build();
    }

    /**
     * Creates Function Server configuration without JWT.
     */
    public static Map<String, String> getFunctionServerConfigWithoutJwt(int port)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(getFunctionServerMtlsConfig(port))
                .build();
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

    /**
     * Loads function namespace manager on coordinator and workers.
     */
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

    /**
     * Create QueryRunner with Function Server that includes valid mTLS and JWT configuration.
     */
    public static DistributedQueryRunner createRunnerWithValidHttpsFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createQueryRunnerWithMtlsAndJwt(
                getFunctionServerMtlsAndJwtConfig(functionServerPort, JWT_SHARED_SECRET));
    }

    /**
     * Create QueryRunner with Function Server that includes mTLS and invalid JWT secret.
     */
    public static DistributedQueryRunner createRunnerWithInvalidJwtSecretOnFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createQueryRunnerWithMtlsAndJwt(
                getFunctionServerMtlsAndJwtConfig(functionServerPort, JWT_WRONG_SECRET));
    }

    /**
     * Create QueryRunner with Function Server that has no JWT in its configuration.
     * Note that only the function-server has no JWT. Coordinator and Worker still has JWT included.
     */
    public static DistributedQueryRunner createRunnerWithNoJwtOnFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createQueryRunnerWithMtlsAndJwt(
                getFunctionServerConfigWithoutJwt(functionServerPort));
    }

    /**
     * Create QueryRunner with Function Server, Coordinator, and Worker where all have mTLS-only configuration set.
     */
    public static DistributedQueryRunner createRunnerWithOnlyMtls()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createQueryRunnerWithOnlyMtls(
                getFunctionServerConfigWithoutJwt(functionServerPort));
    }

    /**
     * Create QueryRunner with Function Server that has INVALID certificate.
     */
    public static DistributedQueryRunner createRunnerWithInvalidCertInFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createQueryRunnerWithMtlsAndJwt(
                getFunctionServerConfigWithInvalidCert(functionServerPort, JWT_SHARED_SECRET));
    }

    /**
     * Create QueryRunner with Native Workers and Function Server with valid mTLS and JWT.
     */
    public static DistributedQueryRunner createNativeRunnerWithValidHttpsFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createNativeQueryRunnerWithMtlsAndJwt(
                getFunctionServerMtlsAndJwtConfig(functionServerPort, JWT_SHARED_SECRET));
    }

    /**
     * Create QueryRunner with Native Workers and Function Server with invalid JWT secret.
     */
    public static DistributedQueryRunner createNativeRunnerWithInvalidJwtSecretOnFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createNativeQueryRunnerWithMtlsAndJwt(
                getFunctionServerMtlsAndJwtConfig(functionServerPort, JWT_WRONG_SECRET));
    }

    /**
     * Create QueryRunner with Native Workers and Function Server without JWT.
     */
    public static DistributedQueryRunner createNativeRunnerWithNoJwtOnFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createNativeQueryRunnerWithMtlsAndJwt(
                getFunctionServerConfigWithoutJwt(functionServerPort));
    }

    /**
     * Create QueryRunner with Native Workers with only mTLS configuration.
     */
    public static DistributedQueryRunner createNativeRunnerWithOnlyMtls()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createNativeQueryRunnerWithOnlyMtls(
                getFunctionServerConfigWithoutJwt(functionServerPort));
    }

    /**
     * Create QueryRunner with Native Workers and Function Server with invalid certificate.
     */
    public static DistributedQueryRunner createNativeRunnerWithInvalidCertInFnServer()
            throws Exception
    {
        int functionServerPort = findUnusedPort();
        return createNativeQueryRunnerWithMtlsAndJwt(
                getFunctionServerConfigWithInvalidCert(functionServerPort, JWT_SHARED_SECRET));
    }

    /**
     * Get external worker launcher for native (C++) workers with mTLS and JWT configuration.
     */
    public static Optional<BiFunction<Integer, URI, Process>> getNativeWorkerLauncher(
            String prestoServerPath,
            String functionServerUri)
    {
        return Optional.of((workerIndex, discoveryUri) -> {
            try {
                Path dir = Paths.get("/tmp", "NativeWorkerTests");
                Files.createDirectories(dir);
                Path tempDirectoryPath = Files.createTempDirectory(dir, "worker");
                log.info("Temp directory for Native Worker #%d: %s", workerIndex, tempDirectoryPath.toString());

//                String httpsDiscoveryUri = discoveryUri.toString().replace("http://", "https://");

                // Get absolute paths for certificates
                String workerCertPath = Paths.get(CERTS_BASE_PATH + "worker/worker.crt").toAbsolutePath().toString();
                String workerKeyPath = Paths.get(CERTS_BASE_PATH + "worker/worker.key").toAbsolutePath().toString();
                String workerCombinedPemPath = Paths.get(CERTS_BASE_PATH + "worker/worker-combined.pem").toAbsolutePath().toString();
                String caCertPath = Paths.get(CERTS_BASE_PATH + "ca/ca.crt").toAbsolutePath().toString();

                // Verify certificate files exist
                if (!Files.exists(Paths.get(workerCombinedPemPath))) {
                    throw new IllegalStateException(
                            "Worker combined PEM file not found at: " + workerCombinedPemPath + "\n" +
                                    "Please run: cat " + workerCertPath + " " + workerKeyPath + " > " + workerCombinedPemPath);
                }

                // Write config.properties with native worker configuration
                String configProperties = format(
                        "discovery.uri=%s%n" +
                                "presto.version=testversion%n" +
                                "http-server.http.port=0%n" +
                                "shutdown-onset-sec=1%n" +
                                "runtime-metrics-collection-enabled=true%n" +
                                "remote-function-server.rest.url=%s%n" +
                                "remote-function-server.catalog-name=rest%n" +
//                                "http-server.http.enabled=true%n" +
//                                "http-server.https.enabled=true%n" +
//                                "http-server.http2.enabled=false%n" +
//                                "http-server.https.port=7443%n" +
//                                "https-cert-path=%s%n" +
//                                "https-key-path=%s%n" +
                                "https-client-cert-key-path=%s%n" +
                                "https-client-ca-file=%s%n",
//                                "internal-communication.jwt.enabled=true%n" +
//                                "internal-communication.shared-secret=%s%n",
                        discoveryUri,
                        functionServerUri,
//                        workerCertPath,
//                        workerKeyPath,
                        workerCombinedPemPath,
                        caCertPath
//                        JWT_SHARED_SECRET
                );

                Files.write(tempDirectoryPath.resolve("config.properties"), configProperties.getBytes());

                // Write node.properties
                Files.write(tempDirectoryPath.resolve("node.properties"),
                        format("node.id=%s%n" +
                                "node.internal-address=127.0.0.1%n" +
                                "node.environment=testing%n" +
                                "node.location=test-location", UUID.randomUUID()).getBytes());

                // Create catalog directory and add tpch catalog
                Path catalogDirectoryPath = tempDirectoryPath.resolve("catalog");
                Files.createDirectory(catalogDirectoryPath);
                Files.write(catalogDirectoryPath.resolve("tpchstandard.properties"),
                        format("connector.name=tpch%n").getBytes());

                Path workerLogFile = tempDirectoryPath.resolve("worker." + workerIndex + ".out");
                log.info("Native Worker #%d log file: %s", workerIndex, workerLogFile.toAbsolutePath());

                Process process = new ProcessBuilder(prestoServerPath, "--logtostderr=1", "--v=1")
                        .directory(tempDirectoryPath.toFile())
                        .redirectErrorStream(true)
                        .redirectOutput(ProcessBuilder.Redirect.to(workerLogFile.toFile()))
                        .redirectError(ProcessBuilder.Redirect.to(workerLogFile.toFile()))
                        .start();

                // Log if process starts successfully
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
     * Creates query runner with mTLS + JWT.
     */
    private static DistributedQueryRunner createQueryRunnerWithMtlsAndJwt(Map<String, String> functionServerConfig)
            throws Exception
    {
        log.info("Creating mTLS + JWT Query Runner");

        // Start Function Server with mTLS + JWT
        TestingFunctionServer functionServer = new TestingFunctionServer(functionServerConfig);
        String functionServerUri = convertToLocalhostUri(functionServer.getServerUri());
        log.info("Function Server started at: %s", functionServerUri);

        // Create session
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        // Build query runner with shared and coordinator-specific properties
        Map<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .putAll(getSharedHttpsMtlsProperties())
                .putAll(getJwtProperties(JWT_SHARED_SECRET))
                .build();

        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .putAll(getCoordinatorHttpsMtlsProperties())
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(2)
                .setExtraProperties(extraProperties) // Worker
                .setCoordinatorProperties(coordinatorProperties)  // Coordinator overrides
                .build();

        // Load function namespace manager on coordinator and workers
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        loadFunctionNamespaceManager(queryRunner, functionServerUri);

        log.info("Query runner created successfully with mTLS + JWT");
        return queryRunner;
    }

    /**
     * Creates query runner with only mTLS configuration on Function-Server, Coordinator, and Worker nodes.
     */
    private static DistributedQueryRunner createQueryRunnerWithOnlyMtls(Map<String, String> functionServerConfig)
            throws Exception
    {
        log.info("Creating only mTLS Query Runner");

        // Start Function Server with only mTLS
        TestingFunctionServer functionServer = new TestingFunctionServer(functionServerConfig);
        String functionServerUri = convertToLocalhostUri(functionServer.getServerUri());
        log.info("Function Server started at: %s", functionServerUri);

        // Create session
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        // Build query runner with shared and coordinator-specific properties
        Map<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .putAll(getSharedHttpsMtlsProperties())
                .build();

        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .putAll(getCoordinatorHttpsMtlsProperties())
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(2)
                .setExtraProperties(extraProperties)
                .setCoordinatorProperties(coordinatorProperties)  // Coordinator overrides
                .build();

        // Load function namespace manager on coordinator and workers
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        loadFunctionNamespaceManager(queryRunner, functionServerUri);

        log.info("Query runner created successfully with only mTLS");
        return queryRunner;
    }

    /**
     * Creates native query runner with mTLS + JWT.
     */
    private static DistributedQueryRunner createNativeQueryRunnerWithMtlsAndJwt(
            Map<String, String> functionServerConfig)
            throws Exception
    {
        log.info("Creating Native Worker mTLS + JWT Query Runner");

        // Get native worker binary path
        Path prestoServerPath = Paths.get(System.getProperty("PRESTO_SERVER",
                "_build/debug/presto_cpp/main/presto_server")).toAbsolutePath();

        if (!Files.exists(prestoServerPath)) {
            throw new IllegalStateException(
                    format("Native worker binary at %s not found. " +
                                    "Add -DPRESTO_SERVER=<path/to/presto_server> to your JVM arguments.",
                            prestoServerPath));
        }
        log.info("Using PRESTO_SERVER binary at %s", prestoServerPath);

        // Start Function Server with mTLS + JWT
        TestingFunctionServer functionServer = new TestingFunctionServer(functionServerConfig);
        String functionServerUri = convertToLocalhostUri(functionServer.getServerUri());
        log.info("Function Server started at: %s", functionServerUri);

        // Create session
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        // Build query runner with coordinator properties and native worker properties
        Map<String, String> extraProperties = ImmutableMap.<String, String>builder()
//                .putAll(getSharedHttpsMtlsProperties())
//                .putAll(getJwtProperties(JWT_SHARED_SECRET))
                .putAll(NativeQueryRunnerUtils.getNativeWorkerSystemProperties())
                .build();

        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .put("http-server.http.enabled", "true")
                .put("http-server.http.port", "8080") // Ephemeral port
//                .put("http-server.https.enabled", "true")
//                .put("http-server.https.port", "8443")
//                .putAll(getCoordinatorHttpsMtlsProperties())
//                .putAll(getJwtProperties(JWT_SHARED_SECRET))
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
//                .setExtraProperties(extraProperties)
//                .setCoordinatorProperties(coordinatorProperties)
                .setExternalWorkerLauncher(
                        getNativeWorkerLauncher(prestoServerPath.toString(), functionServerUri))
                .build();

        // Load function namespace manager on coordinator
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        loadFunctionNamespaceManager(queryRunner, functionServerUri);

        log.info("Native query runner created successfully with mTLS + JWT");
        return queryRunner;
    }

    /**
     * Creates native query runner with only mTLS.
     */
    private static DistributedQueryRunner createNativeQueryRunnerWithOnlyMtls(
            Map<String, String> functionServerConfig)
            throws Exception
    {
        log.info("Creating Native Worker only mTLS Query Runner");

        // Get native worker binary path
        Path prestoServerPath = Paths.get(System.getProperty("PRESTO_SERVER",
                "_build/debug/presto_cpp/main/presto_server")).toAbsolutePath();

        if (!Files.exists(prestoServerPath)) {
            throw new IllegalStateException(
                    format("Native worker binary at %s not found.",
                            prestoServerPath));
        }
        log.info("Using PRESTO_SERVER binary at %s", prestoServerPath);

        // Start Function Server with only mTLS
        TestingFunctionServer functionServer = new TestingFunctionServer(functionServerConfig);
        String functionServerUri = convertToLocalhostUri(functionServer.getServerUri());
        log.info("Function Server started at: %s", functionServerUri);

        // Create session
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        // Build query runner with only mTLS (no JWT)
        Map<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .putAll(getSharedHttpsMtlsProperties())
                .putAll(NativeQueryRunnerUtils.getNativeWorkerSystemProperties())
                .build();

        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .putAll(getCoordinatorHttpsMtlsProperties())
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .setExtraProperties(extraProperties)
                .setCoordinatorProperties(coordinatorProperties)
                .setExternalWorkerLauncher(
                        getNativeWorkerLauncher(prestoServerPath.toString(), functionServerUri))
                .build();

        // Load function namespace manager on coordinator
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        loadFunctionNamespaceManager(queryRunner, functionServerUri);

        log.info("Native query runner created successfully with only mTLS");
        return queryRunner;
    }
}