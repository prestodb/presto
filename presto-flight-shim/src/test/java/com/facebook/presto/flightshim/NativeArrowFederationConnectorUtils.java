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
package com.facebook.presto.flightshim;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.memory.BufferAllocator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class NativeArrowFederationConnectorUtils
{
    private static final Logger log = Logger.get(NativeArrowFederationConnectorUtils.class);
    public static final String ARROW_FEDERATION_CONNECTOR = "arrow-federation";

    private NativeArrowFederationConnectorUtils()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static int findUnusedPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    // todo: fix this hack
    // Not able to run native e2e queries when SslEnabled
    public static Map<String, String> getFlightServerShimConfig(String pluginBundles, boolean isSslEnabled)
            throws IOException
    {
        ImmutableMap.Builder<String, String> configBuilder = ImmutableMap.builder();
        configBuilder.put("flight-shim.server", "localhost");
        configBuilder.put("flight-shim.server.port", String.valueOf(findUnusedPort()));
        configBuilder.put("flight-shim.server-ssl-enabled", String.valueOf(isSslEnabled));
        if (isSslEnabled) {
            configBuilder.put("flight-shim.server-ssl-certificate-file", "src/test/resources/certs/server.crt");
            configBuilder.put("flight-shim.server-ssl-key-file", "src/test/resources/certs/server.key");
        }
        configBuilder.put("plugin.bundles", pluginBundles);

        // Allow for 3 batches using testing tpch db
        configBuilder.put("flight-shim.max-rows-per-batch", String.valueOf(500));
        return configBuilder.build();
    }

    public static Optional<String> getProperty(String name)
    {
        String systemPropertyValue = System.getProperty(name);
        if (systemPropertyValue != null) {
            return Optional.of(systemPropertyValue);
        }
        String environmentVariableValue = System.getenv(name);
        if (environmentVariableValue != null) {
            return Optional.of(environmentVariableValue);
        }
        return Optional.empty();
    }

    public static Map<String, String> getNativeWorkerSystemProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("native-execution-enabled", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("regex-library", "RE2J")
                .put("offset-clause-enabled", "true")
                // By default, Presto will expand some functions into its SQL equivalent (e.g. array_duplicates()).
                // With Velox, we do not want Presto to replace the function with its SQL equivalent.
                // To achieve that, we set inline-sql-functions to false.
                .put("inline-sql-functions", "false")
                .put("use-alternative-function-signatures", "true")
                .build();
    }

    public static Optional<BiFunction<Integer, URI, Process>> getExternalWorkerLauncher(String prestoServerPath, int flightServerPort, List<String> connectorIds)
    {
        return Optional.of((workerIndex, discoveryUri) -> {
            try {
                Path dir = Paths.get("/tmp", NativeArrowFederationConnectorUtils.class.getSimpleName());
                Files.createDirectories(dir);
                Path tempDirectoryPath = Files.createTempDirectory(dir, "worker");
                log.info("Temp directory for Worker #%d: %s", workerIndex, tempDirectoryPath.toString());

                // Write config file - use an ephemeral port for the worker.
                String configProperties = format("discovery.uri=%s%n" +
                        "presto.version=testversion%n" +
                        "system-memory-gb=4%n" +
                        "http-server.http.port=0%n", discoveryUri);

                Files.write(tempDirectoryPath.resolve("config.properties"), configProperties.getBytes());
                Files.write(tempDirectoryPath.resolve("node.properties"),
                        format("node.id=%s%n" +
                                "node.internal-address=127.0.0.1%n" +
                                "node.environment=testing%n" +
                                "node.location=test-location", UUID.randomUUID()).getBytes());

                Path catalogDirectoryPath = tempDirectoryPath.resolve("catalog");
                Files.createDirectory(catalogDirectoryPath);

                String caCertPath = Paths.get("src/test/resources/mtls/ca.crt").toAbsolutePath().toString();

                for (String connectorId : connectorIds) {
                    String catalogBuilder = format(
                            "connector.name=%s\n" +
                                    "protocol-connector.id=%s\n" +
                                    "arrow-flight.server=localhost\n" +
                                    "arrow-flight.server.port=%d\n",
//                                "arrow-flight.server-ssl-enabled=true\n" +
//                                "arrow-flight.server-ssl-certificate=%s\n",
                            ARROW_FEDERATION_CONNECTOR, connectorId, flightServerPort);
                    Files.write(
                            catalogDirectoryPath.resolve(format("%s.properties", connectorId)),
                            catalogBuilder.getBytes());
                }

                // Add a tpch catalog.
                Files.write(catalogDirectoryPath.resolve("tpchstandard.properties"),
                        format("connector.name=tpch%n").getBytes());

                return new ProcessBuilder(prestoServerPath, "--logtostderr=1", "--v=1")
                        .directory(tempDirectoryPath.toFile())
                        .redirectErrorStream(true)
                        .redirectOutput(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("worker." + workerIndex + ".out").toFile()))
                        .redirectError(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("worker." + workerIndex + ".out").toFile()))
                        .start();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public static QueryRunner createNativeQueryRunner(List<String> connectorIds, int port)
            throws Exception
    {
        Path prestoServerPath = Paths.get(getProperty("PRESTO_SERVER")
                        .orElse("_build/debug/presto_cpp/main/presto_server"))
                .toAbsolutePath();
        assertTrue(Files.exists(prestoServerPath), format("Native worker binary at %s not found. Add -DPRESTO_SERVER=<path/to/presto_server> to your JVM arguments.", prestoServerPath));
        log.info("Using PRESTO_SERVER binary at %s", prestoServerPath);

        checkArgument(!connectorIds.isEmpty());
        Session session = testSessionBuilder()
                .setCatalog(connectorIds.get(0))
                .setSchema("tpch")
                .build();

        DistributedQueryRunner.Builder queryRunnerBuilder = DistributedQueryRunner.builder(session);
        Optional<Integer> workerCount = getProperty("WORKER_COUNT").map(Integer::parseInt);
        workerCount.ifPresent(queryRunnerBuilder::setNodeCount);

        DistributedQueryRunner queryRunner = queryRunnerBuilder
                .setExtraProperties(getNativeWorkerSystemProperties())
                .setExternalWorkerLauncher(
                        getExternalWorkerLauncher(prestoServerPath.toString(), port, connectorIds))
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            Map<String, String> tpchProperties = ImmutableMap.<String, String>builder()
                    .put("tpch.column-naming", "standard")
                    .build();
            queryRunner.createCatalog("tpch", "tpch");
            queryRunner.createCatalog("tpchstandard", "tpch", tpchProperties);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static QueryRunner createJavaQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static FlightServer setUpFlightServer(
            Map<String, String> connectorIdAndUrls,
            String pluginBundles,
            List<AutoCloseable> closables)
            throws Exception
    {
        Injector injector = FlightShimServer.initialize(getFlightServerShimConfig(pluginBundles, false));

        FlightServer server = FlightShimServer.start(injector, FlightServer.builder());
        closables.add(server);

        // Set test properties after catalogs have been loaded
        FlightShimPluginManager pluginManager = injector.getInstance(FlightShimPluginManager.class);
        connectorIdAndUrls.forEach((connectorId, connectorUrl) -> {
            pluginManager.setCatalogProperties(connectorId, connectorId, getConnectorProperties(connectorUrl));
        });

        // Make sure these resources close properly
        closables.add(injector.getInstance(BufferAllocator.class));
        closables.add(injector.getInstance(FlightShimProducer.class));
        return server;
    }

    public static Map<String, String> getConnectorProperties(String jdbcUrl)
    {
        Map<String, String> connectorProperties = new HashMap<>();
        connectorProperties.putIfAbsent("connection-url", jdbcUrl);
        connectorProperties.putIfAbsent("connection-user", "testuser");
        connectorProperties.putIfAbsent("connection-password", "testpass");
        connectorProperties.putIfAbsent("allow-drop-table", "true");
        return ImmutableMap.copyOf(connectorProperties);
    }
}
