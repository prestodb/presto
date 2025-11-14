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
package com.facebook.plugin.arrow;

import com.facebook.airlift.log.Logger;
import com.facebook.plugin.arrow.testingServer.TestingArrowProducer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;

import static com.facebook.plugin.arrow.ArrowFlightQueryRunner.getProperty;
import static com.facebook.plugin.arrow.testingConnector.TestingArrowFlightPlugin.ARROW_FLIGHT_CATALOG;
import static com.facebook.plugin.arrow.testingConnector.TestingArrowFlightPlugin.ARROW_FLIGHT_CONNECTOR;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestArrowFlightNativeQueries
        extends AbstractTestArrowFlightNativeQueries
{
    private static final Logger log = Logger.get(TestArrowFlightNativeQueries.class);
    private int serverPort;
    private RootAllocator allocator;
    private FlightServer server;
    private DistributedQueryRunner arrowFlightQueryRunner;

    protected boolean ismTLSEnabled()
    {
        return false;
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        arrowFlightQueryRunner = getDistributedQueryRunner();
        allocator = new RootAllocator(Long.MAX_VALUE);
        Location location = Location.forGrpcTls("localhost", serverPort);
        FlightServer.Builder serverBuilder = FlightServer.builder(allocator, location, new TestingArrowProducer(allocator, false));

        File serverCert = new File("src/test/resources/certs/server.crt");
        File serverKey = new File("src/test/resources/certs/server.key");
        serverBuilder.useTls(serverCert, serverKey);

        if (ismTLSEnabled()) {
            File caCert = new File("src/test/resources/certs/ca.crt");
            serverBuilder.useMTlsClientVerification(caCert);
        }

        server = serverBuilder.build();
        server.start();
        log.info("Server listening on port %s (%s)", server.getPort(), ismTLSEnabled() ? "mTLS" : "TLS");
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws InterruptedException
    {
        arrowFlightQueryRunner.close();
        server.close();
        allocator.close();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path prestoServerPath = Paths.get(getProperty("PRESTO_SERVER")
                        .orElse("_build/debug/presto_cpp/main/presto_server"))
                .toAbsolutePath();
        assertTrue(Files.exists(prestoServerPath), format("Native worker binary at %s not found. Add -DPRESTO_SERVER=<path/to/presto_server> to your JVM arguments.", prestoServerPath));
        log.info("Using PRESTO_SERVER binary at %s", prestoServerPath);

        ImmutableMap<String, String> coordinatorProperties = ImmutableMap.of("native-execution-enabled", "true");

        serverPort = ArrowFlightQueryRunner.findUnusedPort();
        return ArrowFlightQueryRunner.createQueryRunner(
                serverPort,
                getNativeWorkerSystemProperties(),
                coordinatorProperties,
                getExternalWorkerLauncher(prestoServerPath.toString(), serverPort, ismTLSEnabled()),
                Optional.of(ismTLSEnabled()));
    }

    @Override
    protected FeaturesConfig createFeaturesConfig()
    {
        return new FeaturesConfig().setNativeExecutionEnabled(true);
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

    public static Optional<BiFunction<Integer, URI, Process>> getExternalWorkerLauncher(String prestoServerPath, int flightServerPort, boolean ismTLSEnabled)
    {
        return Optional.of((workerIndex, discoveryUri) -> {
            try {
                Path dir = Paths.get("/tmp", TestArrowFlightNativeQueries.class.getSimpleName());
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

                String caCertPath = Paths.get("src/test/resources/certs/ca.crt").toAbsolutePath().toString();

                StringBuilder catalogBuilder = new StringBuilder();
                catalogBuilder.append(format(
                        "connector.name=%s\n" +
                                "arrow-flight.server=localhost\n" +
                                "arrow-flight.server.port=%d\n" +
                                "arrow-flight.server-ssl-enabled=true\n" +
                                "arrow-flight.server-ssl-certificate=%s\n",
                        ARROW_FLIGHT_CONNECTOR, flightServerPort, caCertPath));

                if (ismTLSEnabled) {
                    String clientCertPath = Paths.get("src/test/resources/certs/client.crt").toAbsolutePath().toString();
                    String clientKeyPath = Paths.get("src/test/resources/certs/client.key").toAbsolutePath().toString();
                    catalogBuilder.append(format("arrow-flight.client-ssl-certificate=%s\n", clientCertPath));
                    catalogBuilder.append(format("arrow-flight.client-ssl-key=%s\n", clientKeyPath));
                }

                Files.write(
                        catalogDirectoryPath.resolve(format("%s.properties", ARROW_FLIGHT_CATALOG)),
                        catalogBuilder.toString().getBytes());

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
}
