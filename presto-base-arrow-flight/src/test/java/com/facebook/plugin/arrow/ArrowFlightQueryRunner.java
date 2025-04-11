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
import com.facebook.airlift.log.Logging;
import com.facebook.plugin.arrow.testingConnector.TestingArrowFlightPlugin;
import com.facebook.plugin.arrow.testingServer.TestingArrowProducer;
import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.facebook.plugin.arrow.testingConnector.TestingArrowFlightPlugin.ARROW_FLIGHT_CATALOG;
import static com.facebook.plugin.arrow.testingConnector.TestingArrowFlightPlugin.ARROW_FLIGHT_CONNECTOR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class ArrowFlightQueryRunner
{
    private ArrowFlightQueryRunner()
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

    public static DistributedQueryRunner createQueryRunner(int flightServerPort) throws Exception
    {
        return createQueryRunner(flightServerPort, ImmutableMap.of(), ImmutableMap.of(), Optional.empty());
    }

    public static DistributedQueryRunner createQueryRunner(
            int flightServerPort,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher)
            throws Exception
    {
        return createQueryRunner(extraProperties, ImmutableMap.of("arrow-flight.server.port", String.valueOf(flightServerPort)), coordinatorProperties, externalWorkerLauncher);
    }

    private static DistributedQueryRunner createQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> catalogProperties,
            Map<String, String> coordinatorProperties,
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ARROW_FLIGHT_CATALOG)
                .setSchema("tpch")
                .build();

        DistributedQueryRunner.Builder queryRunnerBuilder = DistributedQueryRunner.builder(session);
        Optional<Integer> workerCount = getProperty("WORKER_COUNT").map(Integer::parseInt);
        workerCount.ifPresent(queryRunnerBuilder::setNodeCount);

        DistributedQueryRunner queryRunner = queryRunnerBuilder
                .setExtraProperties(extraProperties)
                .setCoordinatorProperties(coordinatorProperties)
                .setExternalWorkerLauncher(externalWorkerLauncher).build();

        try {
            boolean nativeExecution = externalWorkerLauncher.isPresent();
            queryRunner.installPlugin(new TestingArrowFlightPlugin(nativeExecution));

            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .putAll(catalogProperties)
                    .put("arrow-flight.server", "localhost")
                    .put("arrow-flight.server-ssl-enabled", "true")
                    .put("arrow-flight.server-ssl-certificate", "src/test/resources/server.crt");

            queryRunner.createCatalog(ARROW_FLIGHT_CATALOG, ARROW_FLIGHT_CONNECTOR, properties.build());

            return queryRunner;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create ArrowFlightQueryRunner", e);
        }
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

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Location serverLocation = Location.forGrpcTls("localhost", 9443);
        File certChainFile = new File("src/test/resources/server.crt");
        File privateKeyFile = new File("src/test/resources/server.key");
        FlightServer server = FlightServer.builder(allocator, serverLocation, new TestingArrowProducer(allocator))
                .useTls(certChainFile, privateKeyFile)
                .build();

        server.start();

        Logger log = Logger.get(ArrowFlightQueryRunner.class);

        log.info("Server listening on port " + server.getPort());

        DistributedQueryRunner queryRunner = createQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of("arrow-flight.server.port", String.valueOf(9443)),
                ImmutableMap.of(),
                Optional.empty());
        Thread.sleep(10);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
