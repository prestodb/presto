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

package com.facebook.plugin.arrow.tests;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.plugin.arrow.TestingArrowFlightPlugin;
import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class ArrowFlightQueryRunner
{
    private ArrowFlightQueryRunner()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static DistributedQueryRunner createQueryRunner(int flightServerPort) throws Exception
    {
        return createQueryRunner(ImmutableMap.of("arrow-flight.server.port", String.valueOf(flightServerPort)));
    }

    private static DistributedQueryRunner createQueryRunner(Map<String, String> catalogProperties) throws Exception
    {
        return createQueryRunner(ImmutableMap.of(), catalogProperties);
    }

    private static DistributedQueryRunner createQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> catalogProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("arrow")
                .setSchema("tpch")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setExtraProperties(extraProperties).build();

        try {
            queryRunner.installPlugin(new TestingArrowFlightPlugin());

            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .putAll(catalogProperties)
                    .put("arrow-flight.server", "localhost")
                    .put("arrow-flight.server-ssl-enabled", "true")
                    .put("arrow-flight.server-ssl-certificate", "src/test/resources/server.crt")
                    .put("arrow-flight.server.verify", "true");

            queryRunner.createCatalog("arrow", "arrow", properties.build());

            return queryRunner;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create ArrowQueryRunner", e);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Location serverLocation = Location.forGrpcInsecure("127.0.0.1", 9443);
        FlightServer server = FlightServer.builder(allocator, serverLocation, new TestingArrowServer(allocator)).build();

        server.start();

        Logger log = Logger.get(ArrowFlightQueryRunner.class);

        log.info("Server listening on port " + server.getPort());

        DistributedQueryRunner queryRunner = createQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of("arrow-flight.server.port", String.valueOf(9443)));
        Thread.sleep(10);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
