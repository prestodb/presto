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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.facebook.plugin.arrow.testingConnector.TestingArrowFlightPlugin.ARROW_FLIGHT_CONNECTOR;

public class TestArrowFlightMtls
        extends AbstractTestQueryFramework
{
    private static final Logger logger = Logger.get(TestArrowFlightMtls.class);
    private final int serverPort;
    private RootAllocator allocator;
    private FlightServer server;
    private DistributedQueryRunner arrowFlightQueryRunner;
    private static final String ARROW_FLIGHT_CATALOG_WITH_INVALID_CERT = "arrow_catalog_with_invalid_cert";
    private static final String ARROW_FLIGHT_CATALOG_WITH_NO_MTLS_CERTS = "arrow_catalog_with_no_mtls_certs";
    private static final String ARROW_FLIGHT_CATALOG_WITH_MTLS_CERTS = "arrow_catalog_with_mtls_certs";

    public TestArrowFlightMtls()
            throws IOException
    {
        this.serverPort = ArrowFlightQueryRunner.findUnusedPort();
    }

    @BeforeClass
    private void setup()
            throws Exception
    {
        arrowFlightQueryRunner = getDistributedQueryRunner();
        arrowFlightQueryRunner.createCatalog(ARROW_FLIGHT_CATALOG_WITH_INVALID_CERT, ARROW_FLIGHT_CONNECTOR, getInvalidCertCatalogProperties());
        arrowFlightQueryRunner.createCatalog(ARROW_FLIGHT_CATALOG_WITH_NO_MTLS_CERTS, ARROW_FLIGHT_CONNECTOR, getNoMtlsCatalogProperties());
        arrowFlightQueryRunner.createCatalog(ARROW_FLIGHT_CATALOG_WITH_MTLS_CERTS, ARROW_FLIGHT_CONNECTOR, getMtlsCatalogProperties());

        File certChainFile = new File("src/test/resources/certs/server.crt");
        File privateKeyFile = new File("src/test/resources/certs/server.key");
        File caCertFile = new File("src/test/resources/certs/ca.crt");

        allocator = new RootAllocator(Long.MAX_VALUE);

        Location location = Location.forGrpcTls("localhost", serverPort);
        server = FlightServer.builder(allocator, location, new TestingArrowProducer(allocator))
                .useTls(certChainFile, privateKeyFile)
                .useMTlsClientVerification(caCertFile)
                .build();

        server.start();
        logger.info("Server listening on port %s", server.getPort());
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
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
        return ArrowFlightQueryRunner.createQueryRunner(serverPort, ImmutableMap.of(), ImmutableMap.of(), Optional.empty(), Optional.empty());
    }

    private Map<String, String> getInvalidCertCatalogProperties()
    {
        ImmutableMap.Builder<String, String> catalogProperties = ImmutableMap.<String, String>builder()
                .put("arrow-flight.server.port", String.valueOf(serverPort))
                .put("arrow-flight.server", "localhost")
                .put("arrow-flight.server-ssl-enabled", "true")
                .put("arrow-flight.server-ssl-certificate", "src/test/resources/certs/server.crt")
                .put("arrow-flight.client-ssl-certificate", "src/test/resources/certs/invalid_cert.crt")
                .put("arrow-flight.client-ssl-key", "src/test/resources/certs/client.key");
        return catalogProperties.build();
    }

    private Map<String, String> getNoMtlsCatalogProperties()
    {
        ImmutableMap.Builder<String, String> catalogProperties = ImmutableMap.<String, String>builder()
                .put("arrow-flight.server.port", String.valueOf(serverPort))
                .put("arrow-flight.server", "localhost")
                .put("arrow-flight.server-ssl-enabled", "true")
                .put("arrow-flight.server-ssl-certificate", "src/test/resources/certs/server.crt");
        return catalogProperties.build();
    }

    private Map<String, String> getMtlsCatalogProperties()
    {
        ImmutableMap.Builder<String, String> catalogProperties = ImmutableMap.<String, String>builder()
                .put("arrow-flight.server.port", String.valueOf(serverPort))
                .put("arrow-flight.server", "localhost")
                .put("arrow-flight.server-ssl-enabled", "true")
                .put("arrow-flight.server-ssl-certificate", "src/test/resources/certs/server.crt")
                .put("arrow-flight.client-ssl-certificate", "src/test/resources/certs/client.crt")
                .put("arrow-flight.client-ssl-key", "src/test/resources/certs/client.key");
        return catalogProperties.build();
    }

    @Test
    public void testMtlsInvalidCert()
    {
        assertQueryFails("SELECT COUNT(*) FROM " + ARROW_FLIGHT_CATALOG_WITH_INVALID_CERT + ".tpch.orders", ".*invalid certificate file.*");
    }

    @Test
    public void testMtlsFailure()
    {
        assertQueryFails("SELECT COUNT(*) FROM " + ARROW_FLIGHT_CATALOG_WITH_NO_MTLS_CERTS + ".tpch.orders", "ssl exception");
    }

    @Test
    public void testMtls()
    {
        assertQuery("SELECT COUNT(*) FROM " + ARROW_FLIGHT_CATALOG_WITH_MTLS_CERTS + ".tpch.orders", "SELECT COUNT(*) FROM orders");
    }
}
