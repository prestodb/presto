
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
import com.facebook.presto.testing.MaterializedResult;
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
import java.util.Map;
import java.util.Optional;

import static com.facebook.plugin.arrow.testingConnector.TestingArrowFlightPlugin.ARROW_FLIGHT_CONNECTOR;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static org.testng.Assert.assertTrue;

@Test
public class TestArrowFlightIntegrationMixedCase
        extends AbstractTestQueryFramework
{
    private static final Logger logger = Logger.get(TestArrowFlightIntegrationMixedCase.class);
    private static final String ARROW_FLIGHT_MIXED_CATALOG = "arrow_mixed_catalog";
    private int serverPort;
    private RootAllocator allocator;
    private FlightServer server;
    private DistributedQueryRunner arrowFlightQueryRunner;

    @BeforeClass
    public void setup()
            throws Exception
    {
        arrowFlightQueryRunner = getDistributedQueryRunner();
        arrowFlightQueryRunner.createCatalog(ARROW_FLIGHT_MIXED_CATALOG, ARROW_FLIGHT_CONNECTOR, getCatalogProperties());
        File certChainFile = new File("src/test/resources/certs/server.crt");
        File privateKeyFile = new File("src/test/resources/certs/server.key");

        allocator = new RootAllocator(Long.MAX_VALUE);
        Location location = Location.forGrpcTls("localhost", serverPort);
        server = FlightServer.builder(allocator, location, new TestingArrowProducer(allocator, true))
                .useTls(certChainFile, privateKeyFile)
                .build();

        server.start();
        logger.info("Server listening on port %s", server.getPort());
    }

    private Map<String, String> getCatalogProperties()
    {
        ImmutableMap.Builder<String, String> catalogProperties = ImmutableMap.<String, String>builder()
                .put("arrow-flight.server.port", String.valueOf(serverPort))
                .put("arrow-flight.server", "localhost")
                .put("arrow-flight.server-ssl-enabled", "true")
                .put("arrow-flight.server-ssl-certificate", "src/test/resources/certs/server.crt")
                .put("case-sensitive-name-matching", "true");
        return catalogProperties.build();
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
        serverPort = ArrowFlightQueryRunner.findUnusedPort();
        return ArrowFlightQueryRunner.createQueryRunner(serverPort, ImmutableMap.of(), ImmutableMap.of(), Optional.empty(), Optional.empty());
    }

    @Test
    public void testShowSchemas()
    {
        MaterializedResult actualRow = computeActual("SHOW schemas FROM arrow_mixed_catalog");
        MaterializedResult expectedRow = resultBuilder(getSession(), createVarcharType(50))
                .row("Tpch_Mx")
                .row("tpch_mx")
                .build();

        assertContains(actualRow, expectedRow);
    }

    @Test
    public void testShowTables()
    {
        MaterializedResult actualRow = computeActual("SHOW TABLES FROM arrow_mixed_catalog.tpch_mx");
        MaterializedResult expectedRow = resultBuilder(getSession(), createVarcharType(50))
                .row("MXTEST")
                .row("mxtest")
                .build();

        assertContains(actualRow, expectedRow);
    }

    @Test
    public void testShowColumns()
    {
        MaterializedResult actualRow = computeActual("SHOW columns FROM arrow_mixed_catalog.tpch_mx.mxtest");
        MaterializedResult expectedRow = resultBuilder(getSession(), createVarcharType(50))
                .row("ID", "integer", "", "", Long.valueOf(10), null, null)
                .row("NAME", "varchar(50)", "", "", null, null, Long.valueOf(50))
                .row("name", "varchar(50)", "", "", null, null, Long.valueOf(50))
                .row("Address", "varchar(50)", "", "", null, null, Long.valueOf(50))
                .build();

        assertContains(actualRow, expectedRow);
    }

    @Test
    public void testSelect()
    {
        MaterializedResult actualRow = computeActual("SELECT * from arrow_mixed_catalog.tpch_mx.mxtest");
        MaterializedResult expectedRow = resultBuilder(getSession(), INTEGER, createVarcharType(50), createVarcharType(50), createVarcharType(50))
                .row(1, "TOM", "test", "kochi")
                .row(2, "MARY", "test", "kochi")
                .build();
        assertTrue(actualRow.equals(expectedRow));
    }
}
