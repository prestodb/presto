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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.File;

public class TestArrowFlightIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private static final Logger logger = Logger.get(TestArrowFlightIntegrationSmokeTest.class);
    private RootAllocator allocator;
    private FlightServer server;
    private Location serverLocation;
    private DistributedQueryRunner arrowFlightQueryRunner;

    @BeforeClass
    public void setup()
            throws Exception
    {
        arrowFlightQueryRunner = getDistributedQueryRunner();
        File certChainFile = new File("src/test/resources/server.crt");
        File privateKeyFile = new File("src/test/resources/server.key");

        allocator = new RootAllocator(Long.MAX_VALUE);
        serverLocation = Location.forGrpcTls("127.0.0.1", 9442);
        server = FlightServer.builder(allocator, serverLocation, new TestingArrowServer(allocator))
                .useTls(certChainFile, privateKeyFile)
                .build();

        server.start();
        logger.info("Server listening on port " + server.getPort());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return ArrowFlightQueryRunner.createQueryRunner(9442);
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws InterruptedException
    {
        server.close();
        allocator.close();
        arrowFlightQueryRunner.close();
    }
}
