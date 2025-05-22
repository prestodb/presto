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
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class TestArrowFlightMTLS
        extends AbstractTestQueryFramework
{
    private static final Logger logger = Logger.get(TestArrowFlightMTLS.class);
    private static final CallOption CALL_OPTIONS = CallOptions.timeout(300, TimeUnit.SECONDS);
    private final int serverPort;
    private RootAllocator allocator;
    private FlightServer server;
    private DistributedQueryRunner arrowFlightQueryRunner;

    public TestArrowFlightMTLS()
            throws IOException
    {
        this.serverPort = ArrowFlightQueryRunner.findUnusedPort();
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        arrowFlightQueryRunner = getDistributedQueryRunner();
        File certChainFile = new File("src/test/resources/mtls/server.crt");
        File privateKeyFile = new File("src/test/resources/mtls/server.key");
        File caCertFile = new File("src/test/resources/mtls/ca.crt");

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
        ImmutableMap.Builder<String, String> catalogProperties = ImmutableMap.<String, String>builder()
                .put("arrow-flight.server.port", String.valueOf(serverPort))
                .put("arrow-flight.server", "localhost")
                .put("arrow-flight.server-ssl-enabled", "true")
                .put("arrow-flight.server-ssl-certificate", "src/test/resources/mtls/server.crt")
                .put("arrow-flight.client-ssl-certificate", "src/test/resources/mtls/client.crt")
                .put("arrow-flight.client-ssl-key", "src/test/resources/mtls/client.key");
        return ArrowFlightQueryRunner.createQueryRunner(ImmutableMap.of(), catalogProperties.build(), ImmutableMap.of(), Optional.empty());
    }

    @Test
    public void testMtls()
    {
        assertQuery("SELECT COUNT(*) FROM orders");
    }
}
