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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestArrowFlightQueriesWithDictionaryVector
        extends AbstractTestQueryFramework
{
    private static final Logger logger = Logger.get(TestArrowFlightQueriesWithDictionaryVector.class);
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
        serverLocation = Location.forGrpcTls("127.0.0.1", 9444);

        server = FlightServer.builder(allocator, serverLocation, new TestingArrowServerUsingDictionaryVector(allocator))
                .useTls(certChainFile, privateKeyFile)
                .build();

        server.start();
        logger.info("Server listening on port " + server.getPort());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return ArrowFlightQueryRunner.createQueryRunner(9444);
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws InterruptedException
    {
        allocator.close();
        server.close();
        arrowFlightQueryRunner.close();
    }

    @Test
    public void testDictionaryBlock()
    {
        // Retrieve the actual result from the query
        MaterializedResult actual = computeActual("SELECT shipmode, orderkey FROM lineitem");
        // Validate the row count first
        assertEquals(actual.getRowCount(), 3);
        // Now, validate each row
        MaterializedResult expectedRow = resultBuilder(getSession(), VARCHAR, BIGINT)
                .row("apple", 0L)
                .row("banana", 1L)
                .row("cherry", 2L)
                .build();
        assertEquals(expectedRow, actual);
    }
}
