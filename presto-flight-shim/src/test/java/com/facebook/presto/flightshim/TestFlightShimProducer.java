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

import com.facebook.airlift.testing.postgresql.TestingPostgreSqlServer;
import com.facebook.presto.plugin.postgresql.PostgreSqlQueryRunner;
import com.facebook.presto.testing.QueryRunner;

import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.testng.annotations.Test;

import java.util.concurrent.CancellationException;

import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

public class TestFlightShimProducer
        extends AbstractTestFlightShimBase
{
    private final TestingPostgreSqlServer postgreSqlServer;

    public TestFlightShimProducer()
            throws Exception
    {
        this.postgreSqlServer = new TestingPostgreSqlServer("testuser", "tpch");
        closables.add(postgreSqlServer);
    }

    @Override
    protected String getConnectorId()
    {
        return "postgresql";
    }

    @Override
    protected String getConnectionUrl()
    {
        return postgreSqlServer.getJdbcUrl();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // NOTE: only creating query runner to populate test server
        return PostgreSqlQueryRunner.createPostgreSqlQueryRunner(postgreSqlServer, ImmutableMap.of(), TpchTable.getTables());
    }

    @Test
    public void testConnectorGetStream() throws Exception
    {
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {

            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchCustomerRequest(getAllColumns())));

            int rowCount = 0;
            try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                while (stream.next()) {
                    rowCount += stream.getRoot().getRowCount();
                }
            }

            // TODO compare results against query
            assertGreaterThan(rowCount, 0);
        }
    }

    @Test
    public void testStopStreamAtLimit() throws Exception
    {
        int rowLimit = 500;
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {

            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchCustomerRequest(getAllColumns())));

            int rowCount = 0;
            try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                while (stream.next()) {
                    rowCount += stream.getRoot().getRowCount();
                    if (rowCount >= rowLimit) {
                        break;
                    }
                }
            }

            assertEquals(rowCount, rowLimit);
        }
    }

    @Test
    public void testCancelStream() throws Exception
    {
        String cancelMessage = "READ COMPLETE";
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {

            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchCustomerRequest(getAllColumns())));

            // Cancel stream explicitly
            int rowCount = 0;
            try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                while (stream.next()) {
                    rowCount += stream.getRoot().getRowCount();
                    if (rowCount >= 500) {
                        stream.cancel("Cancel", new CancellationException(cancelMessage));
                        break;
                    }
                }

                // Drain any remaining messages to properly release messages
                try {
                    do {
                        Thread.sleep(1000);
                    }
                    while (stream.next());
                }
                catch (final Exception e) {
                    assertNotNull(e.getCause());
                    assertEquals(e.getCause().getMessage(), cancelMessage);
                }
            }

            assertGreaterThan(rowCount, 0);
        }
    }
}
