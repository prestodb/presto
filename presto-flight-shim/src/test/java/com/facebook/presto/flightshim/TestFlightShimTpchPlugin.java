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

import com.facebook.presto.Session;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.testng.annotations.Test;

import java.util.concurrent.CancellationException;

import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestFlightShimTpchPlugin
        extends AbstractTestFlightShimPlugins
{
    public static final int SPLITS_PER_NODE = 4;

    protected String getConnectorId()
    {
        return "tpch";
    }

    protected String getPluginBundles()
    {
        return "../presto-tpch/pom.xml";
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        return new LocalQueryRunner(session);
    }

    @Override
    protected int getExpectedTotalParts()
    {
        return SPLITS_PER_NODE;
    }

    @Override
    protected void createTables()
    {
        ((LocalQueryRunner) getExpectedQueryRunner()).createCatalog("tpch", new TpchConnectorFactory(SPLITS_PER_NODE), ImmutableMap.of());
    }

    @Test
    public void testConnectorGetStream() throws Exception
    {
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {
            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchTableRequest(0, 1, ImmutableList.of(getOrderKeyColumn()))));

            int rowCount = 0;
            try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                while (stream.next()) {
                    rowCount += stream.getRoot().getRowCount();
                }
            }

            assertGreaterThan(rowCount, 0);
        }
    }

    @Test
    public void testStopStreamAtLimit() throws Exception
    {
        int rowLimit = 500;
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {
            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchTableRequest(0, 1, ImmutableList.of(getOrderKeyColumn()))));

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
            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchTableRequest(0, 1, ImmutableList.of(getOrderKeyColumn()))));

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
                        Thread.sleep(100);
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

    @Test
    public void testReadFromMultipleSplits()
    {
        assertSelectQueryFromColumns(ImmutableList.of(ORDERKEY_COLUMN, LINENUMBER_COLUMN, SHIPINSTRUCT_COLUMN));
    }
}
