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

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestArrowSplit
{
    private ArrowSplit arrowSplit;
    private String schemaName;
    private String tableName;
    private FlightEndpoint flightEndpoint;

    @BeforeMethod
    public void setUp()
            throws URISyntaxException
    {
        schemaName = "testSchema";
        tableName = "testTable";
        byte[] ticketArray = new byte[] {1, 2, 3, 4};
        Ticket ticket = new Ticket(ByteBuffer.wrap(ticketArray).array());  // Wrap the byte array in a Ticket
        Location location = new Location("http://localhost:8080");
        flightEndpoint = new FlightEndpoint(ticket, location);
        // Instantiate ArrowSplit with mock data
        arrowSplit = new ArrowSplit(schemaName, tableName, flightEndpoint.serialize().array());
    }

    @Test
    public void testConstructorAndGetters()
    {
        // Test that the constructor correctly initializes fields
        assertEquals(arrowSplit.getSchemaName(), schemaName, "Schema name should match.");
        assertEquals(arrowSplit.getTableName(), tableName, "Table name should match.");
        assertEquals(arrowSplit.getFlightEndpointBytes(), flightEndpoint.serialize().array(), "Byte array should match");
    }

    @Test
    public void testNodeSelectionStrategy()
    {
        // Test that the node selection strategy is NO_PREFERENCE
        assertEquals(arrowSplit.getNodeSelectionStrategy(), NodeSelectionStrategy.NO_PREFERENCE, "Node selection strategy should be NO_PREFERENCE.");
    }

    @Test
    public void testGetPreferredNodes()
    {
        // Test that the preferred nodes list is empty
        List<HostAddress> preferredNodes = arrowSplit.getPreferredNodes(null);
        assertNotNull(preferredNodes, "Preferred nodes list should not be null.");
        assertTrue(preferredNodes.isEmpty(), "Preferred nodes list should be empty.");
    }
}
