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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
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
    private byte[] ticket;
    private List<String> locationUrls;

    @BeforeMethod
    public void setUp()
    {
        schemaName = "testSchema";
        tableName = "testTable";
        ticket = new byte[] {1, 2, 3, 4};
        locationUrls = Arrays.asList("http://localhost:8080", "http://localhost:8081");

        // Instantiate ArrowSplit with mock data
        arrowSplit = new ArrowSplit(schemaName, tableName, ticket, locationUrls);
    }

    @Test
    public void testConstructorAndGetters()
    {
        // Test that the constructor correctly initializes fields
        assertEquals(arrowSplit.getSchemaName(), schemaName, "Schema name should match.");
        assertEquals(arrowSplit.getTableName(), tableName, "Table name should match.");
        assertEquals(arrowSplit.getTicket(), ticket, "Ticket byte array should match.");
        assertEquals(arrowSplit.getLocationUrls(), locationUrls, "Location URLs list should match.");
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
