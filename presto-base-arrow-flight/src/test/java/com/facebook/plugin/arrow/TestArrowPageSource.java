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

import com.facebook.presto.spi.ConnectorSession;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestArrowPageSource
{
    @Mock
    private ArrowSplit mockSplit;

    @Mock
    private List<ArrowColumnHandle> mockColumnHandles;

    @Mock
    private ArrowFlightClientHandler mockClientHandler;

    @Mock
    private ConnectorSession mockSession;

    @Mock
    private FlightClient mockFlightClient;

    @Mock
    private FlightStream mockFlightStream;

    @Mock
    private VectorSchemaRoot mockVectorSchemaRoot;

    @BeforeClass
    public void setUp()
    {
        MockitoAnnotations.openMocks(this);
        ArrowFlightClient mockArrowFlightClient = mock(ArrowFlightClient.class);
        when(mockClientHandler.getClient(any(Optional.class))).thenReturn(mockArrowFlightClient);
        when(mockArrowFlightClient.getFlightClient()).thenReturn(mockFlightClient);
        when(mockFlightClient.getStream(any(Ticket.class), any())).thenReturn(mockFlightStream);
    }

    @Test
    public void testInitialization()
    {
        ArrowPageSource arrowPageSource = new ArrowPageSource(mockSplit, mockColumnHandles, mockClientHandler, mockSession);
        assertNotNull(arrowPageSource);
    }

    @Test
    public void testGetNextPageWithEmptyFlightStream()
    {
        when(mockFlightStream.next()).thenReturn(false);
        ArrowPageSource arrowPageSource = new ArrowPageSource(mockSplit, mockColumnHandles, mockClientHandler, mockSession);
        assertNull(arrowPageSource.getNextPage());
    }

    @Test
    public void testGetNextPageWithNonEmptyFlightStream()
    {
        when(mockFlightStream.next()).thenReturn(true);
        when(mockFlightStream.getRoot()).thenReturn(mockVectorSchemaRoot);
        when(mockVectorSchemaRoot.getRowCount()).thenReturn(1);
        ArrowPageSource arrowPageSource = new ArrowPageSource(mockSplit, mockColumnHandles, mockClientHandler, mockSession);
        assertNotNull(arrowPageSource.getNextPage());
    }

    @Test
    public void testCloseResources() throws Exception
    {
        ArrowPageSource arrowPageSource = new ArrowPageSource(mockSplit, mockColumnHandles, mockClientHandler, mockSession);
        arrowPageSource.close();
        verify(mockFlightStream).close();
    }
}
