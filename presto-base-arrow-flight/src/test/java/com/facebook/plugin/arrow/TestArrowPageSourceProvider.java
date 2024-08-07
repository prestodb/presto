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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.testng.annotations.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestArrowPageSourceProvider
{
    @Test
    public void testCreatePageSourceWithValidParameters()
    {
        ArrowFlightClientHandler clientHandler = mock(ArrowFlightClientHandler.class);
        ArrowFlightClient flightClient = mock(ArrowFlightClient.class);
        FlightClient flightClientInstance = mock(FlightClient.class);
        FlightStream flightStream = mock(FlightStream.class);
        when(clientHandler.getClient(any())).thenReturn(flightClient);
        when(flightClient.getFlightClient()).thenReturn(flightClientInstance);
        when(flightClientInstance.getStream(any(Ticket.class), any(), any())).thenReturn(flightStream);
        ArrowPageSourceProvider arrowPageSourceProvider = new ArrowPageSourceProvider(clientHandler);
        ConnectorTransactionHandle transactionHandle = mock(ConnectorTransactionHandle.class);
        ConnectorSession session = mock(ConnectorSession.class);
        ArrowSplit split = mock(ArrowSplit.class);
        List<ColumnHandle> columns = ImmutableList.of(mock(ArrowColumnHandle.class));
        SplitContext splitContext = mock(SplitContext.class);
        when(split.getTicket()).thenReturn(new byte[0]);
        ConnectorPageSource pageSource = arrowPageSourceProvider.createPageSource(transactionHandle, session, split, columns, splitContext);
        assertNotNull(pageSource);
        assertTrue(pageSource instanceof ArrowPageSource);
    }
}
