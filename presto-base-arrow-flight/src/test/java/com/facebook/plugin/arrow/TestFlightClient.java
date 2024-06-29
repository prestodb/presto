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
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.RootAllocator;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestFlightClient
{
    private static RootAllocator allocator;
    private static FlightServer server;

    private static Location serverLocation;

    @BeforeClass
    public void setup() throws Exception
    {
        allocator = new RootAllocator(Long.MAX_VALUE);
        serverLocation = Location.forGrpcInsecure("127.0.0.1", 9443);
        server = FlightServer.builder(allocator, serverLocation, new ArrowServer())
                .build();
        server.start();
        System.out.println("Server listening on port " + server.getPort());
    }

    @AfterClass
    public void tearDown() throws Exception
    {
        server.close();
        allocator.close();
    }

    @Test
    public void testInitializeClient()
    {
        ArrowFlightConfig config = mock(ArrowFlightConfig.class);
        when(config.getFlightServerSSLCertificate()).thenReturn(null);
        when(config.getVerifyServer()).thenReturn(true);
        ConnectorSession connectorSession = mock(ConnectorSession.class);
        ArrowFlightClientHandler handler = new ArrowFlightClientHandler(config) {
            @Override
            protected CredentialCallOption getCallOptions(ConnectorSession connectorSession)
            {
                return null;
            }
        };
        Optional<String> uri = Optional.of("grpc://127.0.0.1:9443");
        handler.getClient(uri);
        assertNotNull(handler.getClient(uri));
    }

    @Test
    public void testGetFlightInfo()
    {
        ArrowFlightConfig config = Mockito.mock(ArrowFlightConfig.class);
        FlightInfo mockFlightInfo = mock(FlightInfo.class);
        ArrowFlightClientHandler clientHandler = Mockito.mock(ArrowFlightClientHandler.class);
        ArrowFlightRequest request = new TestArrowFlightRequest("schema", "table", "query");
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        when(clientHandler.getFlightInfo(request, session)).thenReturn(mockFlightInfo);
        FlightInfo result = clientHandler.getFlightInfo(request, session);
        assertEquals(mockFlightInfo, result);
    }
}
