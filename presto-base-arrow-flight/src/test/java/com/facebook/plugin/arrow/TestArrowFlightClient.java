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

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.RootAllocator;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestArrowFlightClient
{
    @Test
    public void testArrowFlightClient()
    {
        FlightClient flightClient = mock(FlightClient.class);
        InputStream certificateStream = mock(InputStream.class);
        Optional<InputStream> trustedCertificate = Optional.of(certificateStream);
        RootAllocator allocator = mock(RootAllocator.class);

        ArrowFlightClient arrowFlightClient = new ArrowFlightClient(flightClient, trustedCertificate, allocator);

        assertEquals(arrowFlightClient.getFlightClient(), flightClient);
        assertTrue(arrowFlightClient.getTrustedCertificate().isPresent());
        assertEquals(arrowFlightClient.getTrustedCertificate().get(), certificateStream);
    }

    @Test
    public void testArrowFlightClientWithoutCertificate()
    {
        FlightClient flightClient = mock(FlightClient.class);
        Optional<InputStream> trustedCertificate = Optional.empty();
        RootAllocator allocator = mock(RootAllocator.class);
        ArrowFlightClient arrowFlightClient = new ArrowFlightClient(flightClient, trustedCertificate, allocator);

        assertEquals(arrowFlightClient.getFlightClient(), flightClient);
        assertFalse(arrowFlightClient.getTrustedCertificate().isPresent());
    }

    @Test
    public void testClose() throws Exception
    {
        FlightClient flightClient = mock(FlightClient.class);
        InputStream certificateStream = mock(InputStream.class);
        Optional<InputStream> trustedCertificate = Optional.of(certificateStream);
        RootAllocator allocator = mock(RootAllocator.class);

        ArrowFlightClient arrowFlightClient = new ArrowFlightClient(flightClient, trustedCertificate, allocator);

        arrowFlightClient.close();

        verify(flightClient, times(1)).close();
        verify(certificateStream, times(1)).close();
    }

    @Test
    public void testCloseWithoutCertificate() throws Exception
    {
        FlightClient flightClient = mock(FlightClient.class);
        Optional<InputStream> trustedCertificate = Optional.empty();
        RootAllocator allocator = mock(RootAllocator.class);

        ArrowFlightClient arrowFlightClient = new ArrowFlightClient(flightClient, trustedCertificate, allocator);

        arrowFlightClient.close();

        verify(flightClient, times(1)).close();
    }
}
