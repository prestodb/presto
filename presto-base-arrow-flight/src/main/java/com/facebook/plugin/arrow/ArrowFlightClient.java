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

import java.io.InputStream;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ArrowFlightClient
{
    private final FlightClient flightClient;
    private final Optional<InputStream> trustedCertificate;

    public ArrowFlightClient(FlightClient flightClient, Optional<InputStream> trustedCertificate)
    {
        this.flightClient = requireNonNull(flightClient, "flightClient cannot be null");
        this.trustedCertificate = trustedCertificate;
    }

    public FlightClient getFlightClient()
    {
        return flightClient;
    }

    public Optional<InputStream> getTrustedCertificate()
    {
        return trustedCertificate;
    }

    public void close() throws Exception
    {
        flightClient.close();
        if (trustedCertificate.isPresent()) {
            trustedCertificate.get().close();
        }
    }
}
