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
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_CLIENT_ERROR;
import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_INFO_ERROR;
import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_METADATA_ERROR;
import static java.util.Objects.requireNonNull;

public abstract class AbstractArrowFlightClientHandler
{
    private final ArrowFlightConfig config;

    private RootAllocator allocator;

    public AbstractArrowFlightClientHandler(ArrowFlightConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    private ArrowFlightClient initializeClient(Optional<String> uri)
    {
        try {
            Optional<InputStream> trustedCertificate = Optional.empty();

            Location location;
            if (uri.isPresent()) {
                location = new Location(uri.get());
            }
            else {
                if (config.getArrowFlightServerSslEnabled() != null && !config.getArrowFlightServerSslEnabled()) {
                    location = Location.forGrpcInsecure(config.getFlightServerName(), config.getArrowFlightPort());
                }
                else {
                    location = Location.forGrpcTls(config.getFlightServerName(), config.getArrowFlightPort());
                }
            }

            if (null == allocator) {
                initializeAllocator();
            }

            FlightClient.Builder flightClientBuilder = FlightClient.builder(allocator, location);
            if (config.getVerifyServer() != null && !config.getVerifyServer()) {
                flightClientBuilder.verifyServer(false);
            }
            else if (config.getFlightServerSSLCertificate() != null) {
                trustedCertificate = Optional.of(new FileInputStream(config.getFlightServerSSLCertificate()));
                flightClientBuilder.trustedCertificates(trustedCertificate.get()).useTls();
            }

            FlightClient flightClient = flightClientBuilder.build();
            return new ArrowFlightClient(flightClient, trustedCertificate);
        }
        catch (Exception e) {
            throw new ArrowException(ARROW_FLIGHT_CLIENT_ERROR, "The flight client could not be obtained." + e.getMessage(), e);
        }
    }

    private synchronized void initializeAllocator()
    {
        if (allocator == null) {
            allocator = new RootAllocator(Long.MAX_VALUE);
        }
    }

    protected abstract CredentialCallOption[] getCallOptions(ConnectorSession connectorSession);

    public ArrowFlightClient createArrowFlightClient(String uri)
    {
        return initializeClient(Optional.of(uri));
    }

    public ArrowFlightClient createArrowFlightClient()
    {
        return initializeClient(Optional.empty());
    }

    public FlightInfo getFlightInfo(FlightDescriptor flightDescriptor, ConnectorSession connectorSession)
    {
        try (ArrowFlightClient client = createArrowFlightClient()) {
            CredentialCallOption[] auth = this.getCallOptions(connectorSession);
            FlightInfo flightInfo = client.getFlightClient().getInfo(flightDescriptor, auth);
            return flightInfo;
        }
        catch (InterruptedException | IOException e) {
            throw new ArrowException(ARROW_FLIGHT_INFO_ERROR, "The flight information could not be obtained from the flight server." + e.getMessage(), e);
        }
    }

    public Schema getSchema(FlightDescriptor flightDescriptor, ConnectorSession connectorSession)
    {
        try (ArrowFlightClient client = createArrowFlightClient()) {
            CredentialCallOption[] auth = this.getCallOptions(connectorSession);
            return client.getFlightClient().getSchema(flightDescriptor, auth).getSchema();
        }
        catch (InterruptedException | IOException e) {
            throw new ArrowException(ARROW_FLIGHT_METADATA_ERROR, "The flight information could not be obtained from the flight server." + e.getMessage(), e);
        }
    }

    public void closeRootAllocator()
    {
        if (null != allocator) {
            allocator.close();
        }
    }
}
