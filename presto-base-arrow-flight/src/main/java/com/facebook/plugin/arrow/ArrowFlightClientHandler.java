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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorSession;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Optional;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_ERROR;

public abstract class ArrowFlightClientHandler
{
    private static final Logger logger = Logger.get(ArrowFlightClientHandler.class);
    private final ArrowFlightConfig config;

    private RootAllocator allocator;

    public ArrowFlightClientHandler(ArrowFlightConfig config)
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
            throw new ArrowException(ARROW_FLIGHT_ERROR, "The flight client could not be obtained." + ex.getMessage(), ex);
        }
    }

    private synchronized void initializeAllocator()
    {
        if (allocator == null) {
            allocator = new RootAllocator(Long.MAX_VALUE);
        }
    }

    protected abstract CredentialCallOption getCallOptions(ConnectorSession connectorSession);

    public ArrowFlightConfig getConfig()
    {
        return config;
    }

    public ArrowFlightClient getClient(Optional<String> uri)
    {
        return initializeClient(uri);
    }

    public FlightInfo getFlightInfo(FlightDescriptor flightDescriptor, ConnectorSession connectorSession)
    {
        try (ArrowFlightClient client = getClient(Optional.empty())) {
            CredentialCallOption auth = this.getCallOptions(connectorSession);
            FlightInfo flightInfo = client.getFlightClient().getInfo(flightDescriptor, auth);
            return flightInfo;
        }
        catch (Exception e) {
            throw new ArrowException(ARROW_FLIGHT_ERROR, "The flight information could not be obtained from the flight server." + e.getMessage(), e);
        }
    }

    public Optional<Schema> getSchema(FlightDescriptor flightDescriptor, ConnectorSession connectorSession)
    {
        return getFlightInfo(flightDescriptor, connectorSession).getSchemaOptional();
    }

    public void closeRootallocator()
    {
        if (null != allocator) {
            allocator.close();
        }
    }
}
