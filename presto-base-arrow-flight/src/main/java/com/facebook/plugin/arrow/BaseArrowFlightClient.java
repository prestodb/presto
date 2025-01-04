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
import com.facebook.presto.spi.SchemaTableName;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_CLIENT_ERROR;
import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_INFO_ERROR;
import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_METADATA_ERROR;
import static java.nio.file.Files.newInputStream;
import static java.util.Objects.requireNonNull;

public abstract class BaseArrowFlightClient
{
    private final ArrowFlightConfig config;

    private RootAllocator allocator;

    public BaseArrowFlightClient(ArrowFlightConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    protected FlightClient createArrowFlightClient(URI uri)
    {
        return createClient(new Location(uri));
    }

    protected FlightClient createArrowFlightClient()
    {
        Location location;
        if (config.getArrowFlightServerSslEnabled() != null && !config.getArrowFlightServerSslEnabled()) {
            location = Location.forGrpcInsecure(config.getFlightServerName(), config.getArrowFlightPort());
        }
        else {
            location = Location.forGrpcTls(config.getFlightServerName(), config.getArrowFlightPort());
        }
        return createClient(location);
    }

    protected FlightClient createClient(Location location)
    {
        try {
            if (null == allocator) {
                initializeAllocator();
            }
            Optional<InputStream> trustedCertificate = Optional.empty();
            FlightClient.Builder flightClientBuilder = FlightClient.builder(allocator, location);
            if (config.getVerifyServer() != null && !config.getVerifyServer()) {
                flightClientBuilder.verifyServer(false);
            }
            else if (config.getFlightServerSSLCertificate() != null) {
                trustedCertificate = Optional.of(newInputStream(Paths.get(config.getFlightServerSSLCertificate())));
                flightClientBuilder.trustedCertificates(trustedCertificate.get()).useTls();
            }

            FlightClient flightClient = flightClientBuilder.build();
            if (trustedCertificate.isPresent()) {
                trustedCertificate.get().close();
            }

            return flightClient;
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

    public abstract CredentialCallOption[] getCallOptions(ConnectorSession connectorSession);

    protected FlightInfo getFlightInfo(FlightDescriptor flightDescriptor, ConnectorSession connectorSession)
    {
        try (FlightClient client = createArrowFlightClient()) {
            CredentialCallOption[] auth = getCallOptions(connectorSession);
            return client.getInfo(flightDescriptor, auth);
        }
        catch (InterruptedException e) {
            throw new ArrowException(ARROW_FLIGHT_INFO_ERROR, "The flight information could not be obtained from the flight server." + e.getMessage(), e);
        }
    }

    protected ClientClosingFlightStream getFlightStream(ArrowSplit split, Ticket ticket, ConnectorSession connectorSession)
    {
        ByteBuffer byteArray = ByteBuffer.wrap(split.getFlightEndpoint());
        try {
            FlightClient client = FlightEndpoint.deserialize(byteArray).getLocations().stream()
                    .map(Location::getUri)
                    .findAny()
                    .map(this::createArrowFlightClient)
                    .orElseGet(this::createArrowFlightClient);
            return new ClientClosingFlightStream(
                    client.getStream(ticket, getCallOptions(connectorSession)),
                    client);
        }
        catch (FlightRuntimeException | IOException | URISyntaxException e) {
            throw new ArrowException(ARROW_FLIGHT_CLIENT_ERROR, e.getMessage(), e);
        }
    }

    public Schema getSchema(FlightDescriptor flightDescriptor, ConnectorSession connectorSession)
    {
        try (FlightClient client = createArrowFlightClient()) {
            CredentialCallOption[] auth = this.getCallOptions(connectorSession);
            return client.getSchema(flightDescriptor, auth).getSchema();
        }
        catch (InterruptedException e) {
            throw new ArrowException(ARROW_FLIGHT_METADATA_ERROR, "The flight information could not be obtained from the flight server." + e.getMessage(), e);
        }
    }

    public void closeRootAllocator()
    {
        if (null != allocator) {
            allocator.close();
        }
    }

    public abstract List<String> listSchemaNames(ConnectorSession session);

    public abstract List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName);

    protected abstract FlightDescriptor getFlightDescriptorForSchema(String schemaName, String tableName);

    protected abstract FlightDescriptor getFlightDescriptorForTableScan(ArrowTableLayoutHandle tableLayoutHandle);

    public Schema getSchemaForTable(String schemaName, String tableName, ConnectorSession connectorSession)
    {
        FlightDescriptor flightDescriptor = getFlightDescriptorForSchema(schemaName, tableName);
        return getSchema(flightDescriptor, connectorSession);
    }

    public FlightInfo getFlightInfoForTableScan(ArrowTableLayoutHandle tableLayoutHandle, ConnectorSession session)
    {
        FlightDescriptor flightDescriptor = getFlightDescriptorForTableScan(tableLayoutHandle);
        return getFlightInfo(flightDescriptor, session);
    }
}
