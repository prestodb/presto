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
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
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

public abstract class BaseArrowFlightClientHandler
{
    private final ArrowFlightConfig config;
    private final BufferAllocator allocator;

    public BaseArrowFlightClientHandler(BufferAllocator allocator, ArrowFlightConfig config)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.config = requireNonNull(config, "config is null");
    }

    protected FlightClient createFlightClient()
    {
        Location location;
        if (config.getArrowFlightServerSslEnabled()) {
            location = Location.forGrpcTls(config.getFlightServerName(), config.getArrowFlightPort());
        }
        else {
            location = Location.forGrpcInsecure(config.getFlightServerName(), config.getArrowFlightPort());
        }
        return createFlightClient(location);
    }

    protected FlightClient createFlightClient(Location location)
    {
        try {
            Optional<InputStream> trustedCertificate = Optional.empty();
            FlightClient.Builder flightClientBuilder = FlightClient.builder(allocator, location);
            flightClientBuilder.verifyServer(config.getVerifyServer());
            if (config.getFlightServerSSLCertificate() != null) {
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
            throw new ArrowException(ARROW_FLIGHT_CLIENT_ERROR, "Error creating flight client: " + e.getMessage(), e);
        }
    }

    public abstract CallOption[] getCallOptions(ConnectorSession connectorSession);

    protected FlightInfo getFlightInfo(ConnectorSession connectorSession, FlightDescriptor flightDescriptor)
    {
        try (FlightClient client = createFlightClient()) {
            CallOption[] callOptions = getCallOptions(connectorSession);
            return client.getInfo(flightDescriptor, callOptions);
        }
        catch (InterruptedException e) {
            throw new ArrowException(ARROW_FLIGHT_INFO_ERROR, "Error getting flight information: " + e.getMessage(), e);
        }
    }

    protected ClientClosingFlightStream getFlightStream(ConnectorSession connectorSession, ArrowSplit split)
    {
        ByteBuffer endpointBytes = ByteBuffer.wrap(split.getFlightEndpointBytes());
        try {
            FlightEndpoint endpoint = FlightEndpoint.deserialize(endpointBytes);
            FlightClient client = endpoint.getLocations().stream()
                    .findAny()
                    .map(this::createFlightClient)
                    .orElseGet(this::createFlightClient);
            return new ClientClosingFlightStream(
                    client.getStream(endpoint.getTicket(), getCallOptions(connectorSession)),
                    client);
        }
        catch (FlightRuntimeException | IOException | URISyntaxException e) {
            throw new ArrowException(ARROW_FLIGHT_CLIENT_ERROR, e.getMessage(), e);
        }
    }

    public Schema getSchema(ConnectorSession connectorSession, FlightDescriptor flightDescriptor)
    {
        try (FlightClient client = createFlightClient()) {
            CallOption[] callOptions = this.getCallOptions(connectorSession);
            return client.getSchema(flightDescriptor, callOptions).getSchema();
        }
        catch (InterruptedException e) {
            throw new ArrowException(ARROW_FLIGHT_METADATA_ERROR, "Error getting schema for flight: " + e.getMessage(), e);
        }
    }

    public abstract List<String> listSchemaNames(ConnectorSession session);

    public abstract List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName);

    protected abstract FlightDescriptor getFlightDescriptorForSchema(String schemaName, String tableName);

    protected abstract FlightDescriptor getFlightDescriptorForTableScan(ArrowTableLayoutHandle tableLayoutHandle);

    public Schema getSchemaForTable(ConnectorSession connectorSession, String schemaName, String tableName)
    {
        FlightDescriptor flightDescriptor = getFlightDescriptorForSchema(connectorSession, schemaName, tableName);
        return getSchema(connectorSession, flightDescriptor);
    }

    public FlightInfo getFlightInfoForTableScan(ConnectorSession session, ArrowTableLayoutHandle tableLayoutHandle)
    {
        FlightDescriptor flightDescriptor = getFlightDescriptorForTableScan(session, tableLayoutHandle);
        return getFlightInfo(session, flightDescriptor);
    }
}
