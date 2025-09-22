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
package com.facebook.presto.flightshim;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.transaction.IsolationLevel;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;

import javax.inject.Inject;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.testing.TestingSession.DEFAULT_TIME_ZONE_KEY;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class FlightShimProducer
        extends NoOpFlightProducer implements Closeable
{
    private static final JsonCodec<FlightShimRequest> REQUEST_JSON_CODEC = jsonCodec(FlightShimRequest.class);
    private final BufferAllocator allocator;
    private final FlightShimPluginManager pluginManager;
    private final FlightShimConfig config;
    private final ExecutorService shimExecutor;

    @Inject
    public FlightShimProducer(BufferAllocator allocator, FlightShimPluginManager pluginManager, FlightShimConfig config, @ForFlightShimServer ExecutorService shimExecutor)
    {
        this.allocator = allocator.newChildAllocator("flight-shim", 0, Long.MAX_VALUE);
        this.pluginManager = requireNonNull(pluginManager, "pluginManager is null");
        this.config = requireNonNull(config, "config is null");
        this.shimExecutor = requireNonNull(shimExecutor, "shimExecutor is null");
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener)
    {
        shimExecutor.submit(() -> runGetStreamAsync(context, ticket, listener));
    }

    private void runGetStreamAsync(CallContext context, Ticket ticket, ServerStreamListener listener)
    {
        try {
            FlightShimRequest request = REQUEST_JSON_CODEC.fromJson(ticket.getBytes());

            FlightShimPluginManager.ConnectorHolder connectorHolder = pluginManager.getConnector(request.getConnectorId());
            requireNonNull(connectorHolder, format("Requested connector not loaded: %s", request.getConnectorId()));

            Connector connector = connectorHolder.getConnector();
            ConnectorSplit split = connectorHolder.getCodecSplit().fromJson(request.getSplitBytes());

            List<? extends ColumnHandle> columnHandles = request.getColumnHandlesBytes().stream().map(
                    columnHandleBytes -> connectorHolder.getCodecColumnHandle().fromJson(columnHandleBytes)
            ).collect(Collectors.toList());

            List<ColumnMetadata> columnsMetadata = columnHandles.stream()
                    .map(connectorHolder::getColumnMetadata).collect(Collectors.toList());

            ConnectorTransactionHandle transactionHandle = connector.beginTransaction(IsolationLevel.READ_COMMITTED, true);

            // TODO should deserialize session from ticket?
            QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
            Session session = Session.builder(createTestingSessionPropertyManager())
                    .setQueryId(queryIdGenerator.createNextQueryId())
                    .setIdentity(new Identity("user", Optional.empty()))
                    .setTimeZoneKey(DEFAULT_TIME_ZONE_KEY)
                    .setLocale(ENGLISH).build();
            ConnectorId connectorId = new ConnectorId(request.getConnectorId());
            ConnectorSession connectorSession = session.toConnectorSession(connectorId);

            ConnectorRecordSetProvider connectorRecordSetProvider;
            try {
                connectorRecordSetProvider = connector.getRecordSetProvider();
                requireNonNull(connectorRecordSetProvider, format("Connector %s returned a null record set provider", request.getConnectorId()));
            }
            catch (UnsupportedOperationException e) {
                throw new UnsupportedOperationException(format("Connector %s does not provide a record set", request.getConnectorId()), e);
            }

            RecordSet recordSet = connectorRecordSetProvider.getRecordSet(transactionHandle, connectorSession, split, columnHandles);

            try (ArrowBatchSource batchSource = new ArrowBatchSource(allocator, columnsMetadata, recordSet.cursor(), config.getMaxRowsPerBatch())) {
                // TODO use backpressure
                listener.setUseZeroCopy(true);
                listener.start(batchSource.getVectorSchemaRoot());
                while (batchSource.nextBatch() && !listener.isCancelled()) {
                    listener.putNext();
                }
                listener.completed();
            }
        }
        catch (Exception e) {
            listener.error(CallStatus.INTERNAL.withDescription("Error getting connector flight stream: " + e.getMessage()).withCause(e).toRuntimeException());
        }
    }

    @Override
    public void close()
    {
        shimExecutor.shutdownNow();
        pluginManager.stop();
        allocator.close();
    }
}
