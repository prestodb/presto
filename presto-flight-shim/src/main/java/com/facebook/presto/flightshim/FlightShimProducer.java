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
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.log.Logger;
import com.facebook.plugin.arrow.ArrowBatchSource;
import com.facebook.presto.Session;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.BackpressureStrategy;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.testing.TestingSession.DEFAULT_TIME_ZONE_KEY;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class FlightShimProducer
        extends NoOpFlightProducer
        implements AutoCloseable
{
    private static final Logger log = Logger.get(FlightShimProducer.class);
    private static final int CLIENT_POLL_TIME = 5000;  // Backpressure poll time ms
    private final BufferAllocator allocator;
    private final FlightShimPluginManager pluginManager;
    private final PageSourceManager pageSourceManager;
    private final FlightShimConfig config;
    private final ExecutorService shimExecutor;
    private final JsonCodec<FlightShimRequest> requestCodec;

    @Inject
    public FlightShimProducer(
            BufferAllocator allocator,
            FlightShimPluginManager pluginManager,
            FlightShimConfig config,
            @ForFlightShimServer ExecutorService shimExecutor,
            PageSourceManager pageSourceManager,
            TypeDeserializer typeDeserializer,
            BlockEncodingManager blockEncodingManager)
    {
        this.allocator = allocator.newChildAllocator("flight-shim", 0, Long.MAX_VALUE);
        this.pluginManager = requireNonNull(pluginManager, "pluginManager is null");
        this.config = requireNonNull(config, "config is null");
        this.shimExecutor = requireNonNull(shimExecutor, "shimExecutor is null");
        this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
        requireNonNull(typeDeserializer, "typeDeserializer is null");
        requireNonNull(blockEncodingManager, "blockEncodingManager is null");

        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        BlockJsonSerde.Deserializer blockDeserializer = new BlockJsonSerde.Deserializer(blockEncodingManager);
        provider.setJsonDeserializers(ImmutableMap.of(
                RowType.class, typeDeserializer,
                Type.class, typeDeserializer,
                Block.class, blockDeserializer));
        JsonCodecFactory jsonCodecFactory = new JsonCodecFactory(provider);

        this.requestCodec = jsonCodecFactory.jsonCodec(FlightShimRequest.class);
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener)
    {
        log.debug("Received GetStream request");
        shimExecutor.submit(() -> runGetStreamAsync(context, ticket, listener));
    }

    private void runGetStreamAsync(CallContext context, Ticket ticket, ServerStreamListener listener)
    {
        log.debug("Starting GetStream processing");
        int columnCount = 0;
        int rowCount = 0;
        int batchCount = 0;
        try {
            final BackpressureStrategy backpressureStrategy = new BackpressureStrategy.CallbackBackpressureStrategy();
            backpressureStrategy.register(listener);

            FlightShimRequest request = requestCodec.fromJson(ticket.getBytes());
            log.debug("Request for connector: %s", request.getConnectorId());

            FlightShimPluginManager.ConnectorCodecs connectorCodecs = pluginManager.getConnectorCodecs(request.getConnectorId());
            requireNonNull(connectorCodecs, format("Requested connector not loaded: %s", request.getConnectorId()));

            ConnectorSplit connectorSplit = connectorCodecs.getCodecSplit().fromJson(request.getSplitBytes());

            ConnectorTableHandle connectorTableHandle = connectorCodecs.getCodecTableHandle().fromJson(request.getTableHandleBytes());
            ConnectorTransactionHandle transactionHandle = connectorCodecs.getCodecTransactionHandle().fromJson(request.getTransactionHandleBytes());
            Optional<ConnectorTableLayoutHandle> connectorTableLayoutHandle =
                    request.getTableLayoutHandleBytes().map(tableLayoutHandleBytes -> connectorCodecs.getCodecTableLayoutHandle().fromJson(tableLayoutHandleBytes));
            TableHandle tableHandle = new TableHandle(new ConnectorId(request.getConnectorId()), connectorTableHandle, transactionHandle, connectorTableLayoutHandle);

            // Create a dummy session to load the connector
            QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
            Session session = Session.builder(createTestingSessionPropertyManager())
                    .setQueryId(queryIdGenerator.createNextQueryId())
                    .setIdentity(new Identity("user", Optional.empty()))
                    .setTimeZoneKey(DEFAULT_TIME_ZONE_KEY)
                    .setLocale(ENGLISH).build();
            ConnectorId connectorId = new ConnectorId(request.getConnectorId());
            Split split = new Split(connectorId, transactionHandle, connectorSplit);

            List<ColumnHandle> columnHandles = request.getColumnHandlesBytes().stream().map(
                    columnHandleBytes -> connectorCodecs.getCodecColumnHandle().fromJson(columnHandleBytes)
            ).collect(toImmutableList());

            AtomicInteger fieldCount = new AtomicInteger();
            List<ColumnMetadata> columnsMetadata = request.getOutputType().getFields().stream().map(field ->
                            ColumnMetadata.builder().setName(field.getName().orElse(format("$col%s$", fieldCount.incrementAndGet()))).setType(field.getType()).build())
                    .collect(toImmutableList());

            ConnectorPageSource connectorPageSource = pageSourceManager.createPageSource(session, split, tableHandle, columnHandles, new RuntimeStats());

            try (ArrowBatchSource batchSource = new ArrowBatchSource(allocator, columnsMetadata, connectorPageSource, config.getMaxRowsPerBatch())) {
                listener.setUseZeroCopy(true);
                listener.start(batchSource.getVectorSchemaRoot());
                columnCount = batchSource.getVectorSchemaRoot().getFieldVectors().size();
                while (batchSource.nextBatch()) {
                    BackpressureStrategy.WaitResult waitResult;
                    while ((waitResult = backpressureStrategy.waitForListener(CLIENT_POLL_TIME)) == BackpressureStrategy.WaitResult.TIMEOUT) {
                        log.debug(format("Waiting for client to read from connector %s", request.getConnectorId()));
                    }
                    if (waitResult != BackpressureStrategy.WaitResult.READY) {
                        log.info(format("Read stopped from connector %s due to client wait result: %s", request.getConnectorId(), waitResult));
                        break;
                    }
                    rowCount += batchSource.getVectorSchemaRoot().getRowCount();
                    batchCount++;
                    listener.putNext();
                }
                listener.completed();
            }
        }
        catch (Throwable t) {
            final String message = "Error getting connector flight stream";
            log.error(t, message);
            listener.error(CallStatus.INTERNAL.withCause(t).withDescription(format("%s [%s]", message, t)).toRuntimeException());
        }
        finally {
            log.debug(format("Processing GetStream completed [columns=%d, rows=%d, batches=%d]", columnCount, rowCount, batchCount));
        }
    }

    public void shutdown()
    {
        shimExecutor.shutdown();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException
    {
        return shimExecutor.awaitTermination(timeout, unit);
    }

    @Override
    public void close()
            throws InterruptedException
    {
        shutdown();
        if (!awaitTermination(3L, TimeUnit.SECONDS)) {
            log.info("FlightShimProducer not stopped, shutting down now");
            shimExecutor.shutdownNow();
        }
        pluginManager.stop();
        allocator.close();
    }
}
