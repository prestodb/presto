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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.PrestoException;
import com.google.inject.Inject;
import com.yammer.metrics.core.MetricsRegistry;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.response.ServerInstance;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.serde.SerDe;
import org.apache.pinot.transport.common.CompositeFuture;
import org.apache.pinot.transport.metrics.NettyClientMetrics;
import org.apache.pinot.transport.netty.PooledNettyClientResourceManager;
import org.apache.pinot.transport.pool.KeyedPool;
import org.apache.pinot.transport.pool.KeyedPoolImpl;
import org.apache.pinot.transport.scattergather.ScatterGather;
import org.apache.pinot.transport.scattergather.ScatterGatherImpl;
import org.apache.pinot.transport.scattergather.ScatterGatherRequest;
import org.apache.pinot.transport.scattergather.ScatterGatherStats;
import org.apache.thrift.protocol.TCompactProtocol;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_FAILURE_QUERYING_DATA;

/**
 * This class acts as the Pinot broker, fetches data from Pinot segments, gathers and returns the result.
 * Many components were taken from <a href="https://github.com/apache/incubator-pinot/blob/master/pinot-broker/src/main/java/org/apache/pinot/broker/requesthandler/ConnectionPoolBrokerRequestHandler.java">ConnectionPoolBrokerRequestHandler</a>
 */
public class PinotScatterGatherQueryClient
{
    private static final Logger log = Logger.get(PinotScatterGatherQueryClient.class);

    private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();
    private static final String PRESTO_HOST_PREFIX = "presto-pinot-master";
    private static final boolean DEFAULT_EMIT_TABLE_LEVEL_METRICS = true;

    private final AtomicLong requestIdGenerator;
    private final String prestoHostId;
    private final MetricsRegistry registry;
    private final BrokerMetrics brokerMetrics;
    private final ScatterGather scatterGatherer;
    // Netty Specific
    private EventLoopGroup eventLoopGroup;
    private PooledNettyClientResourceManager resourceManager;
    // Connection Pool Related
    private KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> connPool;
    private ScheduledThreadPoolExecutor poolTimeoutExecutor;
    private ExecutorService requestSenderPool;
    private Duration connectionTimeout;

    @Inject
    public PinotScatterGatherQueryClient(PinotConfig pinotConfig)
    {
        requestIdGenerator = new AtomicLong(0);
        prestoHostId = getDefaultPrestoId();

        registry = new MetricsRegistry();
        brokerMetrics = new BrokerMetrics(registry, DEFAULT_EMIT_TABLE_LEVEL_METRICS);
        brokerMetrics.initializeGlobalMeters();

        eventLoopGroup = new NioEventLoopGroup();
        /**
         * Some of the client metrics uses histogram which is doing synchronous operation.
         * These are fixed overhead per request/response.
         * TODO: Measure the overhead of this.
         */
        final NettyClientMetrics clientMetrics = new NettyClientMetrics(registry, "presto_pinot_client_");

        // Setup Netty Connection Pool
        resourceManager = new PooledNettyClientResourceManager(eventLoopGroup, new HashedWheelTimer(), clientMetrics);

        requestSenderPool = Executors.newFixedThreadPool(pinotConfig.getThreadPoolSize());
        poolTimeoutExecutor = new ScheduledThreadPoolExecutor(pinotConfig.getCorePoolSize());
        connectionTimeout = pinotConfig.getConnectionTimeout();
        connPool = new KeyedPoolImpl<PooledNettyClientResourceManager.PooledClientConnection>(pinotConfig.getMinConnectionsPerServer(), pinotConfig.getMaxConnectionsPerServer(), pinotConfig.getIdleTimeout().toMillis(), pinotConfig.getMaxBacklogPerServer(), resourceManager, poolTimeoutExecutor, requestSenderPool, registry);
        resourceManager.setPool(connPool);

        // Setup ScatterGather
        scatterGatherer = new ScatterGatherImpl(connPool, requestSenderPool);
    }

    private String getDefaultPrestoId()
    {
        String defaultBrokerId;
        try {
            defaultBrokerId = PRESTO_HOST_PREFIX + InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e) {
            log.error("Caught exception while getting default broker id", e);
            defaultBrokerId = PRESTO_HOST_PREFIX;
        }
        return defaultBrokerId;
    }

    public Map<ServerInstance, DataTable> queryPinotServerForDataTable(String pql, String serverHost, String segment)
    {
        long requestId = requestIdGenerator.incrementAndGet();
        BrokerRequest brokerRequest;
        try {
            brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(pql);
        }
        catch (Exception e) {
            throw new PrestoException(
                    PINOT_FAILURE_QUERYING_DATA,
                    String.format("Parsing error on requestId %d, PQL = %s", requestId, pql),
                    e);
        }

        Map<String, List<String>> routingTable = new HashMap<>();
        List<String> segmentList = new ArrayList<>();
        segmentList.add(segment);
        routingTable.put(serverHost, segmentList);
        ScatterGatherRequestImpl scatterRequest = new ScatterGatherRequestImpl(brokerRequest, routingTable, 0, connectionTimeout.toMillis(), prestoHostId);

        ScatterGatherStats scatterGatherStats = new ScatterGatherStats();
        CompositeFuture<byte[]> compositeFuture = routeScatterGather(scatterRequest, scatterGatherStats);

        if (compositeFuture == null) {
            // No server found in either OFFLINE or REALTIME table.
            return null;
        }

        Map<ServerInstance, DataTable> dataTableMap = new HashMap<>();

        List<ProcessingException> processingExceptions = new ArrayList<>();
        Map<ServerInstance, byte[]> serverResponseMap = null;
        serverResponseMap = gatherServerResponses(compositeFuture, scatterGatherStats, true, brokerRequest.getQuerySource().getTableName(), processingExceptions);
        deserializeServerResponses(serverResponseMap, true, dataTableMap, brokerRequest.getQuerySource().getTableName(), processingExceptions);
        return dataTableMap;
    }

    /**
     * Gather responses from servers, append processing exceptions to the processing exception list passed in.
     *
     * @param compositeFuture composite future returned from scatter phase.
     * @param scatterGatherStats scatter-gather statistics.
     * @param isOfflineTable whether the scatter-gather target is an OFFLINE table.
     * @param tableNameWithType table name with type suffix.
     * @param processingExceptions list of processing exceptions.
     * @return server response map.
     */
    @Nullable
    private Map<ServerInstance, byte[]> gatherServerResponses(
            @Nonnull CompositeFuture<byte[]> compositeFuture,
            @Nonnull ScatterGatherStats scatterGatherStats, boolean isOfflineTable,
            @Nonnull String tableNameWithType,
            @Nonnull List<ProcessingException> processingExceptions)
    {
        try {
            Map<ServerInstance, byte[]> serverResponseMap = compositeFuture.get();
            Iterator<Map.Entry<ServerInstance, byte[]>> iterator = serverResponseMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ServerInstance, byte[]> entry = iterator.next();
                if (entry.getValue().length == 0) {
                    throw new PrestoException(
                            PINOT_FAILURE_QUERYING_DATA,
                            String.format("Got empty data for table: %s in server %s.", tableNameWithType, entry.getKey().getShortHostName()));
                }
            }
            Map<ServerInstance, Long> responseTimes = compositeFuture.getResponseTimes();
            scatterGatherStats.setResponseTimeMillis(responseTimes, isOfflineTable);
            return serverResponseMap;
        }
        catch (Exception e) {
            brokerMetrics.addMeteredTableValue(tableNameWithType, BrokerMeter.RESPONSE_FETCH_EXCEPTIONS, 1L);
            processingExceptions.add(QueryException.getException(QueryException.BROKER_GATHER_ERROR, e));
            throw new PrestoException(
                    PINOT_FAILURE_QUERYING_DATA,
                    String.format("Caught exception while fetching responses for table: %s", tableNameWithType),
                    e);
        }
    }

    /**
     * Deserialize the server responses, put the de-serialized data table into the data table map passed in, append
     * processing exceptions to the processing exception list passed in.
     * <p>For hybrid use case, multiple responses might be from the same instance. Use response sequence to distinguish
     * them.
     *
     * @param responseMap map from server to response.
     * @param isOfflineTable whether the responses are from an OFFLINE table.
     * @param dataTableMap map from server to data table.
     * @param tableNameWithType table name with type suffix.
     * @param processingExceptions list of processing exceptions.
     */
    private void deserializeServerResponses(
            @Nonnull Map<ServerInstance, byte[]> responseMap, boolean isOfflineTable,
            @Nonnull Map<ServerInstance, DataTable> dataTableMap,
            @Nonnull String tableNameWithType,
            @Nonnull List<ProcessingException> processingExceptions)
    {
        for (Map.Entry<ServerInstance, byte[]> entry : responseMap.entrySet()) {
            ServerInstance serverInstance = entry.getKey();
            if (!isOfflineTable) {
                serverInstance = serverInstance.withSeq(1);
            }
            try {
                dataTableMap.put(serverInstance, DataTableFactory.getDataTable(entry.getValue()));
            }
            catch (Exception e) {
                brokerMetrics.addMeteredTableValue(tableNameWithType, BrokerMeter.DATA_TABLE_DESERIALIZATION_EXCEPTIONS, 1L);
                processingExceptions.add(QueryException.getException(QueryException.DATA_TABLE_DESERIALIZATION_ERROR, e));
                throw new PrestoException(
                        PINOT_FAILURE_QUERYING_DATA,
                        String.format("Caught exceptions while deserializing response for table: %s from server: %s", tableNameWithType, serverInstance),
                        e);
            }
        }
    }

    private CompositeFuture<byte[]> routeScatterGather(ScatterGatherRequestImpl scatterRequest, ScatterGatherStats scatterGatherStats)
    {
        CompositeFuture<byte[]> compositeFuture = null;
        try {
            compositeFuture = this.scatterGatherer.scatterGather(scatterRequest, scatterGatherStats, true, brokerMetrics);
        }
        catch (InterruptedException e) {
            throw new PrestoException(
                    PINOT_FAILURE_QUERYING_DATA,
                    "Caught exception querying Pinot servers.",
                    e);
        }
        return compositeFuture;
    }

    private static class ScatterGatherRequestImpl
            implements ScatterGatherRequest
    {
        private final BrokerRequest brokerRequest;
        private final Map<String, List<String>> routingTable;
        private final long requestId;
        private final long requestTimeoutMs;
        private final String brokerId;

        public ScatterGatherRequestImpl(BrokerRequest request, Map<String, List<String>> routingTable, long requestId, long requestTimeoutMs, String brokerId)
        {
            brokerRequest = request;
            this.routingTable = routingTable;
            this.requestId = requestId;

            this.requestTimeoutMs = requestTimeoutMs;
            this.brokerId = brokerId;
        }

        @Override
        public Map<String, List<String>> getRoutingTable()
        {
            return routingTable;
        }

        @Override
        public byte[] getRequestForService(List<String> segments)
        {
            InstanceRequest r = new InstanceRequest();
            r.setRequestId(requestId);
            r.setEnableTrace(brokerRequest.isEnableTrace());
            r.setQuery(brokerRequest);
            r.setSearchSegments(segments);
            r.setBrokerId(brokerId);
            return new SerDe(new TCompactProtocol.Factory()).serialize(r);
        }

        @Override
        public long getRequestId()
        {
            return requestId;
        }

        @Override
        public long getRequestTimeoutMs()
        {
            return requestTimeoutMs;
        }

        @Override
        public BrokerRequest getBrokerRequest()
        {
            return brokerRequest;
        }
    }
}
