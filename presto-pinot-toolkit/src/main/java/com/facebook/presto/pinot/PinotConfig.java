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

import com.facebook.airlift.configuration.Config;
import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class PinotConfig
{
    public static final int DEFAULT_LIMIT_LARGE_FOR_SEGMENT = Integer.MAX_VALUE;
    public static final int DEFAULT_MAX_BACKLOG_PER_SERVER = 30;
    public static final int DEFAULT_MAX_CONNECTIONS_PER_SERVER = 30;
    public static final int DEFAULT_MIN_CONNECTIONS_PER_SERVER = 10;
    public static final int DEFAULT_THREAD_POOL_SIZE = 30;
    public static final int DEFAULT_NON_AGGREGATE_LIMIT_FOR_BROKER_QUERIES = 25_000;
    public static final int DEFAULT_STREAMING_SERVER_GRPC_MAX_INBOUND_MESSAGE_BYTES = (int) new DataSize(128, MEGABYTE).toBytes();

    // There is a perf penalty of having a large topN since the structures are allocated to this size
    // So size this judiciously
    public static final int DEFAULT_TOPN_LARGE = 10_000;

    private static final Duration DEFAULT_IDLE_TIMEOUT = new Duration(5, TimeUnit.MINUTES);
    private static final Duration DEFAULT_CONNECTION_TIMEOUT = new Duration(1, TimeUnit.MINUTES);
    private static final int DEFAULT_ESTIMATED_SIZE_IN_BYTES_FOR_NON_NUMERIC_COLUMN = 20;

    private static final Splitter LIST_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();
    private static final MapSplitter MAP_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator(":");

    private int maxConnectionsPerServer = DEFAULT_MAX_CONNECTIONS_PER_SERVER;
    private String controllerRestService;
    private String serviceHeaderParam = "RPC-Service";
    private String callerHeaderValue = "presto";
    private String callerHeaderParam = "RPC-Caller";

    private List<String> controllerUrls = ImmutableList.of();
    private String restProxyUrl;
    private String restProxyServiceForQuery;

    private int limitLargeForSegment = DEFAULT_LIMIT_LARGE_FOR_SEGMENT;
    private int topNLarge = DEFAULT_TOPN_LARGE;

    private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    private int minConnectionsPerServer = DEFAULT_MIN_CONNECTIONS_PER_SERVER;
    private int maxBacklogPerServer = DEFAULT_MAX_BACKLOG_PER_SERVER;
    private int estimatedSizeInBytesForNonNumericColumn = DEFAULT_ESTIMATED_SIZE_IN_BYTES_FOR_NON_NUMERIC_COLUMN;
    private Map<String, String> extraHttpHeaders = ImmutableMap.of();
    private Duration metadataCacheExpiry = new Duration(2, TimeUnit.MINUTES);

    private boolean allowMultipleAggregations = true;
    private boolean forbidBrokerQueries;
    private boolean forbidSegmentQueries;

    // Infer Pinot time fields to Presto Date/Timestamp type
    private boolean inferDateTypeInSchema = true;
    private boolean inferTimestampTypeInSchema = true;

    // Retry on data fetch exceptions, e.g. timeout from the server side.
    private boolean markDataFetchExceptionsAsRetriable = true;

    // Requires Pinot version >= 0.4.0.
    private boolean usePinotSqlForBrokerQueries = true;
    // Requires Pinot version >= 0.6.0.
    private boolean useStreamingForSegmentQueries;
    private int streamingServerGrpcMaxInboundMessageBytes = DEFAULT_STREAMING_SERVER_GRPC_MAX_INBOUND_MESSAGE_BYTES;

    private int numSegmentsPerSplit = 1;
    private boolean ignoreEmptyResponses;
    private int fetchRetryCount = 2;
    private boolean useDateTrunc;
    private int nonAggregateLimitForBrokerQueries = DEFAULT_NON_AGGREGATE_LIMIT_FOR_BROKER_QUERIES;
    private boolean pushdownTopNBrokerQueries;

    @NotNull
    public Map<String, String> getExtraHttpHeaders()
    {
        return extraHttpHeaders;
    }

    @Config("pinot.extra-http-headers")
    public PinotConfig setExtraHttpHeaders(String headers)
    {
        extraHttpHeaders = ImmutableMap.copyOf(MAP_SPLITTER.split(headers));
        return this;
    }

    @NotNull
    public List<String> getControllerUrls()
    {
        return controllerUrls;
    }

    @Config("pinot.controller-urls")
    public PinotConfig setControllerUrls(String controllerUrl)
    {
        this.controllerUrls = LIST_SPLITTER.splitToList(controllerUrl);
        return this;
    }

    @Nullable
    public String getRestProxyUrl()
    {
        return restProxyUrl;
    }

    @Config("pinot.rest-proxy-url")
    public PinotConfig setRestProxyUrl(String restProxyUrl)
    {
        this.restProxyUrl = restProxyUrl;
        return this;
    }

    @Nullable
    public String getControllerRestService()
    {
        return controllerRestService;
    }

    @Config("pinot.controller-rest-service")
    public PinotConfig setControllerRestService(String controllerRestService)
    {
        this.controllerRestService = controllerRestService;
        return this;
    }

    @NotNull
    public boolean isAllowMultipleAggregations()
    {
        return allowMultipleAggregations;
    }

    @Config("pinot.allow-multiple-aggregations")
    public PinotConfig setAllowMultipleAggregations(boolean allowMultipleAggregations)
    {
        this.allowMultipleAggregations = allowMultipleAggregations;
        return this;
    }

    @NotNull
    public int getLimitLargeForSegment()
    {
        return limitLargeForSegment;
    }

    @Config("pinot.limit-large-for-segment")
    public PinotConfig setLimitLargeForSegment(int limitLargeForSegment)
    {
        this.limitLargeForSegment = limitLargeForSegment;
        return this;
    }

    @NotNull
    public int getTopNLarge()
    {
        return topNLarge;
    }

    @Config("pinot.topn-large")
    public PinotConfig setTopNLarge(int topNLarge)
    {
        this.topNLarge = topNLarge;
        return this;
    }

    @NotNull
    public int getThreadPoolSize()
    {
        return threadPoolSize;
    }

    @Config("pinot.thread-pool-size")
    public PinotConfig setThreadPoolSize(int threadPoolSize)
    {
        this.threadPoolSize = threadPoolSize;
        return this;
    }

    @NotNull
    public int getMinConnectionsPerServer()
    {
        return minConnectionsPerServer;
    }

    @Config("pinot.min-connections-per-server")
    public PinotConfig setMinConnectionsPerServer(int minConnectionsPerServer)
    {
        this.minConnectionsPerServer = minConnectionsPerServer;
        return this;
    }

    @NotNull
    public int getMaxConnectionsPerServer()
    {
        return maxConnectionsPerServer;
    }

    @Config("pinot.max-connections-per-server")
    public PinotConfig setMaxConnectionsPerServer(int maxConnectionsPerServer)
    {
        this.maxConnectionsPerServer = maxConnectionsPerServer;
        return this;
    }

    @NotNull
    public int getMaxBacklogPerServer()
    {
        return maxBacklogPerServer;
    }

    @Config("pinot.max-backlog-per-server")
    public PinotConfig setMaxBacklogPerServer(int maxBacklogPerServer)
    {
        this.maxBacklogPerServer = maxBacklogPerServer;
        return this;
    }

    @MinDuration("15s")
    @NotNull
    public Duration getIdleTimeout()
    {
        return idleTimeout;
    }

    @Config("pinot.idle-timeout")
    public PinotConfig setIdleTimeout(Duration idleTimeout)
    {
        this.idleTimeout = idleTimeout;
        return this;
    }

    @MinDuration("15s")
    @NotNull
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("pinot.connection-timeout")
    public PinotConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @MinDuration("0s")
    @NotNull
    public Duration getMetadataCacheExpiry()
    {
        return metadataCacheExpiry;
    }

    @Config("pinot.metadata-expiry")
    public PinotConfig setMetadataCacheExpiry(Duration metadataCacheExpiry)
    {
        this.metadataCacheExpiry = metadataCacheExpiry;
        return this;
    }

    @NotNull
    public int getEstimatedSizeInBytesForNonNumericColumn()
    {
        return estimatedSizeInBytesForNonNumericColumn;
    }

    @Config("pinot.estimated-size-in-bytes-for-non-numeric-column")
    public PinotConfig setEstimatedSizeInBytesForNonNumericColumn(int estimatedSizeInBytesForNonNumericColumn)
    {
        this.estimatedSizeInBytesForNonNumericColumn = estimatedSizeInBytesForNonNumericColumn;
        return this;
    }

    @NotNull
    public String getServiceHeaderParam()
    {
        return serviceHeaderParam;
    }

    @Config("pinot.service-header-param")
    public PinotConfig setServiceHeaderParam(String serviceHeaderParam)
    {
        this.serviceHeaderParam = serviceHeaderParam;
        return this;
    }

    @NotNull
    public String getCallerHeaderValue()
    {
        return callerHeaderValue;
    }

    @Config("pinot.caller-header-value")
    public PinotConfig setCallerHeaderValue(String callerHeaderValue)
    {
        this.callerHeaderValue = callerHeaderValue;
        return this;
    }

    @NotNull
    public String getCallerHeaderParam()
    {
        return callerHeaderParam;
    }

    @Config("pinot.caller-header-param")
    public PinotConfig setCallerHeaderParam(String callerHeaderParam)
    {
        this.callerHeaderParam = callerHeaderParam;
        return this;
    }

    public boolean isForbidBrokerQueries()
    {
        return forbidBrokerQueries;
    }

    @Config("pinot.forbid-broker-queries")
    public PinotConfig setForbidBrokerQueries(boolean forbidBrokerQueries)
    {
        this.forbidBrokerQueries = forbidBrokerQueries;
        return this;
    }

    public boolean isForbidSegmentQueries()
    {
        return forbidSegmentQueries;
    }

    @Config("pinot.forbid-segment-queries")
    public PinotConfig setForbidSegmentQueries(boolean forbidSegmentQueries)
    {
        this.forbidSegmentQueries = forbidSegmentQueries;
        return this;
    }

    @Nullable
    public String getRestProxyServiceForQuery()
    {
        return restProxyServiceForQuery;
    }

    @Config("pinot.rest-proxy-service-for-query")
    public PinotConfig setRestProxyServiceForQuery(String restProxyServiceForQuery)
    {
        this.restProxyServiceForQuery = restProxyServiceForQuery;
        return this;
    }

    public boolean isUseDateTrunc()
    {
        return useDateTrunc;
    }

    @Config("pinot.use-date-trunc")
    public PinotConfig setUseDateTrunc(boolean useDateTrunc)
    {
        this.useDateTrunc = useDateTrunc;
        return this;
    }

    public int getNumSegmentsPerSplit()
    {
        return this.numSegmentsPerSplit;
    }

    @Config("pinot.num-segments-per-split")
    public PinotConfig setNumSegmentsPerSplit(int numSegmentsPerSplit)
    {
        checkArgument(numSegmentsPerSplit > 0, "Number of segments per split must be more than zero");
        this.numSegmentsPerSplit = numSegmentsPerSplit;
        return this;
    }

    public boolean isIgnoreEmptyResponses()
    {
        return ignoreEmptyResponses;
    }

    @Config("pinot.ignore-empty-responses")
    public PinotConfig setIgnoreEmptyResponses(boolean ignoreEmptyResponses)
    {
        this.ignoreEmptyResponses = ignoreEmptyResponses;
        return this;
    }

    public int getFetchRetryCount()
    {
        return fetchRetryCount;
    }

    @Config("pinot.fetch-retry-count")
    public PinotConfig setFetchRetryCount(int fetchRetryCount)
    {
        this.fetchRetryCount = fetchRetryCount;
        return this;
    }

    public int getNonAggregateLimitForBrokerQueries()
    {
        return nonAggregateLimitForBrokerQueries;
    }

    @Config("pinot.non-aggregate-limit-for-broker-queries")
    public PinotConfig setNonAggregateLimitForBrokerQueries(int nonAggregateLimitForBrokerQueries)
    {
        this.nonAggregateLimitForBrokerQueries = nonAggregateLimitForBrokerQueries;
        return this;
    }

    public boolean isInferDateTypeInSchema()
    {
        return inferDateTypeInSchema;
    }

    @Config("pinot.infer-date-type-in-schema")
    public PinotConfig setInferDateTypeInSchema(boolean inferDateTypeInSchema)
    {
        this.inferDateTypeInSchema = inferDateTypeInSchema;
        return this;
    }

    public boolean isInferTimestampTypeInSchema()
    {
        return inferTimestampTypeInSchema;
    }

    @Config("pinot.infer-timestamp-type-in-schema")
    public PinotConfig setInferTimestampTypeInSchema(boolean inferTimestampTypeInSchema)
    {
        this.inferTimestampTypeInSchema = inferTimestampTypeInSchema;
        return this;
    }

    public boolean isMarkDataFetchExceptionsAsRetriable()
    {
        return markDataFetchExceptionsAsRetriable;
    }

    @Config("pinot.mark-data-fetch-exceptions-as-retriable")
    public PinotConfig setMarkDataFetchExceptionsAsRetriable(boolean markDataFetchExceptionsAsRetriable)
    {
        this.markDataFetchExceptionsAsRetriable = markDataFetchExceptionsAsRetriable;
        return this;
    }

    public boolean isUsePinotSqlForBrokerQueries()
    {
        return usePinotSqlForBrokerQueries;
    }

    @Config("pinot.use-pinot-sql-for-broker-queries")
    public PinotConfig setUsePinotSqlForBrokerQueries(boolean usePinotSqlForBrokerQueries)
    {
        this.usePinotSqlForBrokerQueries = usePinotSqlForBrokerQueries;
        return this;
    }

    public boolean isPushdownTopNBrokerQueries()
    {
        return pushdownTopNBrokerQueries;
    }

    // This is used to switch on/off push down Pinot broker queries with ORDER BY clause and LIMIT.
    // The reason is that presto doesn't retain the order of query response from Pinot for large number of records returned.
    @Config("pinot.pushdown-topn-broker-queries")
    public PinotConfig setPushdownTopNBrokerQueries(boolean pushdownTopNBrokerQueries)
    {
        this.pushdownTopNBrokerQueries = pushdownTopNBrokerQueries;
        return this;
    }

    public boolean isUseStreamingForSegmentQueries()
    {
        return useStreamingForSegmentQueries;
    }

    @Config("pinot.use-streaming-for-segment-queries")
    public PinotConfig setUseStreamingForSegmentQueries(boolean useStreamingForSegmentQueries)
    {
        this.useStreamingForSegmentQueries = useStreamingForSegmentQueries;
        return this;
    }

    public int getStreamingServerGrpcMaxInboundMessageBytes()
    {
        return streamingServerGrpcMaxInboundMessageBytes;
    }

    @Config("pinot.streaming-server-grpc-max-inbound-message-bytes")
    public PinotConfig setStreamingServerGrpcMaxInboundMessageBytes(int streamingServerGrpcMaxInboundMessageBytes)
    {
        this.streamingServerGrpcMaxInboundMessageBytes = streamingServerGrpcMaxInboundMessageBytes;
        return this;
    }
}
