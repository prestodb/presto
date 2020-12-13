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

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestPinotConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(PinotConfig.class)
                        .setExtraHttpHeaders("")
                        .setControllerUrls("")
                        .setIdleTimeout(new Duration(5, TimeUnit.MINUTES))
                        .setLimitLargeForSegment(PinotConfig.DEFAULT_LIMIT_LARGE_FOR_SEGMENT)
                        .setTopNLarge(PinotConfig.DEFAULT_TOPN_LARGE)
                        .setMaxBacklogPerServer(PinotConfig.DEFAULT_MAX_BACKLOG_PER_SERVER)
                        .setMaxConnectionsPerServer(PinotConfig.DEFAULT_MAX_CONNECTIONS_PER_SERVER)
                        .setMinConnectionsPerServer(PinotConfig.DEFAULT_MIN_CONNECTIONS_PER_SERVER)
                        .setThreadPoolSize(PinotConfig.DEFAULT_THREAD_POOL_SIZE)
                        .setEstimatedSizeInBytesForNonNumericColumn(20)
                        .setConnectionTimeout(new Duration(1, TimeUnit.MINUTES))
                        .setControllerRestService(null)
                        .setServiceHeaderParam("RPC-Service")
                        .setCallerHeaderValue("presto")
                        .setCallerHeaderParam("RPC-Caller")
                        .setMetadataCacheExpiry(new Duration(2, TimeUnit.MINUTES))
                        .setAllowMultipleAggregations(true)
                        .setInferDateTypeInSchema(true)
                        .setInferTimestampTypeInSchema(true)
                        .setForbidBrokerQueries(false)
                        .setUsePinotSqlForBrokerQueries(true)
                        .setRestProxyServiceForQuery(null)
                        .setRestProxyUrl(null)
                        .setNumSegmentsPerSplit(1)
                        .setFetchRetryCount(2)
                        .setMarkDataFetchExceptionsAsRetriable(true)
                        .setPushdownTopNBrokerQueries(false)
                        .setIgnoreEmptyResponses(false)
                        .setUseDateTrunc(false)
                        .setForbidSegmentQueries(false)
                        .setUseStreamingForSegmentQueries(false)
                        .setStreamingServerGrpcMaxInboundMessageBytes(PinotConfig.DEFAULT_STREAMING_SERVER_GRPC_MAX_INBOUND_MESSAGE_BYTES)
                        .setNonAggregateLimitForBrokerQueries(PinotConfig.DEFAULT_NON_AGGREGATE_LIMIT_FOR_BROKER_QUERIES)
                        .setUseDateTrunc(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("pinot.extra-http-headers", "k:v")
                .put("pinot.controller-rest-service", "pinot-controller-service")
                .put("pinot.controller-urls", "host1:1111,host2:1111")
                .put("pinot.idle-timeout", "1h")
                .put("pinot.topn-large", "1000")
                .put("pinot.max-backlog-per-server", "15")
                .put("pinot.max-connections-per-server", "10")
                .put("pinot.min-connections-per-server", "1")
                .put("pinot.thread-pool-size", "100")
                .put("pinot.estimated-size-in-bytes-for-non-numeric-column", "30")
                .put("pinot.connection-timeout", "8m")
                .put("pinot.metadata-expiry", "1m")
                .put("pinot.caller-header-value", "myCaller")
                .put("pinot.caller-header-param", "myParam")
                .put("pinot.service-header-param", "myServiceHeader")
                .put("pinot.allow-multiple-aggregations", "false")
                .put("pinot.infer-date-type-in-schema", "false")
                .put("pinot.infer-timestamp-type-in-schema", "false")
                .put("pinot.forbid-broker-queries", "true")
                .put("pinot.use-pinot-sql-for-broker-queries", "false")
                .put("pinot.rest-proxy-url", "localhost:1111")
                .put("pinot.rest-proxy-service-for-query", "pinot-rest-proxy-service")
                .put("pinot.num-segments-per-split", "2")
                .put("pinot.ignore-empty-responses", "true")
                .put("pinot.fetch-retry-count", "3")
                .put("pinot.mark-data-fetch-exceptions-as-retriable", "false")
                .put("pinot.non-aggregate-limit-for-broker-queries", "10")
                .put("pinot.use-date-trunc", "true")
                .put("pinot.limit-large-for-segment", "100")
                .put("pinot.pushdown-topn-broker-queries", "true")
                .put("pinot.forbid-segment-queries", "true")
                .put("pinot.use-streaming-for-segment-queries", "true")
                .put("pinot.streaming-server-grpc-max-inbound-message-bytes", "65536")
                .build();

        PinotConfig expected = new PinotConfig()
                .setExtraHttpHeaders("k:v")
                .setControllerRestService("pinot-controller-service")
                .setControllerUrls("host1:1111,host2:1111")
                .setRestProxyUrl("localhost:1111")
                .setIdleTimeout(new Duration(1, TimeUnit.HOURS))
                .setLimitLargeForSegment(100000)
                .setTopNLarge(1000)
                .setMaxBacklogPerServer(15)
                .setMaxConnectionsPerServer(10)
                .setMinConnectionsPerServer(1)
                .setThreadPoolSize(100)
                .setEstimatedSizeInBytesForNonNumericColumn(30)
                .setConnectionTimeout(new Duration(8, TimeUnit.MINUTES))
                .setServiceHeaderParam("myServiceHeader")
                .setCallerHeaderValue("myCaller")
                .setCallerHeaderParam("myParam")
                .setMetadataCacheExpiry(new Duration(1, TimeUnit.MINUTES))
                .setAllowMultipleAggregations(false)
                .setInferDateTypeInSchema(false)
                .setInferTimestampTypeInSchema(false)
                .setForbidBrokerQueries(true)
                .setUsePinotSqlForBrokerQueries(false)
                .setRestProxyServiceForQuery("pinot-rest-proxy-service")
                .setNumSegmentsPerSplit(2)
                .setIgnoreEmptyResponses(true)
                .setFetchRetryCount(3)
                .setMarkDataFetchExceptionsAsRetriable(false)
                .setNonAggregateLimitForBrokerQueries(10)
                .setLimitLargeForSegment(100)
                .setPushdownTopNBrokerQueries(true)
                .setForbidSegmentQueries(true)
                .setUseStreamingForSegmentQueries(true)
                .setStreamingServerGrpcMaxInboundMessageBytes(65536)
                .setUseDateTrunc(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
