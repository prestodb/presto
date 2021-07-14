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
package com.facebook.presto.plugin.prometheus;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static com.facebook.presto.plugin.prometheus.PrometheusClock.fixedClockAt;
import static com.facebook.presto.plugin.prometheus.PrometheusSplitManager.OFFSET_MILLIS;
import static com.facebook.presto.plugin.prometheus.PrometheusSplitManager.decimalSecondString;
import static com.facebook.presto.plugin.prometheus.TestPrometheusTable.TYPE_MANAGER;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPrometheusSplit
{
    //private static final Metadata METADATA = createTestMetadataManager();
    //private static final TypeManager TYPE_MANAGER = new TypeRegistry();
    private static final int NUMBER_MORE_THAN_EXPECTED_NUMBER_SPLITS = 100;
    /**
     * Prometheus mock data
     * The array below represents to a response to from real data:
     * $ curl "http://127.0.0.1:9090/api/v1/query?query=up[120s]&time=1568638172"
     * Just the time items from the "values" section of the response
     */
    private static final ImmutableList<Double> promTimeValuesMock = new ImmutableList.Builder<Double>()
            .add(1568638066.999)
            .add(1568638081.996)
            .add(1568638097.0)
            .add(1568638112.0)
            .add(1568638126.997)
            .add(1568638142.0)
            .add(1568638157.0)
            .add(1568638171.999)
            .build();
    private final PrometheusSplit split = new PrometheusSplit(URI.create("http://127.0.0.1/test.file"));
    private PrometheusHttpServer prometheusHttpServer;

    /**
     * mock Prometheus chunked query responses (time values only)
     *
     * @param splitTimes the end times that would be used for each Prometheus instant query
     * @param queryChunkDuration the duration value that would be used for each query, `30s` for instance
     * @return the values from the Prometheus data that would be return by all the chunked queries
     */
    private static List<String> mockPrometheusResponseToChunkedQueries(io.airlift.units.Duration queryChunkDuration, List<String> splitTimes)
    {
        return Lists.reverse(splitTimes).stream()
                .map(endTime -> mockPrometheusResponseToQuery(queryChunkDuration, endTime))
                .flatMap(Collection::stream)
                .sorted()
                .map(TestPrometheusSplit::doubleToPlainString)
                .collect(Collectors.toList());
    }

    /**
     * mock Prometheus instant query
     */
    private static List<Double> mockPrometheusResponseToQuery(io.airlift.units.Duration queryChunkDuration, String endTimeStr)
    {
        Double endTime = Double.valueOf(endTimeStr);
        Double duration = queryChunkDuration.getValue(TimeUnit.SECONDS);
        return promTimeValuesMock.stream()
                .filter(promTimeValue -> ((endTime - duration) <= promTimeValue) && (promTimeValue <= endTime))
                .collect(Collectors.toList());
    }

    /**
     * Convert list of Double to list of String and avoid scientific notation
     */
    private static List<String> convertMockTimesToStrings(List<Double> times)
    {
        return times.stream()
                .map(TestPrometheusSplit::doubleToPlainString)
                .collect(Collectors.toList());
    }

    /**
     * Convert Double to String and avoid scientific notation
     */
    private static String doubleToPlainString(Double aDouble)
    {
        return new BigDecimal(aDouble.toString()).stripTrailingZeros().toPlainString();
    }

    private static long longFromDecimalSecondString(String decimalString)
    {
        return new BigDecimal(decimalString).multiply(new BigDecimal(1000L)).longValueExact();
    }

    private static String getQueryChunkSizeDurationAsPrometheusCompatibleDurationString(PrometheusConnectorConfig config)
    {
        return config.getQueryChunkSizeDuration().roundTo(config.getQueryChunkSizeDuration().getUnit()) +
                io.airlift.units.Duration.timeUnitToString(config.getQueryChunkSizeDuration().getUnit());
    }

    private static PrometheusConnectorConfig getCommonConfig(URI dataUri)
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(dataUri);
        config.setMaxQueryRangeDuration(io.airlift.units.Duration.valueOf("21d"));
        config.setQueryChunkSizeDuration(io.airlift.units.Duration.valueOf("1d"));
        config.setCacheDuration(io.airlift.units.Duration.valueOf("30s"));
        return config;
    }

    @BeforeClass
    public void setUp()
    {
        prometheusHttpServer = new PrometheusHttpServer();
    }

    @Test
    public void testAddresses()
    {
        // http split with default port
        PrometheusSplit httpSplit = new PrometheusSplit(URI.create("http://prometheus.com/prometheus"));
        assertEquals(httpSplit.getAddresses(), ImmutableList.of(HostAddress.fromString("prometheus.com")));
        assertTrue(httpSplit.isRemotelyAccessible());

        // http split with custom port
        httpSplit = new PrometheusSplit(URI.create("http://prometheus.com:8080/prometheus"));
        assertEquals(httpSplit.getAddresses(), ImmutableList.of(HostAddress.fromParts("prometheus.com", 8080)));
        assertTrue(httpSplit.isRemotelyAccessible());

        // http split with default port
        PrometheusSplit httpsSplit = new PrometheusSplit(URI.create("https://prometheus.com/prometheus"));
        assertEquals(httpsSplit.getAddresses(), ImmutableList.of(HostAddress.fromString("prometheus.com")));
        assertTrue(httpsSplit.isRemotelyAccessible());

        // http split with custom port
        httpsSplit = new PrometheusSplit(URI.create("https://prometheus.com:8443/prometheus"));
        assertEquals(httpsSplit.getAddresses(), ImmutableList.of(HostAddress.fromParts("prometheus.com", 8443)));
        assertTrue(httpsSplit.isRemotelyAccessible());
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<PrometheusSplit> codec = jsonCodec(PrometheusSplit.class);
        String json = codec.toJson(split);
        PrometheusSplit copy = codec.fromJson(json);
        assertEquals(copy.getUri(), split.getUri());

        assertEquals(copy.getAddresses(), ImmutableList.of(HostAddress.fromString("127.0.0.1")));
        assertTrue(copy.isRemotelyAccessible());
    }

    @Test
    public void testQueryWithTableNameNeedingURLEncodeInSplits()
            throws URISyntaxException
    {
        Instant now = LocalDateTime.of(2019, 10, 2, 7, 26, 56, 0).toInstant(UTC);
        PrometheusConnectorConfig config = getCommonConfig(prometheusHttpServer.resolve("/prometheus-data/prom-metrics-non-standard-name.json"));
        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TYPE_MANAGER);
        PrometheusTable table = client.getTable("default", "up+now");
        PrometheusTableHandle tableHandle = new PrometheusTableHandle("default", table.getName());
        TupleDomain<ColumnHandle> columnConstraints = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        new PrometheusColumnHandle("value", BIGINT, 1), Domain.all(VARCHAR),
                        new PrometheusColumnHandle("text", createUnboundedVarcharType(), 0), Domain.all(VARCHAR)));
        PrometheusTableLayoutHandle tableLayoutHandle = new PrometheusTableLayoutHandle(tableHandle, columnConstraints);
        PrometheusSplitManager splitManager = new PrometheusSplitManager(client, fixedClockAt(now), config);
        ConnectorSplitSource splits = splitManager.getSplits(
                null,
                null,
                tableLayoutHandle,
                null);
        PrometheusSplit split = (PrometheusSplit) splits.getNextBatch(NOT_PARTITIONED, 1).getNow(null).getSplits().get(0);
        String queryInSplit = split.getUri().getQuery();
        String timeShouldBe = decimalSecondString(now.toEpochMilli() -
                config.getMaxQueryRangeDuration().toMillis() +
                config.getQueryChunkSizeDuration().toMillis() -
                OFFSET_MILLIS * 20);
        assertEquals(queryInSplit,
                new URI("http://doesnotmatter:9090/api/v1/query?query=up+now[" + getQueryChunkSizeDurationAsPrometheusCompatibleDurationString(config) + "]" + "&time=" +
                        timeShouldBe).getQuery());
    }

    @Test
    public void testQueryDividedIntoSplitsFirstSplitHasRightTime()
            throws URISyntaxException
    {
        Instant now = LocalDateTime.of(2019, 10, 2, 7, 26, 56, 0).toInstant(UTC);
        PrometheusConnectorConfig config = getCommonConfig(prometheusHttpServer.resolve("/prometheus-data/prometheus-metrics.json"));
        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TYPE_MANAGER);
        PrometheusTable table = client.getTable("default", "up");
        PrometheusTableHandle tableHandle = new PrometheusTableHandle("default", table.getName());
        TupleDomain<ColumnHandle> columnConstraints = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        new PrometheusColumnHandle("value", BIGINT, 1), Domain.all(VARCHAR),
                        new PrometheusColumnHandle("text", createUnboundedVarcharType(), 0), Domain.all(VARCHAR)));
        PrometheusTableLayoutHandle tableLayoutHandle = new PrometheusTableLayoutHandle(tableHandle, columnConstraints);
        PrometheusSplitManager splitManager = new PrometheusSplitManager(client, fixedClockAt(now), config);
        ConnectorSplitSource splits = splitManager.getSplits(
                null,
                null,
                tableLayoutHandle,
                null);
        PrometheusSplit split = (PrometheusSplit) splits.getNextBatch(NOT_PARTITIONED, 1).getNow(null).getSplits().get(0);
        String queryInSplit = split.getUri().getQuery();
        String timeShouldBe = decimalSecondString(now.toEpochMilli() -
                config.getMaxQueryRangeDuration().toMillis() +
                config.getQueryChunkSizeDuration().toMillis() -
                OFFSET_MILLIS * 20);
        assertEquals(queryInSplit,
                new URI("http://doesnotmatter:9090/api/v1/query?query=up[" + getQueryChunkSizeDurationAsPrometheusCompatibleDurationString(config) + "]" + "&time=" +
                        timeShouldBe).getQuery());
    }

    @Test
    public void testQueryDividedIntoSplitsLastSplitHasRightTime()
            throws URISyntaxException
    {
        Instant now = LocalDateTime.of(2019, 10, 2, 7, 26, 56, 0).toInstant(UTC);
        PrometheusConnectorConfig config = getCommonConfig(prometheusHttpServer.resolve("/prometheus-data/prometheus-metrics.json"));
        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TYPE_MANAGER);
        PrometheusTable table = client.getTable("default", "up");
        PrometheusTableHandle tableHandle = new PrometheusTableHandle("default", table.getName());
        TupleDomain<ColumnHandle> columnConstraints = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        new PrometheusColumnHandle("value", BIGINT, 1), Domain.all(VARCHAR),
                        new PrometheusColumnHandle("text", createUnboundedVarcharType(), 0), Domain.all(VARCHAR)));
        PrometheusTableLayoutHandle tableLayoutHandle = new PrometheusTableLayoutHandle(tableHandle, columnConstraints);
        PrometheusSplitManager splitManager = new PrometheusSplitManager(client, fixedClockAt(now), config);
        ConnectorSplitSource splitsMaybe = splitManager.getSplits(
                null,
                null,
                tableLayoutHandle,
                null);
        List<ConnectorSplit> splits = splitsMaybe.getNextBatch(NOT_PARTITIONED, NUMBER_MORE_THAN_EXPECTED_NUMBER_SPLITS).getNow(null).getSplits();
        int lastSplitIndex = splits.size() - 1;
        PrometheusSplit lastSplit = (PrometheusSplit) splits.get(lastSplitIndex);
        String queryInSplit = lastSplit.getUri().getQuery();
        String timeShouldBe = decimalSecondString(now.toEpochMilli());
        URI uriAsFormed = new URI("http://doesnotmatter:9090/api/v1/query?query=up[" +
                getQueryChunkSizeDurationAsPrometheusCompatibleDurationString(config) + "]" +
                "&time=" + timeShouldBe);
        assertEquals(queryInSplit, uriAsFormed.getQuery());
    }

    @Test
    public void testQueryDividedIntoSplitsShouldHaveCorrectSpacingBetweenTimes()
    {
        Instant now = LocalDateTime.of(2019, 10, 2, 7, 26, 56, 0).toInstant(UTC);
        PrometheusConnectorConfig config = getCommonConfig(prometheusHttpServer.resolve("/prometheus-data/prometheus-metrics.json"));
        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TYPE_MANAGER);
        PrometheusTable table = client.getTable("default", "up");
        PrometheusTableHandle tableHandle = new PrometheusTableHandle("default", table.getName());
        TupleDomain<ColumnHandle> columnConstraints = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        new PrometheusColumnHandle("value", BIGINT, 1), Domain.all(VARCHAR),
                        new PrometheusColumnHandle("text", createUnboundedVarcharType(), 0), Domain.all(VARCHAR)));
        PrometheusTableLayoutHandle tableLayoutHandle = new PrometheusTableLayoutHandle(tableHandle, columnConstraints);
        PrometheusSplitManager splitManager = new PrometheusSplitManager(client, fixedClockAt(now), config);
        ConnectorSplitSource splits = splitManager.getSplits(
                null,
                null,
                tableLayoutHandle,
                null);
        PrometheusSplit split1 = (PrometheusSplit) splits.getNextBatch(NOT_PARTITIONED, 1).getNow(null).getSplits().get(0);
        Map<String, String> paramsMap1 = new HashMap<>();
        String[] splitKV1 = split1.getUri().getQuery().split("&");
        paramsMap1.put("query", splitKV1[0].split("=")[1]);
        paramsMap1.put("time", splitKV1[1].split("=")[1]);
        assertEquals(paramsMap1.get("query"), "up[1d]");

        PrometheusSplit split2 = (PrometheusSplit) splits.getNextBatch(NOT_PARTITIONED, 1).getNow(null).getSplits().get(0);
        Map<String, String> paramsMap2 = new HashMap<>();
        String[] splitKV2 = split2.getUri().getQuery().split("&");
        paramsMap2.put("query", splitKV2[0].split("=")[1]);
        paramsMap2.put("time", splitKV2[1].split("=")[1]);
        assertEquals(paramsMap2.get("query"), "up[1d]");
        long diff = Double.valueOf(paramsMap2.get("time")).longValue() - Double.valueOf(paramsMap1.get("time")).longValue();
        assertEquals(config.getQueryChunkSizeDuration().getValue(TimeUnit.SECONDS), diff, 0.0001);
    }

    @Test
    public void testSplitTimesCorrect()
    {
        io.airlift.units.Duration maxQueryRangeDuration = new io.airlift.units.Duration(3, TimeUnit.DAYS);
        io.airlift.units.Duration queryChunkSizeDuration = new io.airlift.units.Duration(1, TimeUnit.DAYS);
        Instant now = ofEpochMilli(1000000000L);

        PrometheusTableHandle prometheusTableHandle = new PrometheusTableHandle("schemaName", "tableName");
        List<String> splitTimes = PrometheusSplitManager.generateTimesForSplits(
                now,
                maxQueryRangeDuration, queryChunkSizeDuration, prometheusTableHandle);
        List<String> expectedSplitTimes = ImmutableList.of(
                "827199.998", "913599.999", "1000000");
        assertEquals(splitTimes, expectedSplitTimes);
    }

    @Test
    public void testSplitTimesCorrectNonModuloZeroDurationToChunk()
    {
        io.airlift.units.Duration maxQueryRangeDuration = new io.airlift.units.Duration(3, TimeUnit.DAYS);
        io.airlift.units.Duration queryChunkSizeDuration = new io.airlift.units.Duration(2, TimeUnit.DAYS);
        Instant now = ofEpochMilli(1000000000L);

        PrometheusTableHandle prometheusTableHandle = new PrometheusTableHandle("schemaName", "tableName");
        List<String> splitTimes = PrometheusSplitManager.generateTimesForSplits(now, maxQueryRangeDuration, queryChunkSizeDuration, prometheusTableHandle);
        List<String> expectedSplitTimes = ImmutableList.of(
                "827199.999", "1000000");
        assertEquals(splitTimes, expectedSplitTimes);
    }

    @Test
    public void testSplitTimesCorrectVersusMock()
    {
        io.airlift.units.Duration maxQueryRangeDuration = new io.airlift.units.Duration(120, TimeUnit.SECONDS);
        io.airlift.units.Duration queryChunkSizeDuration = new io.airlift.units.Duration(30, TimeUnit.SECONDS);
        Instant now = ofEpochMilli(1568638172000L);

        PrometheusTableHandle prometheusTableHandle = new PrometheusTableHandle("schemaName", "tableName");
        List<String> splitTimes = PrometheusSplitManager.generateTimesForSplits(now, maxQueryRangeDuration, queryChunkSizeDuration, prometheusTableHandle);
        List<String> promTimesReturned = mockPrometheusResponseToChunkedQueries(queryChunkSizeDuration, splitTimes);
        assertEquals(promTimesReturned, convertMockTimesToStrings(promTimeValuesMock));
    }

    @Test
    public void testSplitTimesAreTimesNearBoundaryNotMissing()
    {
        io.airlift.units.Duration maxQueryRangeDuration = new io.airlift.units.Duration(120, TimeUnit.SECONDS);
        io.airlift.units.Duration queryChunkSizeDuration = new io.airlift.units.Duration(30, TimeUnit.SECONDS);
        Instant now = ofEpochMilli(1568638171999L);

        PrometheusTableHandle prometheusTableHandle = new PrometheusTableHandle("schemaName", "tableName");
        List<String> splitTimes = PrometheusSplitManager.generateTimesForSplits(now, maxQueryRangeDuration, queryChunkSizeDuration, prometheusTableHandle);
        List<String> promTimesReturned = mockPrometheusResponseToChunkedQueries(queryChunkSizeDuration, splitTimes);
        assertEquals(promTimesReturned, convertMockTimesToStrings(promTimeValuesMock));
    }

    @Test
    public void testMockPrometheusResponseShouldBeCorrectWhenUpperBoundaryAlignsWithData()
    {
        List<Double> expectedResponse = ImmutableList.of(1568638142.0, 1568638157.0, 1568638171.999);
        assertEquals(mockPrometheusResponseToQuery(new io.airlift.units.Duration(30, TimeUnit.SECONDS), "1568638171.999"), expectedResponse);
    }

    @Test
    public void testMockPrometheusResponseShouldBeCorrectWhenLowerBoundaryAlignsWithData()
    {
        List<Double> expectedResponse = ImmutableList.of(1568638142.0, 1568638157.0, 1568638171.999);
        assertEquals(mockPrometheusResponseToQuery(new io.airlift.units.Duration(30, TimeUnit.SECONDS), "1568638172."), expectedResponse);
    }

    @Test
    public void testMockPrometheusResponseShouldBeCorrectWhenLowerBoundaryLaterThanData()
    {
        List<Double> expectedResponse = ImmutableList.of(1568638157.0, 1568638171.999);
        assertEquals(mockPrometheusResponseToQuery(new io.airlift.units.Duration(30, TimeUnit.SECONDS), "1568638172.001"), expectedResponse);
    }

    @Test
    public void testMockPrometheusResponseWithSeveralChunksShouldBeCorrect()
    {
        List<String> expectedResponse = ImmutableList.of("1568638112", "1568638126.997", "1568638142", "1568638157", "1568638171.999");
        List<String> splitTimes = ImmutableList.of("1568638141.999", "1568638172.");
        assertEquals(mockPrometheusResponseToChunkedQueries(new io.airlift.units.Duration(30, TimeUnit.SECONDS), splitTimes), expectedResponse);
    }

    @Test
    public void testPredicatePushDownLowerBoundDirect()
    {
        Range lowRange = Range.greaterThanOrEqual(TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(1570460709643L, UTC_KEY));
        ValueSet valueSet = ValueSet.ofRanges(lowRange);
        Domain testDomain = Domain.create(valueSet, false);
        TupleDomain<ColumnHandle> testTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                new PrometheusColumnHandle("timestamp", TIMESTAMP_WITH_TIME_ZONE, 2), testDomain));
        PrometheusPredicateTimeInfo predicateTimes = PrometheusSplitManager.determinePredicateTimes(testTupleDomain).orElseThrow(
                () -> new AssertionError("predicate pushdown error on Prometheus Column"));
        Instant expected = ofEpochMilli(1570460709643L);
        assertEquals(predicateTimes.getPredicateLowerTimeBound().orElseThrow(
                () -> new AssertionError("predicate pushdown error on Prometheus Column")
        ), expected);
    }

    @Test
    public void testPredicatePushDownSetsLowerBoundOnly()
    {
        long predicateLowValue = 1568638171999L - 600000L;
        Range lowRange = Range.greaterThanOrEqual(TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(predicateLowValue, UTC_KEY));
        ValueSet valueSet = ValueSet.ofRanges(lowRange);
        Domain testDomain = Domain.create(valueSet, false);
        TupleDomain<ColumnHandle> testTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                new PrometheusColumnHandle("timestamp", TIMESTAMP_WITH_TIME_ZONE, 2), testDomain));
        PrometheusTableHandle prometheusTableHandle = new PrometheusTableHandle("schemaName", "tableName")
                .withPredicate(testTupleDomain);
        io.airlift.units.Duration maxQueryRangeDuration = new io.airlift.units.Duration(120, TimeUnit.SECONDS);
        io.airlift.units.Duration queryChunkSizeDuration = new io.airlift.units.Duration(30, TimeUnit.SECONDS);
        Instant now = ofEpochMilli(1568638171999L);
        TemporalAmount maxQueryAsTime = java.time.Duration.ofMillis(maxQueryRangeDuration.toMillis());
        List<String> splitTimes = PrometheusSplitManager.generateTimesForSplits(now, maxQueryRangeDuration, queryChunkSizeDuration, prometheusTableHandle);

        String earliestSplit = splitTimes.get(0);
        Instant earliestSplitAsTime = ofEpochMilli(longFromDecimalSecondString(earliestSplit));
        TemporalAmount queryChunkAsTime = java.time.Duration.ofMillis(queryChunkSizeDuration.toMillis());
        Instant startOfQuery = earliestSplitAsTime.minus(queryChunkAsTime);
        assertNotEquals(startOfQuery, now.minus(maxQueryAsTime).minus(java.time.Duration.ofMillis((splitTimes.size() - 1) * OFFSET_MILLIS)));
        assertEquals(startOfQuery.toEpochMilli(), ofEpochMilli(predicateLowValue).toEpochMilli() - ((splitTimes.size() - 1) * OFFSET_MILLIS));
    }

    @Test
    public void testPredicatePushDownSetsUpperBoundOnly()
    {
        long predicateHighValue = 1568638171999L;
        Range highRange = Range.lessThanOrEqual(TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(predicateHighValue, UTC_KEY));
        ValueSet valueSet = ValueSet.ofRanges(highRange);

        Domain testDomain = Domain.create(valueSet, false);
        TupleDomain<ColumnHandle> testTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                new PrometheusColumnHandle("timestamp", TIMESTAMP_WITH_TIME_ZONE, 2), testDomain));
        PrometheusTableHandle prometheusTableHandle = new PrometheusTableHandle("schemaName", "tableName")
                .withPredicate(testTupleDomain);
        io.airlift.units.Duration maxQueryRangeDuration = new io.airlift.units.Duration(120, TimeUnit.SECONDS);
        io.airlift.units.Duration queryChunkSizeDuration = new io.airlift.units.Duration(30, TimeUnit.SECONDS);
        Instant now = ofEpochMilli(1568638171999L + 600000L);

        List<String> splitTimes = PrometheusSplitManager.generateTimesForSplits(now, maxQueryRangeDuration, queryChunkSizeDuration, prometheusTableHandle);

        TemporalAmount expectedMaxQueryAsTime = java.time.Duration.ofMillis(maxQueryRangeDuration.toMillis() +
                ((splitTimes.size() - 1) * OFFSET_MILLIS));
        String lastSplit = splitTimes.get(splitTimes.size() - 1);
        Instant lastSplitAsTime = ofEpochMilli(longFromDecimalSecondString(lastSplit));
        String earliestSplit = splitTimes.get(0);
        Instant earliestSplitAsTime = ofEpochMilli(longFromDecimalSecondString(earliestSplit));
        TemporalAmount queryChunkAsTime = java.time.Duration.ofMillis(queryChunkSizeDuration.toMillis());
        java.time.Duration actualMaxDuration = Duration.between(earliestSplitAsTime
                .minus(queryChunkAsTime), lastSplitAsTime);

        assertEquals(lastSplitAsTime.toEpochMilli(), 1568638171999L);
        assertEquals(actualMaxDuration, expectedMaxQueryAsTime);
    }

    @Test
    public void testPredicatePushDownSetsUpperAndLowerBound()
    {
        long predicateHighValue = 1568638171999L;
        Range highRange = Range.equal(TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(predicateHighValue, UTC_KEY));
        long predicateLowValue = 1568638171999L - 600000L;
        Range lowRange = Range.equal(TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(predicateLowValue, UTC_KEY));
        ValueSet valueSet = ValueSet.ofRanges(lowRange, highRange);

        Domain testDomain = Domain.create(valueSet, false);
        TupleDomain<ColumnHandle> testTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                new PrometheusColumnHandle("timestamp", TIMESTAMP_WITH_TIME_ZONE, 2), testDomain));
        PrometheusTableHandle prometheusTableHandle = new PrometheusTableHandle("schemaName", "tableName")
                .withPredicate(testTupleDomain);
        io.airlift.units.Duration maxQueryRangeDuration = new io.airlift.units.Duration(120, TimeUnit.SECONDS);
        io.airlift.units.Duration queryChunkSizeDuration = new io.airlift.units.Duration(30, TimeUnit.SECONDS);
        Instant now = ofEpochMilli(1568638171999L + 1200000L);

        List<String> splitTimes = PrometheusSplitManager.generateTimesForSplits(now, maxQueryRangeDuration, queryChunkSizeDuration, prometheusTableHandle);

        TemporalAmount expectedMaxQueryAsTime = java.time.Duration.ofMillis(new io.airlift.units.Duration(10, TimeUnit.MINUTES).toMillis() +
                ((splitTimes.size() - 1) * OFFSET_MILLIS));
        String lastSplit = splitTimes.get(splitTimes.size() - 1);
        Instant lastSplitAsTime = ofEpochMilli(longFromDecimalSecondString(lastSplit));
        String earliestSplit = splitTimes.get(0);
        Instant earliestSplitAsTime = ofEpochMilli(longFromDecimalSecondString(earliestSplit));
        TemporalAmount queryChunkAsTime = java.time.Duration.ofMillis(queryChunkSizeDuration.toMillis());
        java.time.Duration actualMaxDuration = Duration.between(earliestSplitAsTime
                .minus(queryChunkAsTime), lastSplitAsTime);

        assertEquals(lastSplitAsTime.toEpochMilli(), 1568638171999L);
        assertEquals(actualMaxDuration, expectedMaxQueryAsTime);
    }

    @Test
    public void testEmptyPredicatePredicatePushDown()
    {
        long predicateLowValue = 1570460709643L;
        PrometheusTableHandle prometheusTableHandle = new PrometheusTableHandle("schemaName", "tableName");
        io.airlift.units.Duration maxQueryRangeDuration = new io.airlift.units.Duration(120, TimeUnit.SECONDS);
        io.airlift.units.Duration queryChunkSizeDuration = new io.airlift.units.Duration(30, TimeUnit.SECONDS);
        Instant now = ofEpochMilli(1568638171999L);
        TemporalAmount maxQueryAsTime = java.time.Duration.ofMillis(maxQueryRangeDuration.toMillis());
        List<String> splitTimes = PrometheusSplitManager.generateTimesForSplits(now, maxQueryRangeDuration, queryChunkSizeDuration, prometheusTableHandle);

        String earliestSplit = splitTimes.get(0);
        Instant earliestSplitAsTime = ofEpochMilli(longFromDecimalSecondString(earliestSplit));
        TemporalAmount queryChunkAsTime = java.time.Duration.ofMillis(queryChunkSizeDuration.toMillis());
        Instant startOfQuery = earliestSplitAsTime.minus(queryChunkAsTime);
        assertEquals(startOfQuery, now.minus(maxQueryAsTime).minus(java.time.Duration.ofMillis((splitTimes.size() - 1) * OFFSET_MILLIS)));
        assertNotEquals(startOfQuery.toEpochMilli(), ofEpochMilli(predicateLowValue).toEpochMilli());
    }
}
