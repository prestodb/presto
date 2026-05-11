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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.http.client.testing.TestingResponse;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.scheduler.DynamicFilterService;
import com.facebook.presto.execution.scheduler.DynamicFilterStats;
import com.facebook.presto.execution.scheduler.JoinDynamicFilter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.testing.TestingResponse.contentType;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDynamicFilterFetcher
{
    private static final Duration POLL_TIMEOUT = new Duration(100, MILLISECONDS);
    private static final Duration FAIL_TIMEOUT = new Duration(20, SECONDS);
    private static final long DEFAULT_MAX_SIZE_BYTES = 1_048_576L; // 1 MB

    private JsonCodec<DynamicFilterResponse> codec;
    private DefaultEventLoopGroup eventLoopGroup;
    private EventLoop eventLoop;
    private DynamicFilterFetcher fetcher;

    @BeforeClass
    public void setupCodec()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                binder -> {
                    FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
                    binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
                    binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class);
                    jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
                    jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
                    jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
                    newSetBinder(binder, Type.class);
                    jsonCodecBinder(binder).bindJsonCodec(DynamicFilterResponse.class);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        codec = injector.getInstance(new Key<JsonCodec<DynamicFilterResponse>>() {});
    }

    @BeforeMethod
    public void setup()
    {
        eventLoopGroup = new DefaultEventLoopGroup(1);
        eventLoop = eventLoopGroup.next();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup()
            throws Exception
    {
        if (fetcher != null) {
            fetcher.stop();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(0, 0, MILLISECONDS).sync();
        }
    }

    @Test(timeOut = 30000)
    public void testStopsPollingAfterTooManyFailures()
            throws Exception
    {
        AtomicInteger requestCount = new AtomicInteger();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            requestCount.incrementAndGet();
            throw new IOException("Connection refused");
        });

        DynamicFilterFetcher fetcher = createFetcher(httpClient, new Duration(1, MILLISECONDS));
        fetcher.start();

        // Backoff.MIN_RETRIES is 3, maxErrorDuration is 1ms.
        // After 3+ failures exceeding the duration, errorTracker.requestFailed()
        // throws PrestoException(TOO_MANY_REQUESTS_FAILED) which the fetcher
        // catches and calls stop().
        poll(() -> requestCount.get() >= 3);

        // Allow the event loop to process the final failure and stop
        Thread.sleep(500);
        int countAfterStop = requestCount.get();
        assertTrue(countAfterStop >= 3, "Expected at least 3 requests (Backoff.MIN_RETRIES), got " + countAfterStop);

        Thread.sleep(500);
        assertEquals(requestCount.get(), countAfterStop,
                "Fetcher should have stopped polling after too many failures");
    }

    @Test(timeOut = 30000)
    public void testStopsPollingOnFatalError()
            throws Exception
    {
        AtomicInteger requestCount = new AtomicInteger();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            requestCount.incrementAndGet();
            return new TestingResponse(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    ImmutableListMultimap.of(),
                    "{}".getBytes());
        });

        // Use a long maxErrorDuration — the fatal path doesn't go through Backoff
        DynamicFilterFetcher fetcher = createFetcher(httpClient, new Duration(30, SECONDS));
        fetcher.start();

        poll(() -> requestCount.get() >= 1);
        Thread.sleep(500);
        int countAfterFatal = requestCount.get();

        Thread.sleep(500);
        assertEquals(requestCount.get(), countAfterFatal,
                "Fetcher should have stopped immediately on fatal error");
        assertEquals(countAfterFatal, 1,
                "Fatal error should stop after first request");
    }

    @Test(timeOut = 30000)
    public void testRetriesTransientFailureThenContinues()
            throws Exception
    {
        AtomicInteger requestCount = new AtomicInteger();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            int count = requestCount.incrementAndGet();
            if (count <= 2) {
                throw new IOException("Transient failure");
            }
            byte[] body = codec.toJsonBytes(DynamicFilterResponse.incomplete(ImmutableMap.of(), (long) count));
            return new TestingResponse(OK, contentType(JSON_UTF_8), body);
        });

        // maxErrorDuration long enough that 2 failures don't exhaust it
        DynamicFilterFetcher fetcher = createFetcher(httpClient, new Duration(30, SECONDS));
        fetcher.start();

        poll(() -> requestCount.get() >= 5);
    }

    @Test(timeOut = 30000)
    public void testPerFilterNoneDelivery()
            throws Exception
    {
        String filterIdA = "filterA";
        String filterIdB = "filterB";
        QueryId queryId = new QueryId("test");

        DynamicFilterService dynamicFilterService = new DynamicFilterService();
        JoinDynamicFilter filterA = new JoinDynamicFilter(
                filterIdA, "col1", new Duration(10, SECONDS), DEFAULT_MAX_SIZE_BYTES, new DynamicFilterStats(), new RuntimeStats(), false);
        filterA.setExpectedPartitions(1);
        dynamicFilterService.registerFilter(queryId, filterIdA, filterA);

        JoinDynamicFilter filterB = new JoinDynamicFilter(
                filterIdB, "col2", new Duration(10, SECONDS), DEFAULT_MAX_SIZE_BYTES, new DynamicFilterStats(), new RuntimeStats(), false);
        filterB.setExpectedPartitions(1);
        dynamicFilterService.registerFilter(queryId, filterIdB, filterB);

        DynamicFilterResponse response = new DynamicFilterResponse(
                ImmutableMap.of(),
                1L,
                false,
                ImmutableSet.of(filterIdA));

        AtomicInteger fetchCount = new AtomicInteger();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            if ("DELETE".equals(request.getMethod())) {
                return new TestingResponse(OK, ImmutableListMultimap.of(), new byte[0]);
            }
            fetchCount.incrementAndGet();
            return new TestingResponse(OK, contentType(JSON_UTF_8), codec.toJsonBytes(response));
        });

        fetcher = createFetcher(httpClient, new Duration(30, SECONDS), dynamicFilterService);
        fetcher.start();

        poll(() -> fetchCount.get() >= 3);

        assertTrue(filterA.hasData(),
                "Filter A should have received none() — its pipeline flushed");

        assertFalse(filterB.hasData(),
                "Filter B should not have received data — its pipeline hasn't flushed");
    }

    @Test(timeOut = 30000)
    public void testCompletedFilterWithNoDataDeliversAll()
            throws Exception
    {
        String filterId = "f1";
        QueryId queryId = new QueryId("test");

        DynamicFilterService dynamicFilterService = new DynamicFilterService();
        JoinDynamicFilter joinFilter = new JoinDynamicFilter(
                filterId, "col1", new Duration(10, SECONDS), DEFAULT_MAX_SIZE_BYTES, new DynamicFilterStats(), new RuntimeStats(), false);
        joinFilter.setExpectedPartitions(1);
        dynamicFilterService.registerFilter(queryId, filterId, joinFilter);

        // Operator sent all() (predicate too large); flush converts to per-filter Domain.all()
        TupleDomain<String> allDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(filterId, Domain.all(BIGINT)));
        DynamicFilterResponse response = DynamicFilterResponse.completed(
                ImmutableMap.of(filterId, allDomain),
                1L,
                ImmutableSet.of(filterId));

        AtomicInteger fetchCount = new AtomicInteger();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            if ("DELETE".equals(request.getMethod())) {
                return new TestingResponse(OK, ImmutableListMultimap.of(), new byte[0]);
            }
            fetchCount.incrementAndGet();
            return new TestingResponse(OK, contentType(JSON_UTF_8), codec.toJsonBytes(response));
        });

        fetcher = createFetcher(httpClient, new Duration(30, SECONDS), dynamicFilterService);
        fetcher.start();

        poll(() -> fetchCount.get() >= 1);
        Thread.sleep(500);

        assertTrue(joinFilter.hasData(), "Filter should have received data");
        assertTrue(joinFilter.isComplete(), "Filter should be complete (1 of 1 partitions)");

        TupleDomain<String> constraint = joinFilter.getCurrentConstraintByColumnName();
        assertTrue(constraint.isAll(),
                "Constraint should be all() (let everything through), but was: " + constraint);
    }

    @Test(timeOut = 30000)
    public void testSecondFilterLostWhenFirstFlushSetsOperatorCompleted()
            throws Exception
    {
        String filterIdA = "f1";
        String filterIdB = "f2";
        QueryId queryId = new QueryId("test");

        DynamicFilterService dynamicFilterService = new DynamicFilterService();
        JoinDynamicFilter filterA = new JoinDynamicFilter(
                filterIdA, "col1", new Duration(10, SECONDS), DEFAULT_MAX_SIZE_BYTES, new DynamicFilterStats(), new RuntimeStats(), false);
        filterA.setExpectedPartitions(1);
        dynamicFilterService.registerFilter(queryId, filterIdA, filterA);

        JoinDynamicFilter filterB = new JoinDynamicFilter(
                filterIdB, "col2", new Duration(10, SECONDS), DEFAULT_MAX_SIZE_BYTES, new DynamicFilterStats(), new RuntimeStats(), false);
        filterB.setExpectedPartitions(1);
        dynamicFilterService.registerFilter(queryId, filterIdB, filterB);

        TupleDomain<String> domainA = TupleDomain.withColumnDomains(
                ImmutableMap.of(filterIdA, Domain.singleValue(BIGINT, 42L)));
        DynamicFilterResponse firstResponse = DynamicFilterResponse.incomplete(
                ImmutableMap.of(filterIdA, domainA),
                1L);

        TupleDomain<String> domainB = TupleDomain.withColumnDomains(
                ImmutableMap.of(filterIdB, Domain.singleValue(BIGINT, 99L)));
        DynamicFilterResponse secondResponse = DynamicFilterResponse.completed(
                ImmutableMap.of(filterIdB, domainB),
                2L,
                ImmutableSet.of(filterIdA, filterIdB));

        AtomicInteger fetchCount = new AtomicInteger();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            if ("DELETE".equals(request.getMethod())) {
                return new TestingResponse(OK, ImmutableListMultimap.of(), new byte[0]);
            }
            int count = fetchCount.incrementAndGet();
            if (count == 1) {
                return new TestingResponse(OK, contentType(JSON_UTF_8), codec.toJsonBytes(firstResponse));
            }
            return new TestingResponse(OK, contentType(JSON_UTF_8), codec.toJsonBytes(secondResponse));
        });

        fetcher = createFetcher(httpClient, new Duration(30, SECONDS), dynamicFilterService);
        fetcher.start();

        poll(() -> fetchCount.get() >= 1);
        Thread.sleep(1000);

        assertTrue(filterA.hasData(), "Filter A should have received data");
        assertTrue(filterB.hasData(),
                "Filter B should have received data, but fetcher stopped after first pipeline's flush");
    }

    @Test(timeOut = 30000)
    public void testStopsPollingWhenFiltersArriveWithOperatorCompletion()
            throws Exception
    {
        String filterId = "f1";
        QueryId queryId = new QueryId("test");

        DynamicFilterService dynamicFilterService = new DynamicFilterService();
        JoinDynamicFilter joinFilter = new JoinDynamicFilter(
                filterId, "col1", new Duration(10, SECONDS), DEFAULT_MAX_SIZE_BYTES, new DynamicFilterStats(), new RuntimeStats(), false);
        joinFilter.setExpectedPartitions(2);
        dynamicFilterService.registerFilter(queryId, filterId, joinFilter);

        TupleDomain<String> filterDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(filterId, Domain.singleValue(BIGINT, 42L)));
        DynamicFilterResponse completionResponse = DynamicFilterResponse.completed(
                ImmutableMap.of(filterId, filterDomain),
                1L,
                ImmutableSet.of(filterId));

        AtomicInteger fetchCount = new AtomicInteger();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            if ("DELETE".equals(request.getMethod())) {
                return new TestingResponse(OK, ImmutableListMultimap.of(), new byte[0]);
            }
            int count = fetchCount.incrementAndGet();
            if (count == 1) {
                return new TestingResponse(OK, contentType(JSON_UTF_8), codec.toJsonBytes(completionResponse));
            }
            return new TestingResponse(OK, contentType(JSON_UTF_8),
                    codec.toJsonBytes(DynamicFilterResponse.incomplete(ImmutableMap.of(), (long) count)));
        });

        fetcher = createFetcher(httpClient, new Duration(30, SECONDS), dynamicFilterService);
        fetcher.start();

        poll(() -> fetchCount.get() >= 1);
        Thread.sleep(500);

        assertEquals(fetchCount.get(), 1,
                "Fetcher should stop after processing filters with operator completion");

        assertFalse(joinFilter.isComplete(),
                "Filter should not be complete — only 1 of 2 expected partitions received");
        assertTrue(joinFilter.hasData(), "Filter should have data from the real partition");
    }

    /**
     * Verifies that a TupleDomain with a RANGE (min != max) round-trips
     * correctly through DynamicFilterResponse JSON serialization. This is the
     * code path the C++ HashBuild callback uses when VectorHasher overflows
     * discrete tracking and falls back to min/max range. The C++ sends:
     *
     *   {"filters": {"filterId": {"columnDomains": [{"column": "filterId",
     *      "domain": {"values": {"@type": "sortable", "type": "integer",
     *        "ranges": [{"low": {"type": "integer", "valueBlock": "...",
     *        "bound": "EXACTLY"}, "high": {"type": "integer",
     *        "valueBlock": "...", "bound": "EXACTLY"}}]},
     *      "nullAllowed": false}}]}}}
     *
     * The Marker.valueBlock is a base64-encoded Presto Block with a single
     * integer value. If the Block round-trips with getValue() == null, the
     * coordinator crashes with NPE in JoinDynamicFilter.addPartitionByFilterId.
     */
    @Test(timeOut = 30000)
    public void testFilterWithDataAndCompletedIdMarksAsFinal()
            throws Exception
    {
        // A response that carries the filter id in BOTH `filters` and
        // `completedFilterIds` should mark the contribution as final.
        // expectedPartitions=1 + isFinal=true should resolve the filter.
        String filterId = "f1";
        QueryId queryId = new QueryId("test");

        DynamicFilterService dynamicFilterService = new DynamicFilterService();
        JoinDynamicFilter joinFilter = new JoinDynamicFilter(
                filterId, "col1", new Duration(10, SECONDS), DEFAULT_MAX_SIZE_BYTES, new DynamicFilterStats(), new RuntimeStats(), false);
        joinFilter.setExpectedPartitions(1);
        dynamicFilterService.registerFilter(queryId, filterId, joinFilter);

        TupleDomain<String> filterDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(filterId, Domain.singleValue(BIGINT, 42L)));
        DynamicFilterResponse response = DynamicFilterResponse.completed(
                ImmutableMap.of(filterId, filterDomain),
                1L,
                ImmutableSet.of(filterId));

        AtomicInteger fetchCount = new AtomicInteger();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            if ("DELETE".equals(request.getMethod())) {
                return new TestingResponse(OK, ImmutableListMultimap.of(), new byte[0]);
            }
            fetchCount.incrementAndGet();
            return new TestingResponse(OK, contentType(JSON_UTF_8), codec.toJsonBytes(response));
        });

        fetcher = createFetcher(httpClient, new Duration(30, SECONDS), dynamicFilterService);
        fetcher.start();

        poll(() -> fetchCount.get() >= 1);
        Thread.sleep(500);

        assertTrue(joinFilter.isComplete(),
                "Filter should complete when delivered with isFinal=true");
        TupleDomain<String> constraint = joinFilter.getCurrentConstraintByColumnName();
        assertFalse(constraint.isAll(), "Constraint should reflect the delivered domain");
    }

    @Test(timeOut = 30000)
    public void testFilterDeliveredOncePerTaskAcrossMultiplePolls()
            throws Exception
    {
        // First response delivers the filter; second response repeats it via
        // completedFilterIds only. The fetcher's per-task dedup must prevent a
        // second contribution.
        String filterId = "f1";
        QueryId queryId = new QueryId("test");

        DynamicFilterService dynamicFilterService = new DynamicFilterService();
        JoinDynamicFilter joinFilter = new JoinDynamicFilter(
                filterId, "col1", new Duration(10, SECONDS), DEFAULT_MAX_SIZE_BYTES, new DynamicFilterStats(), new RuntimeStats(), false);
        joinFilter.setExpectedPartitions(1);
        dynamicFilterService.registerFilter(queryId, filterId, joinFilter);

        TupleDomain<String> partialDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(filterId, Domain.singleValue(BIGINT, 42L)));
        DynamicFilterResponse partialResponse = DynamicFilterResponse.incomplete(
                ImmutableMap.of(filterId, partialDomain),
                1L);
        DynamicFilterResponse completionResponse = DynamicFilterResponse.completed(
                ImmutableMap.of(),
                2L,
                ImmutableSet.of(filterId));

        AtomicInteger fetchCount = new AtomicInteger();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            if ("DELETE".equals(request.getMethod())) {
                return new TestingResponse(OK, ImmutableListMultimap.of(), new byte[0]);
            }
            int count = fetchCount.incrementAndGet();
            if (count == 1) {
                return new TestingResponse(OK, contentType(JSON_UTF_8), codec.toJsonBytes(partialResponse));
            }
            return new TestingResponse(OK, contentType(JSON_UTF_8), codec.toJsonBytes(completionResponse));
        });

        fetcher = createFetcher(httpClient, new Duration(30, SECONDS), dynamicFilterService);
        fetcher.start();

        poll(() -> fetchCount.get() >= 2);
        Thread.sleep(500);

        assertTrue(joinFilter.isComplete(),
                "Filter should complete after the second response marks the prior partial as final");
        TupleDomain<String> constraint = joinFilter.getCurrentConstraintByColumnName();
        assertFalse(constraint.isAll(),
                "Final constraint should be the prior partial domain, not all()");
        assertEquals(constraint,
                TupleDomain.withColumnDomains(ImmutableMap.of("col1", Domain.singleValue(BIGINT, 42L))),
                "Final constraint should be the partial domain delivered earlier");
    }

    @Test(timeOut = 30000)
    public void testEmptyBuildPathDeliversNoneAsFinal()
            throws Exception
    {
        // The empty-build path: filter id appears in `completedFilterIds` only
        // and was never previously delivered. The fetcher must deliver
        // TupleDomain.none() so the coordinator records a contribution.
        String filterId = "f1";
        QueryId queryId = new QueryId("test");

        DynamicFilterService dynamicFilterService = new DynamicFilterService();
        JoinDynamicFilter joinFilter = new JoinDynamicFilter(
                filterId, "col1", new Duration(10, SECONDS), DEFAULT_MAX_SIZE_BYTES, new DynamicFilterStats(), new RuntimeStats(), false);
        joinFilter.setExpectedPartitions(1);
        dynamicFilterService.registerFilter(queryId, filterId, joinFilter);

        DynamicFilterResponse response = DynamicFilterResponse.completed(
                ImmutableMap.of(),
                1L,
                ImmutableSet.of(filterId));

        AtomicInteger fetchCount = new AtomicInteger();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            if ("DELETE".equals(request.getMethod())) {
                return new TestingResponse(OK, ImmutableListMultimap.of(), new byte[0]);
            }
            fetchCount.incrementAndGet();
            return new TestingResponse(OK, contentType(JSON_UTF_8), codec.toJsonBytes(response));
        });

        fetcher = createFetcher(httpClient, new Duration(30, SECONDS), dynamicFilterService);
        fetcher.start();

        poll(() -> fetchCount.get() >= 1);
        Thread.sleep(500);

        assertTrue(joinFilter.hasData(),
                "Empty-build path must record a contribution (none())");
        assertTrue(joinFilter.isComplete(),
                "Empty-build delivery is final and must complete the filter");
        TupleDomain<String> constraint = joinFilter.getCurrentConstraintByColumnName();
        assertTrue(constraint.isNone(),
                "Empty-build contribution should produce a none() constraint");
    }

    @Test
    public void testRangeFilterRoundTrip()
            throws Exception
    {
        // Create a range domain: [10, 100000] — mimics the min/max range from
        // VectorHasher overflow for an INTEGER column like c_customer_sk.
        Domain rangeDomain = Domain.create(
                com.facebook.presto.common.predicate.ValueSet.ofRanges(
                        com.facebook.presto.common.predicate.Range.range(
                                BIGINT, 10L, true, 100000L, true)),
                false);

        String filterId = "test_filter";
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(filterId, rangeDomain));

        // Round-trip through JSON serialization (same path as HTTP response).
        DynamicFilterResponse response = DynamicFilterResponse.completed(
                ImmutableMap.of(filterId, tupleDomain),
                1L,
                ImmutableSet.of(filterId));

        byte[] jsonBytes = codec.toJsonBytes(response);
        DynamicFilterResponse parsed = codec.fromJson(jsonBytes);

        // Verify the parsed response has the filter.
        assertTrue(parsed.getFilters().containsKey(filterId),
                "Parsed response should contain filter " + filterId);

        TupleDomain<String> parsedDomain = parsed.getFilters().get(filterId);
        assertFalse(parsedDomain.isAll(), "Parsed domain should not be all()");
        assertFalse(parsedDomain.isNone(), "Parsed domain should not be none()");

        Domain parsedColumnDomain = parsedDomain.getDomains().get().get(filterId);
        com.facebook.presto.common.predicate.SortedRangeSet rangeSet =
                (com.facebook.presto.common.predicate.SortedRangeSet)
                        parsedColumnDomain.getValues();

        // The critical check: Marker.getValue() must be non-null for both
        // the low and high bounds. A null here causes the production NPE.
        com.facebook.presto.common.predicate.Range parsedRange =
                rangeSet.getOrderedRanges().get(0);
        assertFalse(parsedRange.getLow().isLowerUnbounded(),
                "Low bound should not be unbounded");
        assertFalse(parsedRange.getHigh().isUpperUnbounded(),
                "High bound should not be unbounded");
        assertEquals(parsedRange.getLow().getValue(), 10L,
                "Low bound value should be 10");
        assertEquals(parsedRange.getHigh().getValue(), 100000L,
                "High bound value should be 100000");
    }

    /**
     * Same as testRangeFilterRoundTrip but uses INTEGER type (not BIGINT)
     * to match the production c_customer_sk column type. The C++ code's
     * toTypedVariant returns variant(int32_t) for INTEGER, which might
     * serialize differently than variant(int64_t) for BIGINT.
     */
    @Test
    public void testRangeFilterRoundTripInteger()
            throws Exception
    {
        com.facebook.presto.common.type.Type integerType =
                com.facebook.presto.common.type.IntegerType.INTEGER;
        Domain rangeDomain = Domain.create(
                com.facebook.presto.common.predicate.ValueSet.ofRanges(
                        com.facebook.presto.common.predicate.Range.range(
                                integerType, (long) 10, true, (long) 100000, true)),
                false);

        String filterId = "test_filter_int";
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(filterId, rangeDomain));

        DynamicFilterResponse response = DynamicFilterResponse.completed(
                ImmutableMap.of(filterId, tupleDomain),
                1L,
                ImmutableSet.of(filterId));

        byte[] jsonBytes = codec.toJsonBytes(response);
        DynamicFilterResponse parsed = codec.fromJson(jsonBytes);

        TupleDomain<String> parsedDomain = parsed.getFilters().get(filterId);
        Domain parsedColumnDomain = parsedDomain.getDomains().get().get(filterId);
        com.facebook.presto.common.predicate.SortedRangeSet rangeSet =
                (com.facebook.presto.common.predicate.SortedRangeSet)
                        parsedColumnDomain.getValues();

        com.facebook.presto.common.predicate.Range parsedRange =
                rangeSet.getOrderedRanges().get(0);
        assertEquals(parsedRange.getLow().getValue(), (long) 10,
                "INTEGER low bound should round-trip as long 10");
        assertEquals(parsedRange.getHigh().getValue(), (long) 100000,
                "INTEGER high bound should round-trip as long 100000");
    }

    /**
     * Tests deserialization of a DynamicFilterResponse JSON constructed to
     * match what the C++ native worker sends. The valueBlock bytes are
     * manually constructed in Presto's block encoding format (INT_ARRAY
     * for integer, same format as PrestoVectorSerde's serializeSingleColumn).
     * <p>
     * Format for INT_ARRAY with 1 non-null value (22 bytes):
     *   [4 bytes: encoding name length = 9]
     *   [9 bytes: "INT_ARRAY"]
     *   [4 bytes: positionCount = 1]
     *   [1 byte : hasNulls = 0]
     *   [4 bytes: int32 value, little-endian]
     * <p>
     * If this test fails, the C++ PrestoVectorSerde format does NOT match
     * Java's BlockEncodingSerde expectations.
     */
    @Test
    public void testCppSerializedBlockDeserialization()
            throws Exception
    {
        // Construct the raw bytes that C++ PrestoVectorSerde.serializeSingleColumn
        // would produce for a ConstantVector<int32_t>(42) of size 1.
        byte[] blockBytes = new byte[] {
                // Encoding name length = 9 (little-endian int32)
                0x09, 0x00, 0x00, 0x00,
                // Encoding name "INT_ARRAY"
                0x49, 0x4E, 0x54, 0x5F, 0x41, 0x52, 0x52, 0x41, 0x59,
                // positionCount = 1 (little-endian int32)
                0x01, 0x00, 0x00, 0x00,
                // hasNulls = false (single byte)
                0x00,
                // values[0] = 42 (little-endian int32)
                0x2A, 0x00, 0x00, 0x00
        };
        String base64Block = java.util.Base64.getMimeEncoder(-1, new byte[0]).encodeToString(blockBytes);

        // Construct the full DynamicFilterResponse JSON matching what the C++
        // getDynamicFilters endpoint returns.
        String json = String.format(
                "{\"filters\":{\"test_filter\":{\"columnDomains\":[{\"column\":\"test_filter\","
                        + "\"domain\":{\"values\":{\"@type\":\"sortable\",\"type\":\"integer\","
                        + "\"ranges\":[{\"low\":{\"type\":\"integer\",\"valueBlock\":\"%s\","
                        + "\"bound\":\"EXACTLY\"},\"high\":{\"type\":\"integer\","
                        + "\"valueBlock\":\"%s\",\"bound\":\"EXACTLY\"}}]},"
                        + "\"nullAllowed\":false}}]}},"
                        + "\"version\":1,\"operatorCompleted\":true,"
                        + "\"completedFilterIds\":[\"test_filter\"]}",
                base64Block, base64Block);

        // Parse using the production codec — same Jackson config as the
        // DynamicFilterFetcher.
        DynamicFilterResponse parsed = codec.fromJson(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        // Verify the filter was parsed with non-null Marker values.
        TupleDomain<String> parsedDomain = parsed.getFilters().get("test_filter");
        assertFalse(parsedDomain.isNone(), "Parsed domain should not be none()");
        assertFalse(parsedDomain.isAll(), "Parsed domain should not be all()");

        Domain columnDomain = parsedDomain.getDomains().get().get("test_filter");
        com.facebook.presto.common.predicate.SortedRangeSet rangeSet =
                (com.facebook.presto.common.predicate.SortedRangeSet) columnDomain.getValues();

        com.facebook.presto.common.predicate.Range range = rangeSet.getOrderedRanges().get(0);
        // This is the critical assertion — if getValue() returns null,
        // it reproduces the production NPE.
        assertEquals(range.getLow().getValue(), 42L,
                "C++ serialized INT_ARRAY block should deserialize with value 42");
        assertEquals(range.getHigh().getValue(), 42L,
                "C++ serialized INT_ARRAY block should deserialize with value 42");
    }

    private DynamicFilterFetcher createFetcher(TestingHttpClient httpClient, Duration maxErrorDuration)
    {
        return createFetcher(httpClient, maxErrorDuration, new DynamicFilterService());
    }

    private DynamicFilterFetcher createFetcher(TestingHttpClient httpClient, Duration maxErrorDuration, DynamicFilterService dynamicFilterService)
    {
        fetcher = new DynamicFilterFetcher(
                new TaskId("test", 1, 0, 2, 0),
                URI.create("http://fake.invalid/task/node-id/"),
                httpClient,
                eventLoop,
                maxErrorDuration,
                new Duration(100, MILLISECONDS),
                new RemoteTaskStats(),
                codec,
                dynamicFilterService,
                new QueryId("test"),
                new DynamicFilterStats(),
                false,
                throwable -> {});
        return fetcher;
    }

    private static void poll(BooleanSupplier success)
            throws InterruptedException
    {
        long failAt = System.nanoTime() + FAIL_TIMEOUT.roundTo(NANOSECONDS);
        while (!success.getAsBoolean()) {
            long millisUntilFail = (failAt - System.nanoTime()) / 1_000_000;
            if (millisUntilFail <= 0) {
                throw new AssertionError("Timeout of " + FAIL_TIMEOUT + " reached");
            }
            Thread.sleep(min(POLL_TIMEOUT.toMillis(), millisUntilFail));
        }
    }
}
