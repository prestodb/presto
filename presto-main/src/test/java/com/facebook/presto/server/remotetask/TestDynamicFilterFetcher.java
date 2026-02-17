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
import java.util.Optional;
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

    // Reproduces a bug where a task has two DynamicFilterSourceOperators in
    // different pipelines (two JoinNodes in the same fragment).  Pipeline A
    // flushes filter f1 first and sets operatorCompleted=true on the task.
    // The fetcher sees f1 + operatorCompleted, delivers f1, and stops —
    // never polling again for f2 which Pipeline B flushes later.
    @Test(timeOut = 30000)
    public void testSecondFilterLostWhenFirstFlushSetsOperatorCompleted()
            throws Exception
    {
        String filterIdA = "f1";
        String filterIdB = "f2";
        QueryId queryId = new QueryId("test");

        DynamicFilterService dynamicFilterService = new DynamicFilterService();
        JoinDynamicFilter filterA = new JoinDynamicFilter(
                filterIdA, "col1", new Duration(10, SECONDS), new DynamicFilterStats(), Optional.empty());
        filterA.setExpectedPartitions(1);
        dynamicFilterService.registerFilter(queryId, filterIdA, filterA);

        JoinDynamicFilter filterB = new JoinDynamicFilter(
                filterIdB, "col2", new Duration(10, SECONDS), new DynamicFilterStats(), Optional.empty());
        filterB.setExpectedPartitions(1);
        dynamicFilterService.registerFilter(queryId, filterIdB, filterB);

        // Response 1: Pipeline A flushed f1, but task says operatorCompleted=false
        // because only 1 of 2 expected factories have flushed
        TupleDomain<String> domainA = TupleDomain.withColumnDomains(
                ImmutableMap.of(filterIdA, Domain.singleValue(BIGINT, 42L)));
        DynamicFilterResponse firstResponse = DynamicFilterResponse.incomplete(
                ImmutableMap.of(filterIdA, domainA),
                1L);

        // Response 2: Pipeline B flushed f2, now all factories done → operatorCompleted=true
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

        // Wait long enough for the fetcher to have polled at least twice if it were going to
        poll(() -> fetchCount.get() >= 1);
        Thread.sleep(1000);

        // BUG: fetcher stops after first poll because it saw operatorCompleted + non-empty filters.
        // Filter f1 was delivered but f2 was never collected.
        assertTrue(filterA.hasData(), "Filter A should have received data");

        // This assertion exposes the bug: filterB never gets data because the fetcher stopped
        assertTrue(filterB.hasData(),
                "Filter B should have received data, but fetcher stopped after first pipeline's flush");
    }

    // Verifies fetcher stops when filters arrive with operator completion,
    // avoiding a spurious none() partition from an extra poll
    @Test(timeOut = 30000)
    public void testStopsPollingWhenFiltersArriveWithOperatorCompletion()
            throws Exception
    {
        String filterId = "f1";
        QueryId queryId = new QueryId("test");

        DynamicFilterService dynamicFilterService = new DynamicFilterService();
        JoinDynamicFilter joinFilter = new JoinDynamicFilter(
                filterId, "col1", new Duration(10, SECONDS), new DynamicFilterStats(), Optional.empty());
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
                false);
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
