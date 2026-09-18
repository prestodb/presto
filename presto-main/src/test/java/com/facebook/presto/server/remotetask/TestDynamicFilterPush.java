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

import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.http.client.testing.TestingResponse;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SchedulerStatsTracker;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.DomainRuntimeFilter;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMultimap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static com.facebook.airlift.http.client.HttpStatus.NOT_FOUND;
import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.HttpStatus.SERVICE_UNAVAILABLE;
import static com.facebook.presto.client.NodeVersion.UNKNOWN;
import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.BROADCAST;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests the {@code pushDynamicFilter} / {@code sendDynamicFilterPush} retry and
 * backoff logic in {@link HttpRemoteTaskWithEventLoop}:
 * <ul>
 *   <li>HTTP 200: single attempt, no retry</li>
 *   <li>HTTP 404: no retry (endpoint absent on Java workers)</li>
 *   <li>HTTP 503 always: retries up to MAX_DYNAMIC_FILTER_PUSH_ATTEMPTS (3), then gives up</li>
 *   <li>HTTP 503 once then 200: recovers after one retry</li>
 *   <li>Network failure: single attempt, no retry</li>
 *   <li>{@code Retry-After: 0}: retries fire quickly (observable timing)</li>
 *   <li>Non-numeric {@code Retry-After}: NumberFormatException caught, retry still happens</li>
 * </ul>
 *
 * Uses the {@link TestHttpRemoteTaskWithEventLoop} factory infrastructure to avoid
 * duplicating the Guice wiring.
 */
@Test(singleThreaded = true)
public class TestDynamicFilterPush
{
    private static final long FAIL_TIMEOUT_MS = 15_000;
    private static final long POLL_SLEEP_MS = 50;

    private HttpRemoteTaskFactory httpRemoteTaskFactory;
    private RemoteTask remoteTask;

    @AfterMethod(alwaysRun = true)
    public void cleanup()
    {
        if (remoteTask != null) {
            remoteTask.abort();
            remoteTask = null;
        }
        if (httpRemoteTaskFactory != null) {
            httpRemoteTaskFactory.stop();
            httpRemoteTaskFactory = null;
        }
    }

    // -------------------------------------------------------------------------
    // 200 OK — single attempt, no retry
    // -------------------------------------------------------------------------

    @Test(timeOut = 30000)
    public void testPushSuccessOnFirstAttempt()
            throws Exception
    {
        AtomicInteger pushCount = new AtomicInteger();

        remoteTask = createTaskWithHttpClient(request -> {
            if (isPushRequest(request)) {
                pushCount.incrementAndGet();
                return new TestingResponse(OK, ImmutableListMultimap.of(), new byte[0]);
            }
            return new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]);
        });

        remoteTask.pushDynamicFilter(new PlanNodeId("scan"), "f1", new DomainRuntimeFilter(TupleDomain.none()));

        poll(() -> pushCount.get() >= 1);
        Thread.sleep(200);
        assertEquals(pushCount.get(), 1, "HTTP 200 should not trigger any retry");
    }

    // -------------------------------------------------------------------------
    // 404 — no retry (fire-and-forget, endpoint absent on Java workers)
    // -------------------------------------------------------------------------

    @Test(timeOut = 30000)
    public void testPushNotFoundNoRetry()
            throws Exception
    {
        AtomicInteger pushCount = new AtomicInteger();

        remoteTask = createTaskWithHttpClient(request -> {
            if (isPushRequest(request)) {
                pushCount.incrementAndGet();
                return new TestingResponse(NOT_FOUND, ImmutableListMultimap.of(), new byte[0]);
            }
            return new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]);
        });

        remoteTask.pushDynamicFilter(new PlanNodeId("scan"), "f1", new DomainRuntimeFilter(TupleDomain.none()));

        poll(() -> pushCount.get() >= 1);
        Thread.sleep(200);
        assertEquals(pushCount.get(), 1, "HTTP 404 should not trigger any retry");
    }

    // -------------------------------------------------------------------------
    // 503 always — give up after MAX_DYNAMIC_FILTER_PUSH_ATTEMPTS (3)
    // -------------------------------------------------------------------------

    @Test(timeOut = 30000)
    public void testPushThrottledGivesUpAfterMaxAttempts()
            throws Exception
    {
        AtomicInteger pushCount = new AtomicInteger();

        // Always 503 with Retry-After: 0 so retries happen immediately
        remoteTask = createTaskWithHttpClient(request -> {
            if (isPushRequest(request)) {
                pushCount.incrementAndGet();
                return new TestingResponse(
                        SERVICE_UNAVAILABLE,
                        ImmutableListMultimap.of("Retry-After", "0"),
                        new byte[0]);
            }
            return new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]);
        });

        remoteTask.pushDynamicFilter(new PlanNodeId("scan"), "f1", new DomainRuntimeFilter(TupleDomain.none()));

        // Wait for all 3 attempts to complete
        poll(() -> pushCount.get() >= 3);
        Thread.sleep(200);
        assertEquals(pushCount.get(), 3,
                "Should make exactly MAX_DYNAMIC_FILTER_PUSH_ATTEMPTS=3 attempts before giving up");
    }

    // -------------------------------------------------------------------------
    // 503 once, then 200 — recovers on second attempt
    // -------------------------------------------------------------------------

    @Test(timeOut = 30000)
    public void testPushThrottledOnceThenSucceeds()
            throws Exception
    {
        AtomicInteger pushCount = new AtomicInteger();

        remoteTask = createTaskWithHttpClient(request -> {
            if (isPushRequest(request)) {
                int count = pushCount.incrementAndGet();
                if (count == 1) {
                    // First attempt: 503 with Retry-After: 0 for immediate retry
                    return new TestingResponse(
                            SERVICE_UNAVAILABLE,
                            ImmutableListMultimap.of("Retry-After", "0"),
                            new byte[0]);
                }
                return new TestingResponse(OK, ImmutableListMultimap.of(), new byte[0]);
            }
            return new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]);
        });

        remoteTask.pushDynamicFilter(new PlanNodeId("scan"), "f1", new DomainRuntimeFilter(TupleDomain.none()));

        poll(() -> pushCount.get() >= 2);
        Thread.sleep(200);
        assertEquals(pushCount.get(), 2, "Should stop after 503 + 200 (no further retries after success)");
    }

    // -------------------------------------------------------------------------
    // Network failure (exception from HttpClient) — single attempt, no retry
    // -------------------------------------------------------------------------

    @Test(timeOut = 30000)
    public void testPushNetworkFailureNoRetry()
            throws Exception
    {
        AtomicInteger pushCount = new AtomicInteger();

        remoteTask = createTaskWithHttpClient(request -> {
            if (isPushRequest(request)) {
                pushCount.incrementAndGet();
                throw new RuntimeException("Simulated network failure");
            }
            return new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]);
        });

        remoteTask.pushDynamicFilter(new PlanNodeId("scan"), "f1", new DomainRuntimeFilter(TupleDomain.none()));

        poll(() -> pushCount.get() >= 1);
        Thread.sleep(200);
        assertEquals(pushCount.get(), 1, "Network failure is fire-and-forget: should not retry");
    }

    // -------------------------------------------------------------------------
    // parseRetryAfterMs: Retry-After: 0 → near-immediate retries
    // -------------------------------------------------------------------------

    @Test(timeOut = 30000)
    public void testRetryAfterZeroFiresImmediately()
            throws Exception
    {
        AtomicInteger pushCount = new AtomicInteger();
        long startNs = System.nanoTime();

        // 503 twice with Retry-After: 0, then 200
        remoteTask = createTaskWithHttpClient(request -> {
            if (isPushRequest(request)) {
                int count = pushCount.incrementAndGet();
                if (count < 3) {
                    return new TestingResponse(
                            SERVICE_UNAVAILABLE,
                            ImmutableListMultimap.of("Retry-After", "0"),
                            new byte[0]);
                }
                return new TestingResponse(OK, ImmutableListMultimap.of(), new byte[0]);
            }
            return new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]);
        });

        remoteTask.pushDynamicFilter(new PlanNodeId("scan"), "f1", new DomainRuntimeFilter(TupleDomain.none()));

        poll(() -> pushCount.get() >= 3);
        long elapsedMs = NANOSECONDS.toMillis(System.nanoTime() - startNs);

        // With Retry-After: 0 all 3 attempts should complete well within 2 s
        assertTrue(elapsedMs < 2000,
                format("Retry-After: 0 should yield near-immediate retries, but took %d ms", elapsedMs));
    }

    // -------------------------------------------------------------------------
    // parseRetryAfterMs: non-numeric header → NumberFormatException caught, retry fires
    // -------------------------------------------------------------------------

    @Test(timeOut = 30000)
    public void testRetryAfterNonNumericDoesNotCrashRetryLoop()
            throws Exception
    {
        AtomicInteger pushCount = new AtomicInteger();

        // First 503 with non-numeric Retry-After; second attempt succeeds.
        // If parseRetryAfterMs threw an uncaught exception, the retry would never fire.
        remoteTask = createTaskWithHttpClient(request -> {
            if (isPushRequest(request)) {
                int count = pushCount.incrementAndGet();
                if (count == 1) {
                    return new TestingResponse(
                            SERVICE_UNAVAILABLE,
                            ImmutableListMultimap.of("Retry-After", "not-a-number"),
                            new byte[0]);
                }
                return new TestingResponse(OK, ImmutableListMultimap.of(), new byte[0]);
            }
            return new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]);
        });

        remoteTask.pushDynamicFilter(new PlanNodeId("scan"), "f1", new DomainRuntimeFilter(TupleDomain.none()));

        // If retry fired → 2 requests total; if parseRetryAfterMs crashed → stuck at 1
        poll(() -> pushCount.get() >= 2);
        assertEquals(pushCount.get(), 2,
                "Non-numeric Retry-After header must not crash the retry path; retry must still fire");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Returns true if the request is a dynamic-filter push POST
     * (path contains "dynamicFilter", which is the push endpoint; the plural
     * "dynamicFilters" path is the coordinator-side fetch endpoint).
     */
    private static boolean isPushRequest(com.facebook.airlift.http.client.Request request)
    {
        String path = request.getUri().getPath();
        return "POST".equals(request.getMethod()) && path.contains("/dynamicFilter/");
    }

    private RemoteTask createTaskWithHttpClient(TestingHttpClient.Processor processor)
            throws Exception
    {
        httpRemoteTaskFactory = TestHttpRemoteTaskWithEventLoop.createHttpRemoteTaskFactory(processor);
        return httpRemoteTaskFactory.createRemoteTask(
                com.facebook.presto.testing.TestingSession.testSessionBuilder().build(),
                new TaskId("test_query", 1, 0, 1, 0),
                new InternalNode("node-1", URI.create("http://fake.invalid/"), UNKNOWN, false),
                createPlanFragment(),
                ImmutableMultimap.of(),
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(new OutputBuffers.OutputBufferId(0), 0)
                        .withNoMoreBufferIds(),
                new NodeTaskMap.NodeStatsTracker(i -> {}, i -> {}, (age, i) -> {}),
                true,
                new TableWriteInfo(Optional.empty(), Optional.empty()),
                SchedulerStatsTracker.NOOP);
    }

    private static void poll(BooleanSupplier condition)
            throws InterruptedException
    {
        long deadline = System.nanoTime() + NANOSECONDS.convert(FAIL_TIMEOUT_MS, MILLISECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(format("Timed out after %d ms waiting for condition", FAIL_TIMEOUT_MS));
            }
            Thread.sleep(POLL_SLEEP_MS);
        }
    }
}
