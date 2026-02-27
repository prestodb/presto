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
package com.facebook.presto.server.protocol;

import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.SpoolingOutputBuffer;
import com.facebook.presto.execution.buffer.SpoolingOutputBufferFactory;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.HttpShuffleClientProvider;
import com.facebook.presto.operator.MockExchangeRequestProcessor;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.execution.buffer.OutputBuffers.createSpoolingOutputBuffers;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCoordinatorResultBuffer
{
    private ScheduledExecutorService scheduler;
    private ExecutorService pageBufferClientCallbackExecutor;
    private ExecutorService testingHttpClientExecutor;
    private ScheduledExecutorService stateNotificationExecutor;
    private SpoolingOutputBufferFactory spoolingOutputBufferFactory;

    @BeforeClass
    public void setUp()
    {
        scheduler = newScheduledThreadPool(4, daemonThreadsNamed("test-scheduler-%s"));
        pageBufferClientCallbackExecutor = Executors.newSingleThreadExecutor();
        testingHttpClientExecutor = newCachedThreadPool(daemonThreadsNamed("test-http-%s"));
        stateNotificationExecutor = newScheduledThreadPool(5, daemonThreadsNamed("test-state-%s"));

        FeaturesConfig featuresConfig = new FeaturesConfig();
        spoolingOutputBufferFactory = new SpoolingOutputBufferFactory(featuresConfig);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        if (pageBufferClientCallbackExecutor != null) {
            pageBufferClientCallbackExecutor.shutdownNow();
            pageBufferClientCallbackExecutor = null;
        }
        if (testingHttpClientExecutor != null) {
            testingHttpClientExecutor.shutdownNow();
            testingHttpClientExecutor = null;
        }
        if (stateNotificationExecutor != null) {
            stateNotificationExecutor.shutdownNow();
            stateNotificationExecutor = null;
        }
        if (spoolingOutputBufferFactory != null) {
            spoolingOutputBufferFactory.shutdown();
            spoolingOutputBufferFactory = null;
        }
    }

    @Test(timeOut = 30000)
    public void testPollPageReturnsNullBeforeRelease()
    {
        DataSize maxResponseSize = new DataSize(10, MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.setComplete(location);

        ExchangeClient exchangeClient = createExchangeClient(processor, maxResponseSize);
        exchangeClient.addLocation(location, TaskId.valueOf("query.0.0.0.0"));
        exchangeClient.noMoreLocations();
        waitForPages(exchangeClient);

        CoordinatorResultBuffer buffer = createCoordinatorResultBuffer(exchangeClient);

        assertNull(buffer.pollPage());
        assertNull(buffer.pollPage());
        assertFalse(buffer.isFinished());

        buffer.discardForRetry();
    }

    @Test(timeOut = 30000)
    public void testPollPageReturnsPagesAfterRelease()
    {
        DataSize maxResponseSize = new DataSize(10, MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        ExchangeClient exchangeClient = createExchangeClient(processor, maxResponseSize);
        exchangeClient.addLocation(location, TaskId.valueOf("query.0.0.0.0"));
        exchangeClient.noMoreLocations();
        waitForPages(exchangeClient);

        CoordinatorResultBuffer buffer = createCoordinatorResultBuffer(exchangeClient);

        buffer.drainExchangeClient();
        waitForClose(exchangeClient);
        buffer.drainExchangeClient();

        assertNull(buffer.pollPage());

        buffer.release();

        SerializedPage page1 = pollUntilPage(buffer, 10);
        assertNotNull(page1);
        assertEquals(page1.getPositionCount(), 1);

        SerializedPage page2 = pollUntilPage(buffer, 10);
        assertNotNull(page2);
        assertEquals(page2.getPositionCount(), 2);

        SerializedPage page3 = pollUntilPage(buffer, 10);
        assertNotNull(page3);
        assertEquals(page3.getPositionCount(), 3);

        assertNull(buffer.pollPage());
        assertTrue(buffer.isFinished());
    }

    @Test(timeOut = 30000)
    public void testDrainMovesPagesThroughStorageBuffer()
    {
        DataSize maxResponseSize = new DataSize(10, MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(5));
        processor.addPage(location, createPage(10));
        processor.setComplete(location);

        ExchangeClient exchangeClient = createExchangeClient(processor, maxResponseSize);
        exchangeClient.addLocation(location, TaskId.valueOf("query.0.0.0.0"));
        exchangeClient.noMoreLocations();
        waitForPages(exchangeClient);

        CoordinatorResultBuffer buffer = createCoordinatorResultBuffer(exchangeClient);

        buffer.drainExchangeClient();
        waitForClose(exchangeClient);
        buffer.drainExchangeClient();

        buffer.release();

        SerializedPage page1 = pollUntilPage(buffer, 10);
        assertNotNull(page1);
        assertEquals(page1.getPositionCount(), 5);

        SerializedPage page2 = pollUntilPage(buffer, 10);
        assertNotNull(page2);
        assertEquals(page2.getPositionCount(), 10);

        assertNull(buffer.pollPage());
        assertTrue(buffer.isFinished());
    }

    @Test(timeOut = 30000)
    public void testDiscardForRetryClearsEverything()
    {
        DataSize maxResponseSize = new DataSize(10, MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.setComplete(location);

        ExchangeClient exchangeClient = createExchangeClient(processor, maxResponseSize);
        exchangeClient.addLocation(location, TaskId.valueOf("query.0.0.0.0"));
        exchangeClient.noMoreLocations();

        waitForPages(exchangeClient);

        CoordinatorResultBuffer buffer = createCoordinatorResultBuffer(exchangeClient);

        buffer.drainExchangeClient();
        waitForClose(exchangeClient);
        buffer.drainExchangeClient();

        buffer.discardForRetry();

        assertNull(buffer.pollPage());
        assertFalse(buffer.isFinished());
    }

    @Test(timeOut = 30000)
    public void testIsFinishedCorrectness()
    {
        DataSize maxResponseSize = new DataSize(10, MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.setComplete(location);

        ExchangeClient exchangeClient = createExchangeClient(processor, maxResponseSize);
        exchangeClient.addLocation(location, TaskId.valueOf("query.0.0.0.0"));
        exchangeClient.noMoreLocations();
        waitForPages(exchangeClient);

        CoordinatorResultBuffer buffer = createCoordinatorResultBuffer(exchangeClient);

        assertFalse(buffer.isFinished());

        buffer.drainExchangeClient();
        waitForClose(exchangeClient);
        buffer.drainExchangeClient();
        buffer.release();

        assertFalse(buffer.isFinished());

        assertNotNull(pollUntilPage(buffer, 10));

        pollUntilPage(buffer, 1);
        assertTrue(buffer.isFinished());
    }

    @Test(timeOut = 30000)
    public void testReleaseEagerlyDrainsExchangeClient()
    {
        DataSize maxResponseSize = new DataSize(10, MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.setComplete(location);

        ExchangeClient exchangeClient = createExchangeClient(processor, maxResponseSize);
        exchangeClient.addLocation(location, TaskId.valueOf("query.0.0.0.0"));
        exchangeClient.noMoreLocations();

        waitForPages(exchangeClient);

        CoordinatorResultBuffer buffer = createCoordinatorResultBuffer(exchangeClient);

        waitForClose(exchangeClient);
        buffer.release();

        SerializedPage page1 = pollUntilPage(buffer, 10);
        assertNotNull(page1);
        SerializedPage page2 = pollUntilPage(buffer, 10);
        assertNotNull(page2);
        pollUntilPage(buffer, 1);
        assertTrue(buffer.isFinished());
    }

    @Test(timeOut = 30000)
    public void testEmptyExchangeClient()
    {
        DataSize maxResponseSize = new DataSize(10, MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.setComplete(location);

        ExchangeClient exchangeClient = createExchangeClient(processor, maxResponseSize);
        exchangeClient.addLocation(location, TaskId.valueOf("query.0.0.0.0"));
        exchangeClient.noMoreLocations();

        waitForClose(exchangeClient);

        CoordinatorResultBuffer buffer = createCoordinatorResultBuffer(exchangeClient);

        buffer.release();

        // Poll until the storage buffer's empty/complete signal is processed
        pollUntilPage(buffer, 5);
        assertTrue(buffer.isFinished());
    }

    @Test(timeOut = 30000)
    public void testDestroyCleanupOnFailure()
    {
        DataSize maxResponseSize = new DataSize(10, MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        ExchangeClient exchangeClient = createExchangeClient(processor, maxResponseSize);
        exchangeClient.addLocation(location, TaskId.valueOf("query.0.0.0.0"));
        exchangeClient.noMoreLocations();

        waitForPages(exchangeClient);

        CoordinatorResultBuffer buffer = createCoordinatorResultBuffer(exchangeClient);

        buffer.drainExchangeClient();
        waitForClose(exchangeClient);
        buffer.drainExchangeClient();

        buffer.discardForRetry();

        assertNull(buffer.pollPage());
        assertFalse(buffer.isFinished());
    }

    @Test(timeOut = 30000)
    public void testDiscardForRetryWithPendingStorageRead()
    {
        DataSize maxResponseSize = new DataSize(10, MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.setComplete(location);

        ExchangeClient exchangeClient = createExchangeClient(processor, maxResponseSize);
        exchangeClient.addLocation(location, TaskId.valueOf("query.0.0.0.0"));
        exchangeClient.noMoreLocations();

        waitForPages(exchangeClient);

        CoordinatorResultBuffer buffer = createCoordinatorResultBuffer(exchangeClient);

        buffer.drainExchangeClient();
        waitForClose(exchangeClient);
        buffer.drainExchangeClient();
        buffer.release();

        // Initiate a pending storage read
        buffer.pollPage();

        // Discard while storage read may be pending
        buffer.discardForRetry();

        assertNull(buffer.pollPage());
        assertFalse(buffer.isFinished());
        assertFalse(buffer.hasRemainingData());
    }

    @Test(timeOut = 30000)
    public void testHasRemainingDataAfterDiscard()
    {
        DataSize maxResponseSize = new DataSize(10, MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.setComplete(location);

        ExchangeClient exchangeClient = createExchangeClient(processor, maxResponseSize);
        exchangeClient.addLocation(location, TaskId.valueOf("query.0.0.0.0"));
        exchangeClient.noMoreLocations();

        waitForPages(exchangeClient);

        CoordinatorResultBuffer buffer = createCoordinatorResultBuffer(exchangeClient);

        assertTrue(buffer.hasRemainingData());

        buffer.drainExchangeClient();
        waitForClose(exchangeClient);
        buffer.drainExchangeClient();

        assertTrue(buffer.hasRemainingData());

        buffer.discardForRetry();
        assertFalse(buffer.hasRemainingData());
    }

    private CoordinatorResultBuffer createCoordinatorResultBuffer(ExchangeClient exchangeClient)
    {
        TaskId taskId = new TaskId("coordinator_buffer_test", 0, 0, 0, 0);
        String instanceId = "test-instance";
        StateMachine<BufferState> bufferState = new StateMachine<>(
                "coordinator-buffer",
                stateNotificationExecutor,
                OPEN,
                TERMINAL_BUFFER_STATES);
        SpoolingOutputBuffer storageBuffer = spoolingOutputBufferFactory.createSpoolingOutputBuffer(
                taskId, instanceId, createSpoolingOutputBuffers(), bufferState);
        return new CoordinatorResultBuffer(exchangeClient, storageBuffer, new RuntimeStats());
    }

    private ExchangeClient createExchangeClient(MockExchangeRequestProcessor processor, DataSize maxResponseSize)
    {
        DataSize bufferCapacity = new DataSize(32, MEGABYTE);
        return new ExchangeClient(
                bufferCapacity,
                maxResponseSize,
                1,
                new Duration(1, MINUTES),
                true,
                0.2,
                new HttpShuffleClientProvider(new TestingHttpClient(processor, testingHttpClientExecutor)),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);
    }

    private static void waitForPages(ExchangeClient exchangeClient)
    {
        tryGetFutureValue(
                transform(exchangeClient.isBlocked(), ignored -> null, directExecutor()),
                10, SECONDS);
    }

    private static void waitForClose(ExchangeClient exchangeClient)
    {
        int attempts = 0;
        while (!exchangeClient.isClosed() && attempts < 100) {
            tryGetFutureValue(
                    transform(exchangeClient.isBlocked(), ignored -> null, directExecutor()),
                    1, SECONDS);
            attempts++;
        }
    }

    private static SerializedPage pollUntilPage(CoordinatorResultBuffer buffer, long timeoutSeconds)
    {
        long deadline = System.nanoTime() + SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            SerializedPage page = buffer.pollPage();
            if (page != null) {
                return page;
            }
            Thread.yield();
        }
        return null;
    }

    private static Page createPage(int size)
    {
        return new Page(BlockAssertions.createLongSequenceBlock(0, size));
    }
}
