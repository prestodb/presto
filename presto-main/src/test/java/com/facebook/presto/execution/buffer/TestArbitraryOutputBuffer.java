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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.execution.buffer.BufferTestUtils.MAX_WAIT;
import static com.facebook.presto.execution.buffer.BufferTestUtils.NO_WAIT;
import static com.facebook.presto.execution.buffer.BufferTestUtils.PAGES_SERDE;
import static com.facebook.presto.execution.buffer.BufferTestUtils.acknowledgeBufferResult;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertBufferResultEquals;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertFinished;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertFutureIsDone;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createBufferResult;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createPage;
import static com.facebook.presto.execution.buffer.BufferTestUtils.getFuture;
import static com.facebook.presto.execution.buffer.BufferTestUtils.sizeOfPages;
import static com.facebook.presto.execution.buffer.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.ARBITRARY;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestArbitraryOutputBuffer
{
    private static final String TASK_INSTANCE_ID = "task-instance-id";

    private static final ImmutableList<BigintType> TYPES = ImmutableList.of(BIGINT);
    private static final OutputBufferId FIRST = new OutputBufferId(0);
    private static final OutputBufferId SECOND = new OutputBufferId(1);

    private ScheduledExecutorService stateNotificationExecutor;

    @BeforeClass
    public void setUp()
    {
        stateNotificationExecutor = newScheduledThreadPool(5, daemonThreadsNamed("test-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (stateNotificationExecutor != null) {
            stateNotificationExecutor.shutdownNow();
            stateNotificationExecutor = null;
        }
    }

    @Test
    public void testInvalidConstructorArg()
    {
        try {
            createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, BROADCAST_PARTITION_ID).withNoMoreBufferIds(), new DataSize(0, BYTE));
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException ignored) {
        }
        try {
            createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), new DataSize(0, BYTE));
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testSimple()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY);
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, BROADCAST_PARTITION_ID);

        // add a queue
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, 3, FIRST, 0, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, 0, FIRST, 3, 0);

        // acknowledge first three pages
        buffer.get(FIRST, 3, sizeOfPages(1)).cancel(true);
        // pages now acknowledged
        assertQueueState(buffer, 0, FIRST, 0, 3);

        // fill the buffer, so that it has 10 buffered pages
        for (int i = 3; i < 13; i++) {
            addPage(buffer, createPage(i));
        }
        // there is a pending read from above so one page will be assigned to the first buffer
        assertQueueState(buffer, 9, FIRST, 1, 3);

        // try to add one more page, which should block
        ListenableFuture<?> future = enqueuePage(buffer, createPage(13));
        assertFalse(future.isDone());
        assertQueueState(buffer, 10, FIRST, 1, 3);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 3, sizeOfPages(1), NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertQueueState(buffer, 10, FIRST, 1, 3);

        // we should still be blocked
        assertFalse(future.isDone());

        //
        // add another buffer and verify it sees buffered pages
        outputBuffers = outputBuffers.withBuffer(SECOND, BROADCAST_PARTITION_ID);
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, 10, SECOND, 0, 0);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(10), NO_WAIT), bufferResult(0,
                createPage(4),
                createPage(5),
                createPage(6),
                createPage(7),
                createPage(8),
                createPage(9),
                createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        // page not acknowledged yet so sent count is still zero
        assertQueueState(buffer, 0, SECOND, 10, 0);
        // acknowledge the 10 pages
        buffer.get(SECOND, 10, sizeOfPages(10)).cancel(true);
        assertQueueState(buffer, 0, SECOND, 0, 10);

        //
        // tell shared buffer there will be no more queues
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        buffer.setOutputBuffers(outputBuffers);

        // buffers should see the same stats and the blocked page future from above should be done
        assertQueueState(buffer, 0, FIRST, 1, 3);
        assertQueueState(buffer, 0, SECOND, 0, 10);
        assertFutureIsDone(future);

        // add 3 more pages, buffers always show the same stats
        addPage(buffer, createPage(14));
        addPage(buffer, createPage(15));
        addPage(buffer, createPage(16));
        assertQueueState(buffer, 2, FIRST, 1, 3);
        assertQueueState(buffer, 2, SECOND, 1, 10);

        // pull one page from the second buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 10, sizeOfPages(1), NO_WAIT), bufferResult(10, createPage(14)));
        assertQueueState(buffer, 2, FIRST, 1, 3);
        assertQueueState(buffer, 2, SECOND, 1, 10);

        // acknowledge the page in the first buffer and pull remaining ones
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 4, sizeOfPages(10), NO_WAIT), bufferResult(4, createPage(15), createPage(16)));
        assertQueueState(buffer, 0, FIRST, 2, 4);
        assertQueueState(buffer, 0, SECOND, 1, 10);

        //
        // finish the buffer
        assertFalse(buffer.isFinished());
        buffer.setNoMorePages();
        assertQueueState(buffer, 0, FIRST, 2, 4);
        assertQueueState(buffer, 0, SECOND, 1, 10);

        // not fully finished until all pages are consumed
        assertFalse(buffer.isFinished());

        // acknowledge the pages from the first buffer; buffer should not close automatically
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 6, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 6, true));
        assertQueueState(buffer, 0, FIRST, 0, 6);
        assertQueueState(buffer, 0, SECOND, 1, 10);
        assertFalse(buffer.isFinished());

        // finish first queue
        buffer.abort(FIRST);
        assertQueueClosed(buffer, 0, FIRST, 6);
        assertQueueState(buffer, 0, SECOND, 1, 10);
        assertFalse(buffer.isFinished());

        // acknowledge a page from the second queue; queue should not close automatically
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 11, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 11, true));
        assertQueueState(buffer, 0, SECOND, 0, 11);
        assertFalse(buffer.isFinished());

        // finish second queue
        buffer.abort(SECOND);
        assertQueueClosed(buffer, 0, FIRST, 6);
        assertQueueClosed(buffer, 0, SECOND, 11);
        assertFinished(buffer);

        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 6, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 6, true));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 11, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 11, true));
    }

    // TODO: remove this after PR #7987 is landed
    @Test
    public void testAcknowledge()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY);
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, BROADCAST_PARTITION_ID);

        // add a queue
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, 3, FIRST, 0, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // acknowledge pages 0 and 1
        acknowledgeBufferResult(buffer, FIRST, 2);
        // only page 2 is not removed
        assertQueueState(buffer, 0, FIRST, 1, 2);
        // acknowledge page 2
        acknowledgeBufferResult(buffer, FIRST, 3);
        // nothing left
        assertQueueState(buffer, 0, FIRST, 0, 3);
        // acknowledge more pages will fail
        try {
            acknowledgeBufferResult(buffer, FIRST, 4);
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid sequence id");
        }

        // fill the buffer
        for (int i = 3; i < 6; i++) {
            addPage(buffer, createPage(i));
        }
        assertQueueState(buffer, 3, FIRST, 0, 3);

        // getting new pages will again acknowledge the previously acknowledged pages but this is ok
        buffer.get(FIRST, 3, sizeOfPages(1)).cancel(true);
        assertQueueState(buffer, 2, FIRST, 1, 3);
    }

    @Test
    public void testBufferFull()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(buffer, createPage(1));
        addPage(buffer, createPage(2));

        // third page is blocked
        enqueuePage(buffer, createPage(3));
    }

    @Test
    public void testDuplicateRequests()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        // add a queue
        assertQueueState(buffer, 3, FIRST, 0, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, 0, FIRST, 3, 0);

        // get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, 0, FIRST, 3, 0);

        // acknowledge the pages
        buffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);

        // attempt to get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, false));
        // pages are acknowledged
        assertQueueState(buffer, 0, FIRST, 0, 3);
    }

    @Test
    public void testAddQueueAfterCreation()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        assertFalse(buffer.isFinished());

        try {
            buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY)
                    .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                    .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                    .withNoMoreBufferIds());
            fail("Expected IllegalStateException from addQueue after noMoreQueues has been called");
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testAddAfterFinish()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        buffer.setNoMorePages();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(1));
        assertEquals(buffer.getInfo().getTotalPagesSent(), 0);
    }

    @Test
    public void testAddQueueAfterNoMoreQueues()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // tell buffer no more queues will be added
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        // set no more queues a second time to assure that we don't get an exception or such
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        // set no more queues a third time to assure that we don't get an exception or such
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        try {
            OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY)
                    .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                    .withNoMoreBufferIds();

            buffer.setOutputBuffers(outputBuffers);
            fail("Expected IllegalStateException from addQueue after noMoreQueues has been called");
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testAddAfterDestroy()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        buffer.destroy();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(1));
        assertEquals(buffer.getInfo().getTotalPagesSent(), 0);
    }

    @Test
    public void testGetBeforeCreate()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0L, sizeOfPages(1));
        assertFalse(future.isDone());

        // add a page and verify the future is complete
        addPage(buffer, createPage(33));
        assertTrue(future.isDone());
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(33)));
    }

    @Test
    public void testResumeFromPreviousPosition()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY);
        OutputBufferId[] ids = new OutputBufferId[5];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = new OutputBufferId(i);
            outputBuffers = outputBuffers.withBuffer(ids[i], i);
        }

        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(5));
        assertFalse(buffer.isFinished());

        Map<OutputBufferId, ListenableFuture<BufferResult>> firstReads = new HashMap<>();
        for (OutputBufferId id : ids) {
            firstReads.put(id, buffer.get(id, 0L, sizeOfPages(1)));
        }
        // All must be blocked initially
        assertThat(firstReads.values()).allMatch(future -> !future.isDone());

        List<ListenableFuture<BufferResult>> secondReads = new ArrayList<>();

        for (int i = 0; i < ids.length; i++) {
            // add one page
            addPage(buffer, createPage(33));
            assertThat(secondReads).allMatch(future -> !future.isDone(), "No secondary reads should complete until after all first reads");
            List<OutputBufferId> completedIds = firstReads.entrySet().stream()
                                                        .filter(entry -> entry.getValue().isDone())
                                                        .map(Map.Entry::getKey)
                                                        .collect(toList());
            assertEquals(completedIds.size(), 1, "One completed buffer read per page addition");
            OutputBufferId completed = completedIds.get(0);

            BufferResult result = getFuture(firstReads.remove(completed), NO_WAIT);
            // Store completion order of first for follow up sequence
            secondReads.add(buffer.get(completed, result.getNextToken(), sizeOfPages(1)));
        }
        // Test sanity
        assertEquals(secondReads.size(), ids.length);

        // Completion order should be identical to the first iteration at this point
        for (int i = 0; i < ids.length; i++) {
            // add one page
            addPage(buffer, createPage(33));
            assertTrue(secondReads.get(i).isDone(), "Invalid second read completion order at index: " + i);
        }
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No more buffers already set")
    public void testUseUndeclaredBufferAfterFinalBuffersSet()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that was not declared, which will fail
        buffer.get(SECOND, 0L, sizeOfPages(1));
    }

    @Test
    public void testAbortBeforeCreate()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0L, sizeOfPages(1));
        assertFalse(future.isDone());

        // abort that buffer, and verify the future is finishd
        buffer.abort(FIRST);
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, false));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));

        // add a page and verify the future is not complete
        addPage(buffer, createPage(33));

        // add the buffer and verify we did not get the page
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, 0));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFullBufferBlocksWriter()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(buffer, createPage(1));
        addPage(buffer, createPage(2));

        // third page is blocked
        enqueuePage(buffer, createPage(3));
    }

    @Test
    public void testAbort()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 10; i++) {
            addPage(buffer, createPage(i));
        }
        buffer.setNoMorePages();

        // add one output buffer
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, 0);
        buffer.setOutputBuffers(outputBuffers);

        // read a page from the first buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));

        // abort buffer, and verify page cannot be acknowledged
        buffer.abort(FIRST);
        assertQueueClosed(buffer, 9, FIRST, 0);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));

        outputBuffers = outputBuffers.withBuffer(SECOND, 0).withNoMoreBufferIds();
        buffer.setOutputBuffers(outputBuffers);

        // first page is lost because the first buffer was aborted
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(1)));
        buffer.abort(SECOND);
        assertQueueClosed(buffer, 0, SECOND, 0);
        assertFinished(buffer);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFinishClosesEmptyQueues()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // finish while queues are empty
        buffer.setNoMorePages();

        assertQueueState(buffer, 0, FIRST, 0, 0);
        assertQueueState(buffer, 0, SECOND, 0, 0);

        buffer.abort(FIRST);
        buffer.abort(SECOND);

        assertQueueClosed(buffer, 0, FIRST, 0);
        assertQueueClosed(buffer, 0, SECOND, 0);
    }

    @Test
    public void testAbortFreesReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, 0));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // abort the buffer
        buffer.abort(FIRST);
        assertQueueClosed(buffer, 0, FIRST, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));
    }

    @Test
    public void testFinishFreesReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, 0));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // finish the buffer
        assertQueueState(buffer, 0, FIRST, 0, 1);
        buffer.abort(FIRST);
        assertQueueClosed(buffer, 0, FIRST, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));
    }

    @Test
    public void testFinishFreesWriter()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(5));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY)
                .withBuffer(FIRST, 0)
                .withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // enqueue the addition two pages more pages
        ListenableFuture<?> firstEnqueuePage = enqueuePage(buffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(buffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        buffer.get(FIRST, 1, sizeOfPages(100)).cancel(true);

        // verify we are still blocked because the buffer is full
        assertFalse(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // finish the query
        buffer.setNoMorePages();
        assertFalse(buffer.isFinished());

        // verify futures are complete
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);

        // get and acknowledge the last 5 pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(100), NO_WAIT),
                bufferResult(1, createPage(1), createPage(2), createPage(3), createPage(4), createPage(5), createPage(6)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 7, sizeOfPages(100), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 7, true));

        // verify not finished
        assertFalse(buffer.isFinished());

        // finish the queue
        buffer.abort(FIRST);

        // verify finished
        assertFinished(buffer);
    }

    @Test
    public void testDestroyFreesReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(5));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY)
                .withBuffer(FIRST, 0)
                .withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one page
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // destroy the buffer
        buffer.destroy();
        assertQueueClosed(buffer, 0, FIRST, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));
    }

    @Test
    public void testDestroyFreesWriter()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(5));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY)
                .withBuffer(FIRST, 0)
                .withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // add two pages to the buffer queue
        ListenableFuture<?> firstEnqueuePage = enqueuePage(buffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(buffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        buffer.get(FIRST, 1, sizeOfPages(1)).cancel(true);

        // verify we are still blocked because the buffer is full
        assertFalse(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // destroy the buffer (i.e., cancel the query)
        buffer.destroy();
        assertFinished(buffer);

        // verify the futures are completed
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);
    }

    @Test
    public void testFailDoesNotFreeReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one page
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // fail the buffer
        buffer.fail();

        // future should have not finished
        assertFalse(future.isDone());

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());
    }

    @Test
    public void testFailFreesWriter()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertFalse(buffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // add two pages to the buffer queue
        ListenableFuture<?> firstEnqueuePage = enqueuePage(buffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(buffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        buffer.get(FIRST, 1, sizeOfPages(1)).cancel(true);

        // verify we are still blocked because the buffer is full
        assertFalse(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // fail the buffer (i.e., cancel the query)
        buffer.fail();
        assertFalse(buffer.isFinished());

        // verify the futures are completed
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);
    }

    @Test
    public void testAddBufferAfterFail()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY)
                .withBuffer(FIRST, BROADCAST_PARTITION_ID);
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one page
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // fail the buffer
        buffer.fail();

        // add a buffer
        outputBuffers = outputBuffers.withBuffer(SECOND, BROADCAST_PARTITION_ID);
        buffer.setOutputBuffers(outputBuffers);

        // attempt to get page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());
        future = buffer.get(SECOND, 0, sizeOfPages(10));
        assertFalse(future.isDone());

        // set no more buffers
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        buffer.setOutputBuffers(outputBuffers);

        // attempt to get page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());
        future = buffer.get(SECOND, 0, sizeOfPages(10));
        assertFalse(future.isDone());
    }

    @Test
    public void testBufferCompletion()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(5));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY)
                .withBuffer(FIRST, 0)
                .withNoMoreBufferIds());

        assertFalse(buffer.isFinished());

        // fill the buffer
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Page page = createPage(i);
            addPage(buffer, page);
            pages.add(page);
        }

        buffer.setNoMorePages();

        // get and acknowledge 5 pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(5), MAX_WAIT), createBufferResult(TASK_INSTANCE_ID, 0, pages));

        // buffer is not finished
        assertFalse(buffer.isFinished());

        // there are no more pages and no more buffers, but buffer is not finished because it didn't receive an acknowledgement yet
        assertFalse(buffer.isFinished());

        // ask the buffer to finish
        buffer.abort(FIRST);

        // verify that the buffer is finished
        assertTrue(buffer.isFinished());
    }

    @Test
    public void testNoMorePagesFreesReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, 0));
        assertFalse(buffer.isFinished());

        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));
        assertFalse(future.isDone());

        buffer.setNoMorePages();

        assertTrue(future.isDone());
        assertTrue(buffer.get(FIRST, 0, sizeOfPages(10)).isDone());
    }

    @Test
    public void testFinishBeforeNoMoreBuffers()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }
        buffer.setNoMorePages();
        assertFalse(buffer.isFinished());

        // add one output buffer
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, 0);
        buffer.setOutputBuffers(outputBuffers);
        assertFalse(buffer.isFinished());

        // read a page from the first buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        assertFalse(buffer.isFinished());

        // read remaining pages from the first buffer and acknowledge
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(10), NO_WAIT), bufferResult(1, createPage(1), createPage(2)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 3, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 3, true));
        assertFalse(buffer.isFinished());

        // finish first queue
        buffer.abort(FIRST);
        assertQueueClosed(buffer, 0, FIRST, 3);
        assertFinished(buffer);

        // add another buffer after finish
        outputBuffers = outputBuffers.withBuffer(SECOND, 0);
        buffer.setOutputBuffers(outputBuffers);

        // verify second buffer has no results
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testForceFreeMemory()
            throws Throwable
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }
        OutputBufferMemoryManager memoryManager = buffer.getMemoryManager();
        assertTrue(memoryManager.getBufferedBytes() > 0);
        buffer.forceFreeMemory();
        assertEquals(memoryManager.getBufferedBytes(), 0);
        // adding a page after forceFreeMemory() should be NOOP
        addPage(buffer, createPage(1));
        assertEquals(memoryManager.getBufferedBytes(), 0);
    }

    private static BufferResult getBufferResult(OutputBuffer buffer, OutputBufferId bufferId, long sequenceId, DataSize maxSize, Duration maxWait)
    {
        ListenableFuture<BufferResult> future = buffer.get(bufferId, sequenceId, maxSize);
        return getFuture(future, maxWait);
    }

    private static ListenableFuture<?> enqueuePage(OutputBuffer buffer, Page page)
    {
        buffer.enqueue(Lifespan.taskWide(), ImmutableList.of(PAGES_SERDE.serialize(page)));
        ListenableFuture<?> future = buffer.isFull();
        assertFalse(future.isDone());
        return future;
    }

    private static void addPage(OutputBuffer buffer, Page page)
    {
        buffer.enqueue(Lifespan.taskWide(), ImmutableList.of(PAGES_SERDE.serialize(page)));
        assertTrue(buffer.isFull().isDone(), "Expected add page to not block");
    }

    private static void assertQueueState(
            OutputBuffer buffer,
            int unassignedPages,
            OutputBufferId bufferId,
            int bufferedPages,
            int pagesSent)
    {
        OutputBufferInfo outputBufferInfo = buffer.getInfo();

        long assignedPages = outputBufferInfo.getBuffers().stream().mapToInt(BufferInfo::getBufferedPages).sum();

        assertEquals(
                outputBufferInfo.getTotalBufferedPages() - assignedPages,
                unassignedPages,
                "unassignedPages");

        BufferInfo bufferInfo = outputBufferInfo.getBuffers().stream()
                .filter(info -> info.getBufferId().equals(bufferId))
                .findAny()
                .orElse(null);

        assertEquals(
                bufferInfo,
                new BufferInfo(
                        bufferId,
                        false,
                        bufferedPages,
                        pagesSent,
                        new PageBufferInfo(
                                bufferId.getId(),
                                bufferedPages,
                                sizeOfPages(bufferedPages).toBytes(),
                                bufferedPages + pagesSent, // every page has one row
                                bufferedPages + pagesSent)));
    }

    @SuppressWarnings("ConstantConditions")
    private static void assertQueueClosed(OutputBuffer buffer, int unassignedPages, OutputBufferId bufferId, int pagesSent)
    {
        OutputBufferInfo outputBufferInfo = buffer.getInfo();

        long assignedPages = outputBufferInfo.getBuffers().stream().mapToInt(BufferInfo::getBufferedPages).sum();
        assertEquals(
                outputBufferInfo.getTotalBufferedPages() - assignedPages,
                unassignedPages,
                "unassignedPages");

        BufferInfo bufferInfo = outputBufferInfo.getBuffers().stream()
                .filter(info -> info.getBufferId().equals(bufferId))
                .findAny()
                .orElse(null);

        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getPagesSent(), pagesSent);
        assertTrue(bufferInfo.isFinished());
    }

    private ArbitraryOutputBuffer createArbitraryBuffer(OutputBuffers buffers, DataSize dataSize)
    {
        ArbitraryOutputBuffer buffer = new ArbitraryOutputBuffer(
                TASK_INSTANCE_ID,
                new StateMachine<>("bufferState", stateNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES),
                dataSize,
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                stateNotificationExecutor);
        buffer.setOutputBuffers(buffers);
        buffer.registerLifespanCompletionCallback(ignore -> {});
        return buffer;
    }

    private static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return createBufferResult(TASK_INSTANCE_ID, token, pages);
    }
}
