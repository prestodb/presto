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
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.memory.context.MemoryReservationHandler;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.spi.page.PagesSerde;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.execution.buffer.BufferTestUtils.MAX_WAIT;
import static com.facebook.presto.execution.buffer.BufferTestUtils.NO_WAIT;
import static com.facebook.presto.execution.buffer.BufferTestUtils.acknowledgeBufferResult;
import static com.facebook.presto.execution.buffer.BufferTestUtils.addPage;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertBufferResultEquals;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertFinished;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertFutureIsDone;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertQueueClosed;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertQueueState;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createBufferResult;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createPage;
import static com.facebook.presto.execution.buffer.BufferTestUtils.enqueuePage;
import static com.facebook.presto.execution.buffer.BufferTestUtils.getBufferResult;
import static com.facebook.presto.execution.buffer.BufferTestUtils.getFuture;
import static com.facebook.presto.execution.buffer.BufferTestUtils.sizeOfPages;
import static com.facebook.presto.execution.buffer.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.BROADCAST;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.buffer.TestingPagesSerdeFactory.testingPagesSerde;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestBroadcastOutputBuffer
{
    private static final PagesSerde PAGES_SERDE = testingPagesSerde();
    private static final String TASK_INSTANCE_ID = "task-instance-id";

    private static final ImmutableList<BigintType> TYPES = ImmutableList.of(BIGINT);
    private static final OutputBufferId FIRST = new OutputBufferId(0);
    private static final OutputBufferId SECOND = new OutputBufferId(1);
    private static final OutputBufferId THIRD = new OutputBufferId(2);

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
            createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST).withBuffer(FIRST, BROADCAST_PARTITION_ID).withNoMoreBufferIds(), new DataSize(0, BYTE));
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException ignored) {
        }
        try {
            createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), new DataSize(0, BYTE));
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testSimple()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(BROADCAST);
        BroadcastOutputBuffer buffer = createBroadcastBuffer(outputBuffers, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        outputBuffers = createInitialEmptyOutputBuffers(BROADCAST).withBuffer(FIRST, BROADCAST_PARTITION_ID);

        // add a queue
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, FIRST, 3, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 3, 0);

        // acknowledge first three pages
        buffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);
        // pages now acknowledged
        assertQueueState(buffer, FIRST, 0, 3);

        // fill the buffer (we already added 3 pages)
        for (int i = 3; i < 10; i++) {
            addPage(buffer, createPage(i));
        }
        assertQueueState(buffer, FIRST, 7, 3);

        // try to add one more page, which should block
        ListenableFuture<?> future = enqueuePage(buffer, createPage(10));
        assertFalse(future.isDone());
        assertQueueState(buffer, FIRST, 8, 3);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 3, sizeOfPages(1), NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertQueueState(buffer, FIRST, 8, 3);

        // we should still be blocked
        assertFalse(future.isDone());

        //
        // add another buffer and verify it sees all pages
        outputBuffers = outputBuffers.withBuffer(SECOND, BROADCAST_PARTITION_ID);
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, SECOND, 11, 0);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0),
                createPage(1),
                createPage(2),
                createPage(3),
                createPage(4),
                createPage(5),
                createPage(6),
                createPage(7),
                createPage(8),
                createPage(9)));
        // page not acknowledged yet so sent count is still zero
        assertQueueState(buffer, SECOND, 11, 0);
        // acknowledge the 10 pages
        buffer.get(SECOND, 10, sizeOfPages(10)).cancel(true);
        assertQueueState(buffer, SECOND, 1, 10);

        //
        // tell shared buffer there will be no more queues
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        buffer.setOutputBuffers(outputBuffers);

        // queues consumed the first three pages, so they should be dropped now and the blocked page future from above should be done
        assertQueueState(buffer, FIRST, 8, 3);
        assertQueueState(buffer, SECOND, 1, 10);
        assertFutureIsDone(future);

        // we should be able to add 3 more pages (the third will be queued)
        // although the first queue fetched the 4th page, the page has not been acknowledged yet
        addPage(buffer, createPage(11));
        addPage(buffer, createPage(12));
        future = enqueuePage(buffer, createPage(13));
        assertFalse(future.isDone());
        assertQueueState(buffer, FIRST, 11, 3);
        assertQueueState(buffer, SECOND, 4, 10);

        // acknowledge the receipt of the 3rd page and try to remove the 4th page from the first queue
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 4, sizeOfPages(1), NO_WAIT), bufferResult(4, createPage(4)));

        // the blocked page future above should be done
        assertFutureIsDone(future);
        assertQueueState(buffer, FIRST, 10, 4);
        assertQueueState(buffer, SECOND, 4, 10);

        //
        // finish the buffer
        assertFalse(buffer.isFinished());
        buffer.setNoMorePages();
        assertQueueState(buffer, FIRST, 10, 4);
        assertQueueState(buffer, SECOND, 4, 10);

        // not fully finished until all pages are consumed
        assertFalse(buffer.isFinished());

        // remove a page, not finished
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 5, sizeOfPages(1), NO_WAIT), bufferResult(5, createPage(5)));
        assertQueueState(buffer, FIRST, 9, 5);
        assertQueueState(buffer, SECOND, 4, 10);
        assertFalse(buffer.isFinished());

        // remove all remaining pages from first queue, should not be finished
        BufferResult x = getBufferResult(buffer, FIRST, 6, sizeOfPages(10), NO_WAIT);
        assertBufferResultEquals(TYPES, x, bufferResult(6, createPage(6),
                createPage(7),
                createPage(8),
                createPage(9),
                createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(buffer, FIRST, 8, 6);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));

        // finish first queue
        buffer.abort(FIRST);
        assertQueueClosed(buffer, FIRST, 14);
        assertQueueState(buffer, SECOND, 4, 10);
        assertFalse(buffer.isFinished());

        // remove all remaining pages from second queue, should be finished
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 10, sizeOfPages(10), NO_WAIT), bufferResult(10, createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(buffer, SECOND, 4, 10);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
        buffer.abort(SECOND);
        assertQueueClosed(buffer, FIRST, 14);
        assertQueueClosed(buffer, SECOND, 14);
        assertFinished(buffer);

        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
    }

    // TODO: remove this after PR #7987 is landed
    @Test
    public void testAcknowledge()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(BROADCAST);
        BroadcastOutputBuffer buffer = createBroadcastBuffer(outputBuffers, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        outputBuffers = createInitialEmptyOutputBuffers(BROADCAST).withBuffer(FIRST, BROADCAST_PARTITION_ID);

        // add a queue
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, FIRST, 3, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // acknowledge pages 0 and 1
        acknowledgeBufferResult(buffer, FIRST, 2);
        // only page 2 is not removed
        assertQueueState(buffer, FIRST, 1, 2);
        // acknowledge page 2
        acknowledgeBufferResult(buffer, FIRST, 3);
        // nothing left
        assertQueueState(buffer, FIRST, 0, 3);
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
        assertQueueState(buffer, FIRST, 3, 3);

        // getting new pages will again acknowledge the previously acknowledged pages but this is ok
        buffer.get(FIRST, 3, sizeOfPages(1)).cancel(true);
        assertQueueState(buffer, FIRST, 3, 3);
    }

    @Test
    public void testSharedBufferFull()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(buffer, createPage(1));
        addPage(buffer, createPage(2));

        // third page is blocked
        enqueuePage(buffer, createPage(3));
    }

    @Test
    public void testDuplicateRequests()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        // add a queue
        assertQueueState(buffer, FIRST, 3, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 3, 0);

        // get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 3, 0);

        // acknowledge the pages
        buffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);

        // attempt to get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, false));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 0, 3);
    }

    @Test
    public void testAddQueueAfterCreation()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        assertFalse(buffer.isFinished());

        try {
            buffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST)
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
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        buffer.setNoMorePages();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertEquals(buffer.getInfo().getTotalPagesSent(), 0);
    }

    @Test
    public void testAddQueueAfterNoMoreQueues()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // tell buffer no more queues will be added
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withNoMoreBufferIds());
        assertTrue(buffer.isFinished());

        // set no more queues a second time to assure that we don't get an exception or such
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withNoMoreBufferIds());
        assertTrue(buffer.isFinished());

        // set no more queues a third time to assure that we don't get an exception or such
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withNoMoreBufferIds());
        assertTrue(buffer.isFinished());
    }

    @Test
    public void testAddAfterDestroy()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        buffer.destroy();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertEquals(buffer.getInfo().getTotalPagesSent(), 0);
    }

    @Test
    public void testGetBeforeCreate()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0L, sizeOfPages(1));
        assertFalse(future.isDone());

        // add a page and verify the future is complete
        addPage(buffer, createPage(33));
        assertTrue(future.isDone());
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(33)));
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*does not contain.*\\[0]")
    public void testSetFinalBuffersWihtoutDeclaringUsedBuffer()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0L, sizeOfPages(1));
        assertFalse(future.isDone());

        // add a page and set no more pages
        addPage(buffer, createPage(33));
        buffer.setNoMorePages();

        // read the page
        assertTrue(future.isDone());
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(33)));

        // acknowledge the page and verify we are finished
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));
        buffer.abort(FIRST);

        // set final buffers to a set that does not contain the buffer, which will fail
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withNoMoreBufferIds());
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No more buffers already set")
    public void testUseUndeclaredBufferAfterFinalBuffersSet()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
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
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPages(2));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(1));
        assertFalse(future.isDone());

        // abort that buffer, and verify the future is complete and buffer is finished
        buffer.abort(FIRST);
        assertTrue(future.isDone());
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFullBufferBlocksWriter()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
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
    public void testAcknowledgementFreesWriters()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(buffer, createPage(1));
        addPage(buffer, createPage(2));
        assertQueueState(buffer, FIRST, 2, 0);

        // third page is blocked
        ListenableFuture<?> future = enqueuePage(buffer, createPage(3));

        // we should be blocked
        assertFalse(future.isDone());
        assertQueueState(buffer, FIRST, 3, 0);
        assertQueueState(buffer, SECOND, 3, 0);

        // acknowledge pages for first buffer, no space is freed
        buffer.get(FIRST, 2, sizeOfPages(10)).cancel(true);
        assertFalse(future.isDone());

        // acknowledge pages for second buffer, which makes space in the buffer
        buffer.get(SECOND, 2, sizeOfPages(10)).cancel(true);

        // writer should not be blocked
        assertFutureIsDone(future);
        assertQueueState(buffer, SECOND, 1, 2);
    }

    @Test
    public void testAbort()
    {
        BroadcastOutputBuffer bufferedBuffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 10; i++) {
            addPage(bufferedBuffer, createPage(i));
        }
        bufferedBuffer.setNoMorePages();

        assertBufferResultEquals(TYPES, getBufferResult(bufferedBuffer, FIRST, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        bufferedBuffer.abort(FIRST);
        assertQueueClosed(bufferedBuffer, FIRST, 0);
        assertBufferResultEquals(TYPES, getBufferResult(bufferedBuffer, FIRST, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));

        assertBufferResultEquals(TYPES, getBufferResult(bufferedBuffer, SECOND, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        bufferedBuffer.abort(SECOND);
        assertQueueClosed(bufferedBuffer, SECOND, 0);
        assertFinished(bufferedBuffer);
        assertBufferResultEquals(TYPES, getBufferResult(bufferedBuffer, SECOND, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFinishClosesEmptyQueues()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // finish while queues are empty
        buffer.setNoMorePages();

        assertQueueState(buffer, FIRST, 0, 0);
        assertQueueState(buffer, SECOND, 0, 0);

        buffer.abort(FIRST);
        buffer.abort(SECOND);

        assertQueueClosed(buffer, FIRST, 0);
        assertQueueClosed(buffer, SECOND, 0);
    }

    @Test
    public void testAbortFreesReader()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(buffer, createPage(0));
        assertTrue(future.isDone());

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // abort the buffer
        buffer.abort(FIRST);

        // verify the future completed
        // broadcast buffer does not return a "complete" result in this case, but it doesn't mapper
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));

        // further requests will see a completed result
        assertQueueClosed(buffer, FIRST, 1);
    }

    @Test
    public void testFinishFreesReader()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
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
        buffer.setNoMorePages();
        assertQueueState(buffer, FIRST, 0, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));
    }

    @Test
    public void testFinishFreesWriter()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
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
        buffer.get(FIRST, 1, sizeOfPages(1)).cancel(true);

        // verify we are still blocked because the buffer is full
        assertFalse(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // finish the query
        buffer.setNoMorePages();
        assertFalse(buffer.isFinished());

        // verify futures are complete
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);

        // get and acknowledge the last 6 pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(100), NO_WAIT),
                bufferResult(1, createPage(1), createPage(2), createPage(3), createPage(4), createPage(5), createPage(6)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 7, sizeOfPages(100), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 7, true));

        buffer.abort(FIRST);

        // verify finished
        assertFinished(buffer);
    }

    @Test
    public void testDestroyFreesReader()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
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

        // destroy the buffer
        buffer.destroy();
        assertQueueClosed(buffer, FIRST, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));
    }

    @Test
    public void testDestroyFreesWriter()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
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
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
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
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
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
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(BROADCAST)
                .withBuffer(FIRST, BROADCAST_PARTITION_ID);
        BroadcastOutputBuffer buffer = createBroadcastBuffer(outputBuffers, sizeOfPages(5));
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
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));

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
    public void testSharedBufferBlocking()
    {
        SettableFuture<?> blockedFuture = SettableFuture.create();
        MockMemoryReservationHandler reservationHandler = new MockMemoryReservationHandler(blockedFuture);
        AggregatedMemoryContext memoryContext = newRootAggregatedMemoryContext(reservationHandler, 0L);

        Page page = createPage(1);
        long pageSize = PAGES_SERDE.serialize(page).getRetainedSizeInBytes();

        // create a buffer that can only hold two pages
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), new DataSize(pageSize * 2, BYTE), memoryContext, directExecutor());
        OutputBufferMemoryManager memoryManager = buffer.getMemoryManager();

        // adding the first page will block as no memory is available (MockMemoryReservationHandler will return a future that is not done)
        enqueuePage(buffer, page);

        // more memory is available
        blockedFuture.set(null);
        memoryManager.onMemoryAvailable();
        assertTrue(memoryManager.getBufferBlockedFuture().isDone(), "buffer shouldn't be blocked");

        // we should be able to add one more page after more memory is available
        addPage(buffer, page);

        // the buffer is full now
        enqueuePage(buffer, page);
    }

    @Test
    public void testSharedBufferBlocking2()
    {
        // start with a complete future
        SettableFuture<?> blockedFuture = SettableFuture.create();
        blockedFuture.set(null);
        MockMemoryReservationHandler reservationHandler = new MockMemoryReservationHandler(blockedFuture);
        AggregatedMemoryContext memoryContext = newRootAggregatedMemoryContext(reservationHandler, 0L);

        Page page = createPage(1);
        long pageSize = PAGES_SERDE.serialize(page).getRetainedSizeInBytes();

        // create a buffer that can only hold two pages
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), new DataSize(pageSize * 2, BYTE), memoryContext, directExecutor());
        OutputBufferMemoryManager memoryManager = buffer.getMemoryManager();

        // add two pages to fill up the buffer (memory is available)
        addPage(buffer, page);
        addPage(buffer, page);

        // fill up the memory pool
        blockedFuture = SettableFuture.create();
        reservationHandler.updateBlockedFuture(blockedFuture);

        // allocate one more byte to make the buffer full
        memoryManager.updateMemoryUsage(1L);

        // more memory is available
        blockedFuture.set(null);
        memoryManager.onMemoryAvailable();

        // memoryManager should still return a blocked future as the buffer is still full
        assertFalse(memoryManager.getBufferBlockedFuture().isDone(), "buffer should be blocked");

        // remove all pages from the memory manager and the 1 byte that we added above
        memoryManager.updateMemoryUsage(-pageSize * 2 - 1);

        // now we have both buffer space and memory available, so memoryManager shouldn't be blocked
        assertTrue(memoryManager.getBufferBlockedFuture().isDone(), "buffer shouldn't be blocked");

        // we should be able to add two pages after more memory is available
        addPage(buffer, page);
        addPage(buffer, page);

        // the buffer is full now
        enqueuePage(buffer, page);
    }

    @Test
    public void testSharedBufferBlockingNoBlockOnFull()
    {
        SettableFuture<?> blockedFuture = SettableFuture.create();
        MockMemoryReservationHandler reservationHandler = new MockMemoryReservationHandler(blockedFuture);
        AggregatedMemoryContext memoryContext = newRootAggregatedMemoryContext(reservationHandler, 0L);

        Page page = createPage(1);
        long pageSize = PAGES_SERDE.serialize(page).getRetainedSizeInBytes();

        // create a buffer that can only hold two pages
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), new DataSize(pageSize * 2, BYTE), memoryContext, directExecutor());
        OutputBufferMemoryManager memoryManager = buffer.getMemoryManager();

        memoryManager.setNoBlockOnFull();

        // even if setNoBlockOnFull() is called the buffer should block on memory when we add the first page
        // as no memory is available (MockMemoryReservationHandler will return a future that is not done)
        enqueuePage(buffer, page);

        // more memory is available
        blockedFuture.set(null);
        memoryManager.onMemoryAvailable();
        assertTrue(memoryManager.getBufferBlockedFuture().isDone(), "buffer shouldn't be blocked");

        // we should be able to add one more page after more memory is available
        addPage(buffer, page);

        // the buffer is full now, but setNoBlockOnFull() is called so the buffer shouldn't block
        addPage(buffer, page);
    }

    private static class MockMemoryReservationHandler
            implements MemoryReservationHandler
    {
        private ListenableFuture<?> blockedFuture;

        public MockMemoryReservationHandler(ListenableFuture<?> blockedFuture)
        {
            this.blockedFuture = requireNonNull(blockedFuture, "blockedFuture is null");
        }

        @Override
        public ListenableFuture<?> reserveMemory(String allocationTag, long delta, boolean enforceBroadcastMemoryLimit)
        {
            return blockedFuture;
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta, boolean enforceBroadcastMemoryLimit)
        {
            return true;
        }

        public void updateBlockedFuture(ListenableFuture<?> blockedFuture)
        {
            this.blockedFuture = requireNonNull(blockedFuture);
        }
    }

    private BroadcastOutputBuffer createBroadcastBuffer(OutputBuffers outputBuffers, DataSize dataSize, AggregatedMemoryContext memoryContext, Executor notificationExecutor)
    {
        BroadcastOutputBuffer buffer = new BroadcastOutputBuffer(
                TASK_INSTANCE_ID,
                new StateMachine<>("bufferState", stateNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES),
                dataSize,
                () -> memoryContext.newLocalMemoryContext("test"),
                notificationExecutor);
        buffer.setOutputBuffers(outputBuffers);
        buffer.registerLifespanCompletionCallback(ignore -> {});
        return buffer;
    }

    @Test
    public void testBufferFinishesWhenClientBuffersDestroyed()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withBuffer(THIRD, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));

        // add pages before closing the buffers to make sure
        // that the buffers close even if there are pending pages
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // the buffer is in the NO_MORE_BUFFERS state now
        // and if we abort all the buffers it should destroy itself
        // and move to the FINISHED state
        buffer.abort(FIRST);
        assertFalse(buffer.isFinished());
        buffer.abort(SECOND);
        assertFalse(buffer.isFinished());
        buffer.abort(THIRD);
        assertTrue(buffer.isFinished());
    }

    @Test
    public void testForceFreeMemory()
            throws Throwable
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(1), 0);
        }
        OutputBufferMemoryManager memoryManager = buffer.getMemoryManager();
        assertTrue(memoryManager.getBufferedBytes() > 0);
        buffer.forceFreeMemory();
        assertEquals(memoryManager.getBufferedBytes(), 0);
        // adding a page after forceFreeMemory() should be NOOP
        addPage(buffer, createPage(1));
        assertEquals(memoryManager.getBufferedBytes(), 0);
    }

    private BroadcastOutputBuffer createBroadcastBuffer(OutputBuffers outputBuffers, DataSize dataSize)
    {
        BroadcastOutputBuffer buffer = new BroadcastOutputBuffer(
                TASK_INSTANCE_ID,
                new StateMachine<>("bufferState", stateNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES),
                dataSize,
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                stateNotificationExecutor);
        buffer.setOutputBuffers(outputBuffers);
        buffer.registerLifespanCompletionCallback(ignore -> {});
        return buffer;
    }

    private static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return createBufferResult(TASK_INSTANCE_ID, token, pages);
    }
}
