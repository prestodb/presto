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

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.OutputBuffers.OutputBufferId;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.operator.PageAssertions;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.OutputBuffers.BufferType.BROADCAST;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestBroadcastOutputBuffer
{
    private static final Duration NO_WAIT = new Duration(0, MILLISECONDS);
    private static final Duration MAX_WAIT = new Duration(1, SECONDS);
    private static final DataSize PAGE_SIZE = new DataSize(createPage(42).getRetainedSizeInBytes(), BYTE);
    private static final String TASK_INSTANCE_ID = "task-instance-id";

    private static final ImmutableList<BigintType> TYPES = ImmutableList.of(BIGINT);
    private static final OutputBufferId FIRST = new OutputBufferId(0);
    private static final OutputBufferId SECOND = new OutputBufferId(1);

    private ScheduledExecutorService stateNotificationExecutor;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        stateNotificationExecutor = newScheduledThreadPool(5, daemonThreadsNamed("test-%s"));
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        if (stateNotificationExecutor != null) {
            stateNotificationExecutor.shutdownNow();
            stateNotificationExecutor = null;
        }
    }

    @Test
    public void testInvalidConstructorArg()
            throws Exception
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
            throws Exception
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

    @Test
    public void testSharedBufferFull()
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // tell buffer no more queues will be added
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        // set no more queues a second time to assure that we don't get an exception or such
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        // set no more queues a third time to assure that we don't get an exception or such
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        try {
            OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(BROADCAST)
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
            throws Exception
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
            throws Exception
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        CompletableFuture<BufferResult> future = buffer.get(FIRST, (long) 0, sizeOfPages(1));
        assertFalse(future.isDone());

        // add a page and verify the future is complete
        addPage(buffer, createPage(33));
        assertTrue(future.isDone());
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(33)));
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*does not contain.*\\[0\\]")
    public void testFinishWithoutDeclaringBuffer()
            throws Exception
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        CompletableFuture<BufferResult> future = buffer.get(FIRST, (long) 0, sizeOfPages(1));
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

        // destroy without declaring the buffer, which will fail
        buffer.destroy();
    }

    @Test
    public void testAbortBeforeCreate()
            throws Exception
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPages(2));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        CompletableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(1));
        assertFalse(future.isDone());

        // abort that buffer, and verify the future is complete and buffer is finished
        buffer.abort(FIRST);
        assertTrue(future.isDone());
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFullBufferBlocksWriter()
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        CompletableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

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
            throws Exception
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        CompletableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

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
            throws Exception
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
            throws Exception
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        CompletableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

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
            throws Exception
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
            throws Exception
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        CompletableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

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
            throws Exception
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
    public void testBufferCompletion()
            throws Exception
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
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(5), MAX_WAIT), bufferResult(0, pages));

        // buffer is not finished
        assertFalse(buffer.isFinished());

        // there are no more pages and no more buffers, but buffer is not finished because it didn't receive an acknowledgement yet
        assertFalse(buffer.isFinished());

        // ask the buffer to finish
        buffer.abort(FIRST);

        // verify that the buffer is finished
        assertTrue(buffer.isFinished());
    }

    public static BufferResult getBufferResult(BroadcastOutputBuffer buffer, OutputBufferId bufferId, long sequenceId, DataSize maxSize, Duration maxWait)
    {
        CompletableFuture<BufferResult> future = buffer.get(bufferId, sequenceId, maxSize);
        return getFuture(future, maxWait);
    }

    public static BufferResult getFuture(CompletableFuture<BufferResult> future, Duration maxWait)
    {
        return tryGetFutureValue(future, (int) maxWait.toMillis(), MILLISECONDS).get();
    }

    private static ListenableFuture<?> enqueuePage(BroadcastOutputBuffer buffer, Page page)
    {
        ListenableFuture<?> future = buffer.enqueue(page);
        assertFalse(future.isDone());
        return future;
    }

    private static void addPage(BroadcastOutputBuffer buffer, Page page)
    {
        assertTrue(buffer.enqueue(page).isDone(), "Expected add page to not block");
    }

    private static void assertQueueState(
            BroadcastOutputBuffer buffer,
            OutputBufferId bufferId,
            int bufferedPages,
            int pagesSent)
    {
        assertEquals(
                getBufferInfo(buffer, bufferId),
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
    private static void assertQueueClosed(BroadcastOutputBuffer buffer, OutputBufferId bufferId, int pagesSent)
    {
        BufferInfo bufferInfo = getBufferInfo(buffer, bufferId);
        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getPagesSent(), pagesSent);
        assertEquals(bufferInfo.isFinished(), true);
    }

    private BroadcastOutputBuffer createBroadcastBuffer(OutputBuffers outputBuffers, DataSize dataSize)
    {
        BroadcastOutputBuffer buffer = new BroadcastOutputBuffer(
                TASK_INSTANCE_ID,
                new StateMachine<>("bufferState", stateNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES),
               dataSize,
                ignored -> { },
                stateNotificationExecutor);
        buffer.setOutputBuffers(outputBuffers);
        return buffer;
    }

    private static BufferInfo getBufferInfo(BroadcastOutputBuffer buffer, OutputBufferId bufferId)
    {
        for (BufferInfo bufferInfo : buffer.getInfo().getBuffers()) {
            if (bufferInfo.getBufferId().equals(bufferId)) {
                return bufferInfo;
            }
        }
        return null;
    }

    private static void assertFinished(BroadcastOutputBuffer buffer)
            throws Exception
    {
        assertTrue(buffer.isFinished());
        for (BufferInfo bufferInfo : buffer.getInfo().getBuffers()) {
            assertTrue(bufferInfo.isFinished());
            assertEquals(bufferInfo.getBufferedPages(), 0);
        }
    }

    private static void assertBufferResultEquals(List<? extends Type> types, BufferResult actual, BufferResult expected)
    {
        assertEquals(actual.getPages().size(), expected.getPages().size());
        assertEquals(actual.getToken(), expected.getToken());
        for (int i = 0; i < actual.getPages().size(); i++) {
            Page actualPage = actual.getPages().get(i);
            Page expectedPage = expected.getPages().get(i);
            assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
            PageAssertions.assertPageEquals(types, actualPage, expectedPage);
        }
        assertEquals(actual.isBufferComplete(), expected.isBufferComplete());
    }

    private static void assertFutureIsDone(Future<?> future)
    {
        tryGetFutureValue(future, 5, SECONDS);
        assertTrue(future.isDone());
    }

    public static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return bufferResult(token, pages);
    }

    public static BufferResult bufferResult(long token, List<Page> pages)
    {
        checkArgument(!pages.isEmpty(), "pages is empty");
        return new BufferResult(TASK_INSTANCE_ID, token, token + pages.size(), false, pages);
    }

    private static Page createPage(int i)
    {
        return new Page(BlockAssertions.createLongsBlock(i));
    }

    public static DataSize sizeOfPages(int count)
    {
        return new DataSize(PAGE_SIZE.toBytes() * count, BYTE);
    }
}
