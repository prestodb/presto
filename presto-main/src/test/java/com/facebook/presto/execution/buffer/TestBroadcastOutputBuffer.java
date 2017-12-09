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
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.OutputBuffers.BufferType.BROADCAST;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.execution.buffer.BufferSummary.emptySummary;
import static com.facebook.presto.execution.buffer.BufferTestUtils.MAX_WAIT;
import static com.facebook.presto.execution.buffer.BufferTestUtils.NO_WAIT;
import static com.facebook.presto.execution.buffer.BufferTestUtils.addPage;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertBufferResultEquals;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertBufferSummaryEquals;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertFinished;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertFutureIsDone;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertQueueClosed;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertQueueState;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createBufferResult;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createBufferSummary;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createPage;
import static com.facebook.presto.execution.buffer.BufferTestUtils.enqueuePage;
import static com.facebook.presto.execution.buffer.BufferTestUtils.getBufferResult;
import static com.facebook.presto.execution.buffer.BufferTestUtils.getBufferSummary;
import static com.facebook.presto.execution.buffer.BufferTestUtils.getFuture;
import static com.facebook.presto.execution.buffer.BufferTestUtils.sizeOfPagesInBytes;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestBroadcastOutputBuffer
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
            createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST).withBuffer(FIRST, BROADCAST_PARTITION_ID).withNoMoreBufferIds(), 0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException ignored) {
        }
        try {
            createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), 0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testSimple()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(BROADCAST);
        BroadcastOutputBuffer buffer = createBroadcastBuffer(outputBuffers, sizeOfPagesInBytes(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        outputBuffers = createInitialEmptyOutputBuffers(BROADCAST).withBuffer(FIRST, BROADCAST_PARTITION_ID);

        // add a queue
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, FIRST, 3, 0);

        // get the three elements
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 0, sizeOfPagesInBytes(10), NO_WAIT), bufferSummary(0, createPage(0), createPage(1), createPage(2)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 3, 0);

        // acknowledge first three pages
        buffer.getSummary(FIRST, 3, sizeOfPagesInBytes(10)).cancel(true);
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
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 3, sizeOfPagesInBytes(1), NO_WAIT), bufferSummary(3, createPage(3)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 3, sizeOfPagesInBytes(1)), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertQueueState(buffer, FIRST, 8, 3);

        // we should still be blocked
        assertFalse(future.isDone());

        //
        // add another buffer and verify it sees all pages
        outputBuffers = outputBuffers.withBuffer(SECOND, BROADCAST_PARTITION_ID);
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, SECOND, 11, 0);
        assertBufferSummaryEquals(getBufferSummary(buffer, SECOND, 0, sizeOfPagesInBytes(10), NO_WAIT), bufferSummary(0, createPage(0),
                createPage(1),
                createPage(2),
                createPage(3),
                createPage(4),
                createPage(5),
                createPage(6),
                createPage(7),
                createPage(8),
                createPage(9)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0),
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
        buffer.getSummary(SECOND, 10, sizeOfPagesInBytes(10)).cancel(true);
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
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 4, sizeOfPagesInBytes(1), NO_WAIT), bufferSummary(4, createPage(4)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 4, sizeOfPagesInBytes(1)), bufferResult(4, createPage(4)));

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
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 5, sizeOfPagesInBytes(1), NO_WAIT), bufferSummary(5, createPage(5)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 5, sizeOfPagesInBytes(1)), bufferResult(5, createPage(5)));
        assertQueueState(buffer, FIRST, 9, 5);
        assertQueueState(buffer, SECOND, 4, 10);
        assertFalse(buffer.isFinished());

        // remove all remaining pages from first queue, should not be finished
        getBufferSummary(buffer, FIRST, 6, sizeOfPagesInBytes(10), NO_WAIT);
        BufferResult x = getBufferResult(buffer, FIRST, 6, sizeOfPagesInBytes(10));
        assertBufferResultEquals(TYPES, x, bufferResult(6, createPage(6),
                createPage(7),
                createPage(8),
                createPage(9),
                createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(buffer, FIRST, 8, 6);
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 14, sizeOfPagesInBytes(10), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 14, true));

        // finish first queue
        buffer.abort(FIRST);
        assertQueueClosed(buffer, FIRST, 14);
        assertQueueState(buffer, SECOND, 4, 10);
        assertFalse(buffer.isFinished());

        // remove all remaining pages from second queue, should be finished
        assertBufferSummaryEquals(getBufferSummary(buffer, SECOND, 10, sizeOfPagesInBytes(10), NO_WAIT), bufferSummary(10, createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 10, sizeOfPagesInBytes(10)), bufferResult(10, createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(buffer, SECOND, 4, 10);
        assertBufferSummaryEquals(getBufferSummary(buffer, SECOND, 14, sizeOfPagesInBytes(10), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 14, true));
        buffer.abort(SECOND);
        assertQueueClosed(buffer, FIRST, 14);
        assertQueueClosed(buffer, SECOND, 14);
        assertFinished(buffer);

        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 14, sizeOfPagesInBytes(10), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 14, true));
        assertBufferSummaryEquals(getBufferSummary(buffer, SECOND, 14, sizeOfPagesInBytes(10), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 14, true));
    }

    @Test
    public void testSharedBufferFull()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPagesInBytes(2));

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
                sizeOfPagesInBytes(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        // add a queue
        assertQueueState(buffer, FIRST, 3, 0);

        // get the three elements
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 0, sizeOfPagesInBytes(10), NO_WAIT), bufferSummary(0, createPage(0), createPage(1), createPage(2)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 3, 0);

        // peek the three elements again
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 0, sizeOfPagesInBytes(10), NO_WAIT), bufferSummary(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 3, 0);
        // get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 3, 0);

        // acknowledge the pages
        buffer.getSummary(FIRST, 3, sizeOfPagesInBytes(10)).cancel(true);

        // attempt to get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPagesInBytes(10)), emptyResults(TASK_INSTANCE_ID, 0, false));
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
                sizeOfPagesInBytes(10));

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
                sizeOfPagesInBytes(10));
        buffer.setNoMorePages();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertEquals(buffer.getInfo().getTotalPagesSent(), 0);
    }

    @Test
    public void testAddQueueAfterNoMoreQueues()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPagesInBytes(10));
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
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPagesInBytes(10));
        buffer.destroy();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertEquals(buffer.getInfo().getTotalPagesSent(), 0);
    }

    @Test
    public void testGetBeforeCreate()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPagesInBytes(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferSummary> future = buffer.getSummary(FIRST, 0L, sizeOfPagesInBytes(1));
        assertFalse(future.isDone());

        // add a page and verify the future is complete
        addPage(buffer, createPage(33));
        assertTrue(future.isDone());
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), bufferSummary(0, createPage(33)));
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*does not contain.*\\[0]")
    public void testSetFinalBuffersWihtoutDeclaringUsedBuffer()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPagesInBytes(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferSummary> future = buffer.getSummary(FIRST, 0L, sizeOfPagesInBytes(1));
        assertFalse(future.isDone());

        // add a page and set no more pages
        addPage(buffer, createPage(33));
        buffer.setNoMorePages();

        // read the page
        assertTrue(future.isDone());
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), bufferSummary(0, createPage(33)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPagesInBytes(1)), bufferResult(0, createPage(33)));

        // acknowledge the page and verify we are finished
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 1, sizeOfPagesInBytes(10), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 1, true));
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
                sizeOfPagesInBytes(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that was not declared, which will fail
        buffer.getData(SECOND, 0L, sizeOfPagesInBytes(1));
    }

    @Test
    public void testAbortBeforeCreate()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(createInitialEmptyOutputBuffers(BROADCAST), sizeOfPagesInBytes(2));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferSummary> future = buffer.getSummary(FIRST, 0, sizeOfPagesInBytes(1));
        assertFalse(future.isDone());

        // abort that buffer, and verify the future is complete and buffer is finished
        buffer.abort(FIRST);
        assertTrue(future.isDone());
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 0, sizeOfPagesInBytes(10), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFullBufferBlocksWriter()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPagesInBytes(2));

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
                sizeOfPagesInBytes(2));

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
        buffer.getSummary(FIRST, 2, sizeOfPagesInBytes(10)).cancel(true);
        assertFalse(future.isDone());

        // acknowledge pages for second buffer, which makes space in the buffer
        buffer.getSummary(SECOND, 2, sizeOfPagesInBytes(10)).cancel(true);

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
                sizeOfPagesInBytes(10));

        // fill the buffer
        for (int i = 0; i < 10; i++) {
            addPage(bufferedBuffer, createPage(i));
        }
        bufferedBuffer.setNoMorePages();

        assertBufferSummaryEquals(getBufferSummary(bufferedBuffer, FIRST, 0, sizeOfPagesInBytes(1), NO_WAIT), bufferSummary(0, createPage(0)));
        assertBufferResultEquals(TYPES, getBufferResult(bufferedBuffer, FIRST, 0, sizeOfPagesInBytes(1)), bufferResult(0, createPage(0)));
        bufferedBuffer.abort(FIRST);
        assertQueueClosed(bufferedBuffer, FIRST, 0);
        assertBufferSummaryEquals(getBufferSummary(bufferedBuffer, FIRST, 1, sizeOfPagesInBytes(1), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 0, true));

        assertBufferSummaryEquals(getBufferSummary(bufferedBuffer, SECOND, 0, sizeOfPagesInBytes(1), NO_WAIT), bufferSummary(0, createPage(0)));
        assertBufferResultEquals(TYPES, getBufferResult(bufferedBuffer, SECOND, 0, sizeOfPagesInBytes(1)), bufferResult(0, createPage(0)));
        bufferedBuffer.abort(SECOND);
        assertQueueClosed(bufferedBuffer, SECOND, 0);
        assertFinished(bufferedBuffer);
        assertBufferSummaryEquals(getBufferSummary(bufferedBuffer, SECOND, 1, sizeOfPagesInBytes(1), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFinishClosesEmptyQueues()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPagesInBytes(10));

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
                sizeOfPagesInBytes(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferSummary> future = buffer.getSummary(FIRST, 0, sizeOfPagesInBytes(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(buffer, createPage(0));
        assertTrue(future.isDone());

        // verify we got one page
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), bufferSummary(0, createPage(0)));
        assertBufferResultEquals(TYPES, buffer.getData(FIRST, 0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.getSummary(FIRST, 1, sizeOfPagesInBytes(10));
        assertFalse(future.isDone());

        // abort the buffer
        buffer.abort(FIRST);

        // verify the future completed
        // broadcast buffer does not return a "complete" result in this case, but it doesn't mapper
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), emptySummary(TASK_INSTANCE_ID, 1, false));

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
                sizeOfPagesInBytes(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferSummary> future = buffer.getSummary(FIRST, 0, sizeOfPagesInBytes(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), bufferSummary(0, createPage(0)));
        assertBufferResultEquals(TYPES, buffer.getData(FIRST, 0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.getSummary(FIRST, 1, sizeOfPagesInBytes(10));
        assertFalse(future.isDone());

        // finish the buffer
        buffer.setNoMorePages();
        assertQueueState(buffer, FIRST, 0, 1);

        // verify the future completed
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), emptySummary(TASK_INSTANCE_ID, 1, true));
    }

    @Test
    public void testFinishFreesWriter()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPagesInBytes(5));
        assertFalse(buffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // enqueue the addition two pages more pages
        ListenableFuture<?> firstEnqueuePage = enqueuePage(buffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(buffer, createPage(6));

        // get and acknowledge one page
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 0, sizeOfPagesInBytes(1), MAX_WAIT), bufferSummary(0, createPage(0)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPagesInBytes(1)), bufferResult(0, createPage(0)));
        buffer.getSummary(FIRST, 1, sizeOfPagesInBytes(1)).cancel(true);

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
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 1, sizeOfPagesInBytes(100), NO_WAIT),
                bufferSummary(1, createPage(1), createPage(2), createPage(3), createPage(4), createPage(5), createPage(6)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPagesInBytes(100)),
                bufferResult(1, createPage(1), createPage(2), createPage(3), createPage(4), createPage(5), createPage(6)));
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 7, sizeOfPagesInBytes(100), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 7, true));

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
                sizeOfPagesInBytes(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferSummary> future = buffer.getSummary(FIRST, 0, sizeOfPagesInBytes(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one page
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), bufferSummary(0, createPage(0)));
        assertBufferResultEquals(TYPES, buffer.getData(FIRST, 0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.getSummary(FIRST, 1, sizeOfPagesInBytes(10));
        assertFalse(future.isDone());

        // destroy the buffer
        buffer.destroy();
        assertQueueClosed(buffer, FIRST, 1);

        // verify the future completed
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), emptySummary(TASK_INSTANCE_ID, 1, false));
    }

    @Test
    public void testDestroyFreesWriter()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPagesInBytes(5));
        assertFalse(buffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // add two pages to the buffer queue
        ListenableFuture<?> firstEnqueuePage = enqueuePage(buffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(buffer, createPage(6));

        // get and acknowledge one page
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 0, sizeOfPagesInBytes(1), MAX_WAIT), bufferSummary(0, createPage(0)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPagesInBytes(1)), bufferResult(0, createPage(0)));
        buffer.getSummary(FIRST, 1, sizeOfPagesInBytes(1)).cancel(true);

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
                sizeOfPagesInBytes(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferSummary> future = buffer.getSummary(FIRST, 0, sizeOfPagesInBytes(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one page
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), bufferSummary(0, createPage(0)));
        assertBufferResultEquals(TYPES, buffer.getData(FIRST, 0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.getSummary(FIRST, 1, sizeOfPagesInBytes(10));
        assertFalse(future.isDone());

        // fail the buffer
        buffer.fail();

        // future should have not finished
        assertFalse(future.isDone());

        // attempt to get another page, and verify we are blocked
        future = buffer.getSummary(FIRST, 1, sizeOfPagesInBytes(10));
        assertFalse(future.isDone());
    }

    @Test
    public void testFailFreesWriter()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPagesInBytes(5));
        assertFalse(buffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // add two pages to the buffer queue
        ListenableFuture<?> firstEnqueuePage = enqueuePage(buffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(buffer, createPage(6));

        // get and acknowledge one page
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 0, sizeOfPagesInBytes(1), MAX_WAIT), bufferSummary(0, createPage(0)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPagesInBytes(1)), bufferResult(0, createPage(0)));
        buffer.getSummary(FIRST, 1, sizeOfPagesInBytes(1)).cancel(true);

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
        BroadcastOutputBuffer buffer = createBroadcastBuffer(outputBuffers, sizeOfPagesInBytes(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferSummary> future = buffer.getSummary(FIRST, 0, sizeOfPagesInBytes(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one page
        addPage(buffer, createPage(0));

        // verify we got one page
        BufferSummary summary = getFuture(future, NO_WAIT);
        assertBufferSummaryEquals(summary, bufferSummary(0, createPage(0)));

        // fail the buffer
        buffer.fail();

        // add a buffer
        outputBuffers = outputBuffers.withBuffer(SECOND, BROADCAST_PARTITION_ID);
        buffer.setOutputBuffers(outputBuffers);

        // attempt to get page, and verify we are blocked
        future = buffer.getSummary(FIRST, 1, sizeOfPagesInBytes(10));
        assertFalse(future.isDone());
        future = buffer.getSummary(SECOND, 0, sizeOfPagesInBytes(10));
        assertFalse(future.isDone());

        // set no more buffers
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        buffer.setOutputBuffers(outputBuffers);

        // attempt to get page, and verify we are blocked
        future = buffer.getSummary(FIRST, 1, sizeOfPagesInBytes(10));
        assertFalse(future.isDone());
        future = buffer.getSummary(SECOND, 0, sizeOfPagesInBytes(10));
        assertFalse(future.isDone());
    }

    @Test
    public void testBufferCompletion()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                createInitialEmptyOutputBuffers(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPagesInBytes(5));

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
        assertBufferSummaryEquals(getBufferSummary(buffer, FIRST, 0, sizeOfPagesInBytes(5), MAX_WAIT), createBufferSummary(TASK_INSTANCE_ID, 0, pages));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPagesInBytes(5)), createBufferResult(TASK_INSTANCE_ID, 0, pages));

        // buffer is not finished
        assertFalse(buffer.isFinished());

        // there are no more pages and no more buffers, but buffer is not finished because it didn't receive an acknowledgement yet
        assertFalse(buffer.isFinished());

        // ask the buffer to finish
        buffer.abort(FIRST);

        // verify that the buffer is finished
        assertTrue(buffer.isFinished());
    }

    private BroadcastOutputBuffer createBroadcastBuffer(OutputBuffers outputBuffers, long dataSizeInBytes)
    {
        BroadcastOutputBuffer buffer = new BroadcastOutputBuffer(
                TASK_INSTANCE_ID,
                new StateMachine<>("bufferState", stateNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES),
                new DataSize(dataSizeInBytes, BYTE),
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext()),
                stateNotificationExecutor);
        buffer.setOutputBuffers(outputBuffers);
        return buffer;
    }

    private static BufferSummary bufferSummary(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return createBufferSummary(TASK_INSTANCE_ID, token, pages);
    }

    private static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return createBufferResult(TASK_INSTANCE_ID, token, pages);
    }
}
