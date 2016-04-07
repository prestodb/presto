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

import static com.facebook.presto.OutputBuffers.BufferType.PARTITIONED;
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

public class TestPartitionedOutputBuffer
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
            createPartitionedBuffer(createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(FIRST, 0).withNoMoreBufferIds(), new DataSize(0, BYTE));
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException ignored) {
        }
        try {
            createPartitionedBuffer(createInitialEmptyOutputBuffers(PARTITIONED), new DataSize(0, BYTE));
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testSimplePartitioned()
            throws Exception
    {
        int firstPartition = 0;
        int secondPartition = 1;
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, firstPartition)
                        .withBuffer(SECOND, secondPartition)
                        .withNoMoreBufferIds(),
                sizeOfPages(20));

        // add three items to each buffer
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i), firstPartition);
            addPage(buffer, createPage(i), secondPartition);
        }

        // add first partition
        assertQueueState(buffer, FIRST, 3, 0);
        assertQueueState(buffer, SECOND, 3, 0);

        // get the three elements from the first buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 3, 0);
        assertQueueState(buffer, SECOND, 3, 0);

        // acknowledge first three pages in the first buffer
        buffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);
        // pages now acknowledged
        assertQueueState(buffer, FIRST, 0, 3);
        assertQueueState(buffer, SECOND, 3, 0);

        // Fill each buffer so they both have 10 buffered pages
        for (int i = 3; i < 13; i++) {
            addPage(buffer, createPage(i), firstPartition);
        }
        // (we already added 3 pages in the second buffer)
        for (int i = 3; i < 10; i++) {
            addPage(buffer, createPage(i), secondPartition);
        }
        assertQueueState(buffer, FIRST, 10, 3);
        assertQueueState(buffer, SECOND, 10, 0);

        // try to add one more page, which should block
        ListenableFuture<?> future = enqueuePage(buffer, createPage(13), firstPartition);
        assertFalse(future.isDone());
        assertQueueState(buffer, FIRST, 11, 3);
        assertQueueState(buffer, SECOND, 10, 0);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 3, sizeOfPages(1), NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertQueueState(buffer, FIRST, 11, 3);
        assertQueueState(buffer, SECOND, 10, 0);

        // we should still be blocked
        assertFalse(future.isDone());

        // read pages from second partition
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(10), NO_WAIT), bufferResult(
                0,
                createPage(0),
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
        assertQueueState(buffer, SECOND, 10, 0);
        // acknowledge the 10 pages
        buffer.get(SECOND, 10, sizeOfPages(3)).cancel(true);
        assertQueueState(buffer, SECOND, 0, 10);

        // since we consumed some pages, the blocked page future from above should be done
        assertFutureIsDone(future);
        assertQueueState(buffer, FIRST, 11, 3);
        assertQueueState(buffer, SECOND, 0, 10);

        // we should be able to add 2 more pages
        addPage(buffer, createPage(14), firstPartition);
        addPage(buffer, createPage(15), firstPartition);
        assertQueueState(buffer, FIRST, 13, 3);
        assertQueueState(buffer, SECOND, 0, 10);

        // remove a page from the first queue
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 4, sizeOfPages(1), NO_WAIT), bufferResult(4, createPage(4)));
        assertQueueState(buffer, FIRST, 12, 4);
        assertQueueState(buffer, SECOND, 0, 10);

        //
        // finish the buffer
        assertFalse(buffer.isFinished());
        buffer.setNoMorePages();
        assertQueueState(buffer, FIRST, 12, 4);
        assertQueueState(buffer, SECOND, 0, 10);
        buffer.abort(SECOND);
        assertQueueClosed(buffer, SECOND, 10);

        // not fully finished until all pages are consumed
        assertFalse(buffer.isFinished());

        // remove a page, not finished
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 5, sizeOfPages(1), NO_WAIT), bufferResult(5, createPage(5)));
        assertQueueState(buffer, FIRST, 11, 5);
        assertFalse(buffer.isFinished());

        // remove all remaining pages from first queue, should not be finished
        BufferResult x = getBufferResult(buffer, FIRST, 6, sizeOfPages(30), NO_WAIT);
        assertBufferResultEquals(TYPES, x, bufferResult(
                6,
                createPage(6),
                createPage(7),
                createPage(8),
                createPage(9),
                createPage(10),
                createPage(11),
                createPage(12),
                createPage(13),
                createPage(14),
                createPage(15)));
        assertQueueState(buffer, FIRST, 10, 6);
        // acknowledge all pages from the first partition, should transition to finished state
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 16, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 16, true));
        buffer.abort(FIRST);
        assertQueueClosed(buffer, FIRST, 16);
        assertFinished(buffer);
    }

    @Test
    public void testDuplicateRequests()
            throws Exception
    {
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        assertFalse(buffer.isFinished());

        try {
            buffer.setOutputBuffers(createInitialEmptyOutputBuffers(PARTITIONED)
                    .withBuffer(FIRST, 0)
                    .withBuffer(SECOND, 0)
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
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        buffer.setNoMorePages();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertEquals(buffer.getInfo().getTotalPagesSent(), 0);
    }

    @Test
    public void testAddAfterDestroy()
            throws Exception
    {
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        buffer.destroy();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertEquals(buffer.getInfo().getTotalPagesSent(), 0);
    }

    @Test
    public void testFullBufferBlocksWriter()
            throws Exception
    {
        int firstPartition = 0;
        int secondPartition = 1;
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, firstPartition)
                        .withBuffer(SECOND, secondPartition)
                        .withNoMoreBufferIds(),
                sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(buffer, createPage(1), firstPartition);
        addPage(buffer, createPage(2), secondPartition);

        // third page is blocked
        enqueuePage(buffer, createPage(3), secondPartition);
    }

    @Test
    public void testAcknowledgementFreesWriters()
            throws Exception
    {
        int firstPartition = 0;
        int secondPartition = 1;
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, firstPartition)
                        .withBuffer(SECOND, secondPartition)
                        .withNoMoreBufferIds(),
                sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(buffer, createPage(1), firstPartition);
        addPage(buffer, createPage(2), firstPartition);
        assertQueueState(buffer, FIRST, 2, 0);

        // third page is blocked
        ListenableFuture<?> future = enqueuePage(buffer, createPage(3), secondPartition);

        // we should be blocked
        assertFalse(future.isDone());
        assertQueueState(buffer, FIRST, 2, 0);
        assertQueueState(buffer, SECOND, 1, 0);

        // acknowledge pages for first partition, make space in the buffer
        buffer.get(FIRST, 2, sizeOfPages(10)).cancel(true);

        // writer should not be blocked
        assertFutureIsDone(future);
        assertQueueState(buffer, SECOND, 1, 0);
    }

    @Test
    public void testAbort()
            throws Exception
    {
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
                        .withBuffer(SECOND, 1)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i), 0);
            addPage(buffer, createPage(i), 1);
        }
        buffer.setNoMorePages();

        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        buffer.abort(FIRST);
        assertQueueClosed(buffer, FIRST, 0);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));

        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        buffer.abort(SECOND);
        assertQueueClosed(buffer, SECOND, 0);
        assertFinished(buffer);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFinishClosesEmptyQueues()
            throws Exception
    {
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
                        .withBuffer(SECOND, 1)
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
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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
        // partitioned buffer does not return a "complete" result in this case, but it doesn't matter
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));

        // further requests will see a completed result
        assertQueueClosed(buffer, FIRST, 1);
    }

    @Test
    public void testFinishFreesReader()
            throws Exception
    {
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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

    public static BufferResult getBufferResult(PartitionedOutputBuffer buffer, OutputBufferId bufferId, long sequenceId, DataSize maxSize, Duration maxWait)
    {
        CompletableFuture<BufferResult> future = buffer.get(bufferId, sequenceId, maxSize);
        return getFuture(future, maxWait);
    }

    public static BufferResult getFuture(CompletableFuture<BufferResult> future, Duration maxWait)
    {
        return tryGetFutureValue(future, (int) maxWait.toMillis(), MILLISECONDS).get();
    }

    private static ListenableFuture<?> enqueuePage(PartitionedOutputBuffer buffer, Page page)
    {
        ListenableFuture<?> future = buffer.enqueue(page);
        assertFalse(future.isDone());
        return future;
    }

    private static ListenableFuture<?> enqueuePage(PartitionedOutputBuffer buffer, Page page, int partition)
    {
        ListenableFuture<?> future = buffer.enqueue(partition, page);
        assertFalse(future.isDone());
        return future;
    }

    private static void addPage(PartitionedOutputBuffer buffer, Page page)
    {
        assertTrue(buffer.enqueue(page).isDone(), "Expected add page to not block");
    }

    private static void addPage(PartitionedOutputBuffer buffer, Page page, int partition)
    {
        assertTrue(buffer.enqueue(partition, page).isDone(), "Expected add page to not block");
    }

    private static void assertQueueState(
            PartitionedOutputBuffer buffer,
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
    private static void assertQueueClosed(PartitionedOutputBuffer buffer, OutputBufferId bufferId, int pagesSent)
    {
        BufferInfo bufferInfo = getBufferInfo(buffer, bufferId);
        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getPagesSent(), pagesSent);
        assertEquals(bufferInfo.isFinished(), true);
    }

    private PartitionedOutputBuffer createPartitionedBuffer(OutputBuffers buffers, DataSize dataSize)
    {
        return new PartitionedOutputBuffer(
                TASK_INSTANCE_ID,
                new StateMachine<>("bufferState", stateNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES),
                buffers,
                dataSize,
                ignored -> { },
                stateNotificationExecutor);
    }

    private static BufferInfo getBufferInfo(PartitionedOutputBuffer buffer, OutputBufferId bufferId)
    {
        for (BufferInfo bufferInfo : buffer.getInfo().getBuffers()) {
            if (bufferInfo.getBufferId().equals(bufferId)) {
                return bufferInfo;
            }
        }
        return null;
    }

    private static void assertFinished(PartitionedOutputBuffer buffer)
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
