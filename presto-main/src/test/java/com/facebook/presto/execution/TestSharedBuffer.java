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
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.UnpartitionedPagePartitionFunction;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.operator.PageAssertions;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.execution.BufferResult.emptyResults;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSharedBuffer
{
    private static final Duration NO_WAIT = new Duration(0, TimeUnit.MILLISECONDS);
    private static final Duration MAX_WAIT = new Duration(1, TimeUnit.SECONDS);
    private static final DataSize PAGE_SIZE = new DataSize(createPage(42).getSizeInBytes(), BYTE);
    private static final TaskId TASK_ID = new TaskId("query", "stage", "task");

    private static final ImmutableList<BigintType> TYPES = ImmutableList.of(BIGINT);
    public static final TaskId FIRST = new TaskId("query", "stage", "first_task");
    public static final TaskId SECOND = new TaskId("query", "stage", "second_task");
    public static final TaskId QUEUE = new TaskId("query", "stage", "queue");
    public static final TaskId FOO = new TaskId("foo", "bar", "baz");

    private static Page createPage(int i)
    {
        return new Page(BlockAssertions.createLongsBlock(i));
    }

    public static DataSize sizeOfPages(int count)
    {
        return new DataSize(PAGE_SIZE.toBytes() * count, Unit.BYTE);
    }

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
            new SharedBuffer(TASK_ID, stateNotificationExecutor, new DataSize(0, Unit.BYTE));
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testSimple()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(sharedBuffer, createPage(i));
        }

        OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(FIRST, new UnpartitionedPagePartitionFunction());

        // add a queue
        sharedBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(sharedBuffer, FIRST, 3, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, FIRST, 3, 0);

        // acknowledge first three pages
        sharedBuffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);
        // pages now acknowledged
        assertQueueState(sharedBuffer, FIRST, 0, 3);

        // fill the buffer (we already added 3 pages)
        for (int i = 3; i < 10; i++) {
            addPage(sharedBuffer, createPage(i));
        }
        assertQueueState(sharedBuffer, FIRST, 7, 3);

        // try to add one more page, which should block
        ListenableFuture<?> future = enqueuePage(sharedBuffer, createPage(10));

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 3, sizeOfPages(1), NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertQueueState(sharedBuffer, FIRST, 8, 3);

        // we should still be blocked
        assertFalse(future.isDone());

        //
        // add another buffer and verify it sees all pages
        outputBuffers = outputBuffers.withBuffer(SECOND, new UnpartitionedPagePartitionFunction());
        sharedBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(sharedBuffer, SECOND, 11, 0);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, SECOND, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0),
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
        assertQueueState(sharedBuffer, SECOND, 11, 0);
        // acknowledge the 10 pages
        sharedBuffer.get(SECOND, 10, sizeOfPages(10)).cancel(true);
        assertQueueState(sharedBuffer, SECOND, 1, 10);

        //
        // tell shared buffer there will be no more queues
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        sharedBuffer.setOutputBuffers(outputBuffers);

        // since both queues consumed the first three pages, the blocked page future from above should be done
        future.get(1, TimeUnit.SECONDS);

        // we should be able to add 3 more pages (the third will be queued)
        // although the first queue fetched the 4th page, the page has not been acknowledged yet
        addPage(sharedBuffer, createPage(11));
        addPage(sharedBuffer, createPage(12));
        future = enqueuePage(sharedBuffer, createPage(13));
        assertQueueState(sharedBuffer, FIRST, 11, 3);
        assertQueueState(sharedBuffer, SECOND, 4, 10);

        // remove a page from the first queue
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 4, sizeOfPages(1), NO_WAIT), bufferResult(4, createPage(4)));

        // the blocked page future above should be done
        future.get(1, TimeUnit.SECONDS);
        assertQueueState(sharedBuffer, FIRST, 10, 4);
        assertQueueState(sharedBuffer, SECOND, 4, 10);

        //
        // finish the buffer
        assertFalse(sharedBuffer.isFinished());
        sharedBuffer.setNoMorePages();
        assertQueueState(sharedBuffer, FIRST, 10, 4);
        assertQueueState(sharedBuffer, SECOND, 4, 10);

        // not fully finished until all pages are consumed
        assertFalse(sharedBuffer.isFinished());

        // remove a page, not finished
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 5, sizeOfPages(1), NO_WAIT), bufferResult(5, createPage(5)));
        assertQueueState(sharedBuffer, FIRST, 9, 5);
        assertQueueState(sharedBuffer, SECOND, 4, 10);
        assertFalse(sharedBuffer.isFinished());

        // remove all remaining pages from first queue, should not be finished
        BufferResult x = getBufferResult(sharedBuffer, FIRST, 6, sizeOfPages(10), NO_WAIT);
        assertBufferResultEquals(TYPES, x, bufferResult(6, createPage(6),
                createPage(7),
                createPage(8),
                createPage(9),
                createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(sharedBuffer, FIRST, 8, 6);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 14, sizeOfPages(10), NO_WAIT), emptyResults(14, true));
        assertQueueClosed(sharedBuffer, FIRST, 14);
        assertQueueState(sharedBuffer, SECOND, 4, 10);
        assertFalse(sharedBuffer.isFinished());

        // remove all remaining pages from second queue, should be finished
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, SECOND, 10, sizeOfPages(10), NO_WAIT), bufferResult(10, createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(sharedBuffer, SECOND, 4, 10);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, SECOND, 14, sizeOfPages(10), NO_WAIT), emptyResults(14, true));
        assertQueueClosed(sharedBuffer, FIRST, 14);
        assertQueueClosed(sharedBuffer, SECOND, 14);
        assertFinished(sharedBuffer);

        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 14, sizeOfPages(10), NO_WAIT), emptyResults(14, true));
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, SECOND, 14, sizeOfPages(10), NO_WAIT), emptyResults(14, true));
    }

    public static BufferResult getBufferResult(SharedBuffer sharedBuffer, TaskId outputId, long sequenceId, DataSize maxSize, Duration maxWait)
    {
        ListenableFuture<BufferResult> future = sharedBuffer.get(outputId, sequenceId, maxSize);
        return getFuture(future, maxWait);
    }

    public static BufferResult getFuture(ListenableFuture<BufferResult> future, Duration maxWait)
    {
        try {
            return future.get(maxWait.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
            throw Throwables.propagate(e);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }

    @Test
    public void testDuplicateRequests()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(10));
        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(sharedBuffer, createPage(i));
        }

        // add a queue
        OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS;
        outputBuffers = outputBuffers.withBuffer(FIRST, new UnpartitionedPagePartitionFunction());
        sharedBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(sharedBuffer, FIRST, 3, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, FIRST, 3, 0);

        // get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, FIRST, 3, 0);

        // acknowledge the pages
        sharedBuffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);

        // attempt to get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(0, false));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, FIRST, 0, 3);
    }

    @Test
    public void testAddQueueAfterNoMoreQueues()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(10));
        assertFalse(sharedBuffer.isFinished());

        // tell buffer no more queues will be added
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS.withNoMoreBufferIds());
        assertFalse(sharedBuffer.isFinished());

        // set no more queues a second time to assure that we don't get an exception or such
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS.withNoMoreBufferIds());
        assertFalse(sharedBuffer.isFinished());

        // set no more queues a third time to assure that we don't get an exception or such
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS.withNoMoreBufferIds());
        assertFalse(sharedBuffer.isFinished());

        try {
            OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS
                    .withBuffer(FOO, new UnpartitionedPagePartitionFunction())
                    .withNoMoreBufferIds();

            sharedBuffer.setOutputBuffers(outputBuffers);
            fail("Expected IllegalStateException from addQueue after noMoreQueues has been called");
        }
        catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testAddQueueAfterDestroy()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(10));
        assertFalse(sharedBuffer.isFinished());

        // destroy buffer
        sharedBuffer.destroy();
        assertFinished(sharedBuffer);

        // set no more queues to assure that we don't get an exception or such
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS.withNoMoreBufferIds());
        assertFinished(sharedBuffer);

        // set no more queues a second time to assure that we don't get an exception or such
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS.withNoMoreBufferIds());
        assertFinished(sharedBuffer);

        // add queue calls after finish should be ignored
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(FOO, new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds());
    }

    @Test
    public void testGetBeforeCreate()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(10));
        assertFalse(sharedBuffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = sharedBuffer.get(FIRST, (long) 0, sizeOfPages(1));
        assertFalse(future.isDone());

        // add a page and verify the future is not complete
        addPage(sharedBuffer, createPage(33));
        assertFalse(future.isDone());

        // add the buffer and verify the future completed
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(FIRST, new UnpartitionedPagePartitionFunction()));
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(33)));
    }

    @Test
    public void testAbortBeforeCreate()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(10));
        assertFalse(sharedBuffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = sharedBuffer.get(FIRST, (long) 0, sizeOfPages(1));
        assertFalse(future.isDone());

        // abort that buffer
        sharedBuffer.abort(FIRST);

        // add a page and verify the future is not complete
        addPage(sharedBuffer, createPage(33));
        assertFalse(future.isDone());

        // add the buffer and verify we did not get the page
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(FIRST, new UnpartitionedPagePartitionFunction()));
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(0, true));

        // verify that a normal read returns a closed empty result
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(0, true));
    }

    @Test
    public void testAddStateMachine()
            throws Exception
    {
        // add after finish
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(10));
        sharedBuffer.setNoMorePages();
        addPage(sharedBuffer, createPage(0));
        addPage(sharedBuffer, createPage(0));
        assertEquals(sharedBuffer.getInfo().getPagesAdded(), 0);

        // add after destroy
        sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(10));
        sharedBuffer.destroy();
        addPage(sharedBuffer, createPage(0));
        addPage(sharedBuffer, createPage(0));
        assertEquals(sharedBuffer.getInfo().getPagesAdded(), 0);
    }

    @Test
    public void testAbort()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 10; i++) {
            addPage(sharedBuffer, createPage(i));
        }
        sharedBuffer.setNoMorePages();

        OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS;
        outputBuffers = outputBuffers.withBuffer(FIRST, new UnpartitionedPagePartitionFunction());
        sharedBuffer.setOutputBuffers(outputBuffers);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        sharedBuffer.abort(FIRST);
        assertQueueClosed(sharedBuffer, FIRST, 0);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 1, sizeOfPages(1), NO_WAIT), emptyResults(1, true));

        outputBuffers = outputBuffers.withBuffer(SECOND, new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds();
        sharedBuffer.setOutputBuffers(outputBuffers);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, SECOND, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        sharedBuffer.abort(SECOND);
        assertQueueClosed(sharedBuffer, SECOND, 0);
        assertFinished(sharedBuffer);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, SECOND, 1, sizeOfPages(1), NO_WAIT), emptyResults(0, true));
    }

    @Test
    public void testFinishClosesEmptyQueues()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(10));
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS
                .withBuffer(FIRST, new UnpartitionedPagePartitionFunction())
                .withBuffer(SECOND, new UnpartitionedPagePartitionFunction()));

        // finish while queues are empty
        sharedBuffer.setNoMorePages();

        assertQueueClosed(sharedBuffer, FIRST, 0);
        assertQueueClosed(sharedBuffer, SECOND, 0);
    }

    @Test
    public void testAbortFreesReader()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(5));
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(QUEUE, new UnpartitionedPagePartitionFunction()));
        assertFalse(sharedBuffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = sharedBuffer.get(QUEUE, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(sharedBuffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = sharedBuffer.get(QUEUE, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // abort the buffer
        sharedBuffer.abort(QUEUE);
        assertQueueClosed(sharedBuffer, QUEUE, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(1, true));
    }

    @Test
    public void testFinishFreesReader()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(5));
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer(QUEUE, new UnpartitionedPagePartitionFunction()));
        assertFalse(sharedBuffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = sharedBuffer.get(QUEUE, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(sharedBuffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = sharedBuffer.get(QUEUE, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // finish the buffer
        sharedBuffer.setNoMorePages();
        assertQueueClosed(sharedBuffer, QUEUE, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(1, true));
    }

    @Test
    public void testFinishFreesWriter()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(5));
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS
                .withBuffer(QUEUE, new UnpartitionedPagePartitionFunction())
                .withNoMoreBufferIds());
        assertFalse(sharedBuffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(sharedBuffer, createPage(i));
        }

        // enqueue the addition two pages more pages
        ListenableFuture<?> firstEnqueuePage = enqueuePage(sharedBuffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(sharedBuffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, QUEUE, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        sharedBuffer.get(QUEUE, 1, sizeOfPages(1)).cancel(true);

        // verify the first blocked page was accepted but the second one was not
        assertTrue(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // finish the query
        sharedBuffer.setNoMorePages();
        assertFalse(sharedBuffer.isFinished());

        // verify second future was completed
        assertTrue(secondEnqueuePage.isDone());

        // get the last 5 page (page 6 was never accepted)
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, QUEUE, 1, sizeOfPages(100), NO_WAIT),
                bufferResult(1, createPage(1), createPage(2), createPage(3), createPage(4), createPage(5)));
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, QUEUE, 6, sizeOfPages(100), NO_WAIT), emptyResults(6, true));

        // verify finished
        assertFinished(sharedBuffer);
    }

    @Test
    public void testDestroyFreesReader()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(5));
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS
                .withBuffer(QUEUE, new UnpartitionedPagePartitionFunction())
                .withNoMoreBufferIds());
        assertFalse(sharedBuffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = sharedBuffer.get(QUEUE, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one page
        addPage(sharedBuffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = sharedBuffer.get(QUEUE, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // destroy the buffer
        sharedBuffer.destroy();
        assertQueueClosed(sharedBuffer, QUEUE, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(1, true));
    }

    @Test
    public void testDestroyFreesWriter()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(TASK_ID, stateNotificationExecutor, sizeOfPages(5));
        sharedBuffer.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS
                .withBuffer(QUEUE, new UnpartitionedPagePartitionFunction())
                .withNoMoreBufferIds());
        assertFalse(sharedBuffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(sharedBuffer, createPage(i));
        }

        // enqueue the addition two pages more pages
        ListenableFuture<?> firstEnqueuePage = enqueuePage(sharedBuffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(sharedBuffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, QUEUE, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        sharedBuffer.get(QUEUE, 1, sizeOfPages(1)).cancel(true);

        // verify the first blocked page was accepted but the second one was not
        assertTrue(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // destroy the buffer (i.e., cancel the query)
        sharedBuffer.destroy();
        assertFinished(sharedBuffer);

        // verify the second future was completed
        assertTrue(secondEnqueuePage.isDone());
    }

    private static ListenableFuture<?> enqueuePage(SharedBuffer sharedBuffer, Page page)
    {
        ListenableFuture<?> future = sharedBuffer.enqueue(page);
        assertFalse(future.isDone());
        return future;
    }

    private static void addPage(SharedBuffer sharedBuffer, Page page)
    {
        assertTrue(sharedBuffer.enqueue(page).isDone());
    }

    private static void assertQueueState(SharedBuffer sharedBuffer, TaskId queueId, int size, int pagesSent)
    {
        assertEquals(getBufferInfo(sharedBuffer, queueId), new BufferInfo(queueId, false, size, pagesSent));
    }

    private static void assertQueueClosed(SharedBuffer sharedBuffer, TaskId queueId, int pagesSent)
    {
        assertEquals(getBufferInfo(sharedBuffer, queueId), new BufferInfo(queueId, true, 0, pagesSent));
    }

    private static BufferInfo getBufferInfo(SharedBuffer sharedBuffer, TaskId queueId)
    {
        for (BufferInfo bufferInfo : sharedBuffer.getInfo().getBuffers()) {
            if (bufferInfo.getBufferId().equals(queueId)) {
                return bufferInfo;
            }
        }
        return null;
    }

    private static void assertFinished(SharedBuffer sharedBuffer)
            throws Exception
    {
        assertTrue(sharedBuffer.isFinished());
        for (BufferInfo bufferInfo : sharedBuffer.getInfo().getBuffers()) {
            assertTrue(bufferInfo.isFinished());
            assertEquals(bufferInfo.getBufferedPages(), 0);
        }
    }

//<<<<<<< HEAD
    private static void assertBufferResultEquals(List<? extends Type> types, BufferResult actual, BufferResult expected)
//=======
//    private static void assertBufferResultEquals(BufferResult actual, BufferResult expected)
//>>>>>>> Use async http responses for task communication
    {
        assertEquals(actual.getPages().size(), expected.getPages().size());
        assertEquals(actual.getToken(), expected.getToken());
        for (int i = 0; i < actual.getPages().size(); i++) {
            Page actualPage = actual.getPages().get(i);
            Page expectedPage = expected.getPages().get(i);
            assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
            PageAssertions.assertPageEquals(types, actualPage, expectedPage);
        }
        assertEquals(actual.isBufferClosed(), expected.isBufferClosed());
    }

    public static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return new BufferResult(token, token + pages.size(), false, pages);
    }
}
