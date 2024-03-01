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
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
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
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPartitionedOutputBuffer
{
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

    // TODO: remove this after PR #7987 is landed
    @Test
    public void testAcknowledge()
    {
        int partitionId = 0;
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, partitionId)
                        .withNoMoreBufferIds(),
                sizeOfPages(20));

        // add three items to the buffer
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i), partitionId);
        }
        assertQueueState(buffer, FIRST, 3, 0);

        // get the three elements from the first buffer
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
            addPage(buffer, createPage(i), partitionId);
        }
        assertQueueState(buffer, FIRST, 3, 3);

        // getting new pages will again acknowledge the previously acknowledged pages but this is ok
        buffer.get(FIRST, 3, sizeOfPages(1)).cancel(true);
        assertQueueState(buffer, FIRST, 3, 3);
    }

    @Test
    public void testDuplicateRequests()
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
    {
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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
        // partitioned buffer does not return a "complete" result in this case, but it doesn't matter
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));

        // further requests will see a completed result
        assertQueueClosed(buffer, FIRST, 1);
    }

    @Test
    public void testFinishFreesReader()
    {
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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
    {
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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
    {
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
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
    public void testBufferFinishesWhenClientBuffersDestroyed()
    {
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
                        .withBuffer(SECOND, 1)
                        .withBuffer(THIRD, 2)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));

        // add a page to each partition before closing the buffers to make sure
        // that the buffers close even if there are pending pages
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i), i);
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
    public void testBufferPeakMemoryUsage()
    {
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        Page page = createPage(1);
        long serializePageSize = PAGES_SERDE.serialize(page).getRetainedSizeInBytes();
        for (int i = 0; i < 5; i++) {
            addPage(buffer, page, 0);
            assertEquals(buffer.getPeakMemoryUsage(), (i + 1) * serializePageSize);
        }
    }

    @Test
    public void testForceFreeMemory()
            throws Throwable
    {
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(FIRST, 0)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        for (int i = 0; i < 5; i++) {
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

    private PartitionedOutputBuffer createPartitionedBuffer(OutputBuffers buffers, DataSize dataSize)
    {
        PartitionedOutputBuffer buffer = new PartitionedOutputBuffer(
                TASK_INSTANCE_ID,
                new StateMachine<>("bufferState", stateNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES),
                buffers,
                dataSize,
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                stateNotificationExecutor);
        buffer.registerLifespanCompletionCallback(ignore -> {});
        return buffer;
    }

    private static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return createBufferResult(TASK_INSTANCE_ID, token, pages);
    }
}
