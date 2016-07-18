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
import com.facebook.presto.execution.TaskId;
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
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.OutputBuffers.BufferType.BROADCAST;
import static com.facebook.presto.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSharedOutputBuffer
{
    private static final Duration NO_WAIT = new Duration(0, TimeUnit.MILLISECONDS);
    private static final Duration MAX_WAIT = new Duration(1, TimeUnit.SECONDS);
    private static final DataSize PAGE_SIZE = new DataSize(createPage(42).getSizeInBytes(), BYTE);
    private static final DataSize RETAINED_PAGE_SIZE = new DataSize(createPage(42).getRetainedSizeInBytes(), BYTE);
    private static final TaskId TASK_ID = new TaskId("query", "stage", 0);
    private static final int DEFAULT_PARTITION = 0;
    private static final String TASK_INSTANCE_ID = "task-instance-id";

    private static final ImmutableList<BigintType> TYPES = ImmutableList.of(BIGINT);
    public static final OutputBufferId FIRST = new OutputBufferId(0);
    public static final OutputBufferId SECOND = new OutputBufferId(1);
    public static final OutputBufferId QUEUE = new OutputBufferId(0);
    public static final OutputBufferId FOO = new OutputBufferId(1);

    private static Page createPage(int i)
    {
        return new Page(BlockAssertions.createLongsBlock(i));
    }

    public static DataSize sizeOfPages(int count)
    {
        return new DataSize(PAGE_SIZE.toBytes() * count, BYTE);
    }

    public static DataSize retainedSizeOfPages(int count)
    {
        return new DataSize(RETAINED_PAGE_SIZE.toBytes() * count, BYTE);
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
            new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, new DataSize(0, BYTE));
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testSimple()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(sharedBuffer, createPage(i));
        }

        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(BROADCAST).withBuffer(FIRST, 0);

        // add a queue
        sharedBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 3, 0, 3, 3);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 3, 0, 3, 3);

        // acknowledge first three pages
        sharedBuffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);
        // pages now acknowledged
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 0, 3, 3, 3);

        // fill the buffer (we already added 3 pages)
        for (int i = 3; i < 10; i++) {
            addPage(sharedBuffer, createPage(i));
        }
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 7, 3, 10, 10);

        // try to add one more page, which should block
        ListenableFuture<?> future = enqueuePage(sharedBuffer, createPage(10));
        assertFalse(future.isDone());
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 8, 3, 11, 11);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 3, sizeOfPages(1), NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 8, 3, 11, 11);

        // we should still be blocked
        assertFalse(future.isDone());

        //
        // add another buffer and verify it sees all pages
        outputBuffers = outputBuffers.withBuffer(SECOND, 0);
        sharedBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(sharedBuffer, SECOND, DEFAULT_PARTITION, 11, 0, 11, 11);
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
        assertQueueState(sharedBuffer, SECOND, DEFAULT_PARTITION, 11, 0, 11, 11);
        // acknowledge the 10 pages
        sharedBuffer.get(SECOND, 10, sizeOfPages(10)).cancel(true);
        assertQueueState(sharedBuffer, SECOND, DEFAULT_PARTITION, 1, 10, 11, 11);

        //
        // tell shared buffer there will be no more queues
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        sharedBuffer.setOutputBuffers(outputBuffers);

        // queues consumed the first three pages, so they should be dropped now and the blocked page future from above should be done
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 8, 3, 8, 11);
        assertQueueState(sharedBuffer, SECOND, DEFAULT_PARTITION, 1, 10, 8, 11);
        assertFutureIsDone(future);

        // we should be able to add 3 more pages (the third will be queued)
        // although the first queue fetched the 4th page, the page has not been acknowledged yet
        addPage(sharedBuffer, createPage(11));
        addPage(sharedBuffer, createPage(12));
        future = enqueuePage(sharedBuffer, createPage(13));
        assertFalse(future.isDone());
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 11, 3, 11, 14);
        assertQueueState(sharedBuffer, SECOND, DEFAULT_PARTITION, 4, 10, 11, 14);

        // acknowledge the receipt of the 3rd page and try to remove the 4th page from the first queue
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 4, sizeOfPages(1), NO_WAIT), bufferResult(4, createPage(4)));

        // the blocked page future above should be done
        assertFutureIsDone(future);
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 10, 4, 10, 14);
        assertQueueState(sharedBuffer, SECOND, DEFAULT_PARTITION, 4, 10, 10, 14);

        //
        // finish the buffer
        assertFalse(sharedBuffer.isFinished());
        sharedBuffer.setNoMorePages();
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 10, 4, 10, 14);
        assertQueueState(sharedBuffer, SECOND, DEFAULT_PARTITION, 4, 10, 10, 14);

        // not fully finished until all pages are consumed
        assertFalse(sharedBuffer.isFinished());

        // remove a page, not finished
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 5, sizeOfPages(1), NO_WAIT), bufferResult(5, createPage(5)));
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 9, 5, 9, 14);
        assertQueueState(sharedBuffer, SECOND, DEFAULT_PARTITION, 4, 10, 9, 14);
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
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 8, 6, 8, 14);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));

        // finish first queue
        sharedBuffer.abort(FIRST);
        assertQueueClosed(sharedBuffer, FIRST, 14);
        assertQueueState(sharedBuffer, SECOND, DEFAULT_PARTITION, 4, 10, 4, 14);
        assertFalse(sharedBuffer.isFinished());

        // remove all remaining pages from second queue, should be finished
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, SECOND, 10, sizeOfPages(10), NO_WAIT), bufferResult(10, createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(sharedBuffer, SECOND, DEFAULT_PARTITION, 4, 10, 4, 14);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, SECOND, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
        sharedBuffer.abort(SECOND);
        assertQueueClosed(sharedBuffer, FIRST, 14);
        assertQueueClosed(sharedBuffer, SECOND, 14);
        assertFinished(sharedBuffer);

        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, SECOND, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
    }

    @Test
    public void testSharedBufferFull()
            throws Exception
    {
        int firstPartition = 0;
        int secondPartition = 1;
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(2));

        // Add two pages, buffer is full
        addPage(sharedBuffer, createPage(1), firstPartition);
        addPage(sharedBuffer, createPage(2), secondPartition);

        // third page is blocked
        enqueuePage(sharedBuffer, createPage(3), secondPartition);
    }

    @Test
    public void testDeqeueueOnAcknowledgement()
            throws Exception
    {
        int firstPartition = 0;
        int secondPartition = 1;
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(2));
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(BROADCAST)
                .withBuffer(FIRST, firstPartition)
                .withBuffer(SECOND, secondPartition)
                .withNoMoreBufferIds();
        sharedBuffer.setOutputBuffers(outputBuffers);

        // Add two pages, buffer is full
        addPage(sharedBuffer, createPage(1), firstPartition);
        addPage(sharedBuffer, createPage(2), firstPartition);
        assertQueueState(sharedBuffer, FIRST, firstPartition, 2, 0, 2, 2);

        // third page is blocked
        ListenableFuture<?> future = enqueuePage(sharedBuffer, createPage(3), secondPartition);

        // we should be blocked
        assertFalse(future.isDone());
        assertQueueState(sharedBuffer, FIRST, firstPartition, 2, 0, 2, 2);   // 2 buffered pages
        assertQueueState(sharedBuffer, SECOND, secondPartition, 1, 0, 1, 1); // 1 queued page

        // acknowledge pages for first partition, make space in the shared buffer
        sharedBuffer.get(FIRST, 2, sizeOfPages(10)).cancel(true);

        // page should be dequeued, we should not be blocked
        assertFutureIsDone(future);
        assertQueueState(sharedBuffer, SECOND, secondPartition, 1, 0, 1, 1); // no more queued pages
    }

    @Test
    public void testSimplePartitioned()
            throws Exception
    {
        int firstPartition = 0;
        int secondPartition = 1;
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(20));

        // add three items to each buffer
        for (int i = 0; i < 3; i++) {
            addPage(sharedBuffer, createPage(i), firstPartition);
            addPage(sharedBuffer, createPage(i), secondPartition);
        }

        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(FIRST, firstPartition);

        // add first partition
        sharedBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(sharedBuffer, FIRST, firstPartition, 3, 0, 3, 3);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, FIRST, firstPartition, 3, 0, 3, 3);

        // acknowledge first three pages
        sharedBuffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);
        // pages now acknowledged
        assertQueueState(sharedBuffer, FIRST, firstPartition, 0, 3, 3, 3);

        // fill the buffer (we already added 3 pages)
        for (int i = 3; i < 10; i++) {
            addPage(sharedBuffer, createPage(i), firstPartition);
            addPage(sharedBuffer, createPage(i), secondPartition);
        }
        assertQueueState(sharedBuffer, FIRST, firstPartition, 7, 3, 10, 10);

        // try to add one more page, which should block
        ListenableFuture<?> future = enqueuePage(sharedBuffer, createPage(10), firstPartition);
        assertFalse(future.isDone());
        assertQueueState(sharedBuffer, FIRST, firstPartition, 8, 3, 11, 11);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 3, sizeOfPages(1), NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertQueueState(sharedBuffer, FIRST, firstPartition, 8, 3, 11, 11);

        // we should still be blocked
        assertFalse(future.isDone());

        //
        // add second partition and verify it sees all pages
        outputBuffers = outputBuffers.withBuffer(SECOND, secondPartition);
        sharedBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(sharedBuffer, SECOND, secondPartition, 10, 0, 10, 10);
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
        assertQueueState(sharedBuffer, SECOND, secondPartition, 10, 0, 10, 10);
        // acknowledge the 10 pages
        sharedBuffer.get(SECOND, 10, sizeOfPages(10)).cancel(true);
        assertQueueState(sharedBuffer, SECOND, secondPartition, 0, 10, 10, 10);

        //
        // tell shared buffer there will be no more queues
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        sharedBuffer.setOutputBuffers(outputBuffers);

        // since both queues consumed some pages, the blocked page future from above should be done
        assertFutureIsDone(future);
        assertQueueState(sharedBuffer, FIRST, firstPartition, 8, 3, 8, 11);
        assertQueueState(sharedBuffer, SECOND, secondPartition, 0, 10, 0, 10);

        // we should be able to add 3 more pages
        // although the first queue fetched the 4th page, the page has not been acknowledged yet
        addPage(sharedBuffer, createPage(11), firstPartition);
        addPage(sharedBuffer, createPage(12), firstPartition);
        addPage(sharedBuffer, createPage(13), firstPartition);
        assertQueueState(sharedBuffer, FIRST, firstPartition, 11, 3, 11, 14);
        assertQueueState(sharedBuffer, SECOND, secondPartition, 0, 10, 0, 10);

        // remove a page from the first queue
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 4, sizeOfPages(1), NO_WAIT), bufferResult(4, createPage(4)));

        // the blocked page future above should be done
        assertFutureIsDone(future);
        assertQueueState(sharedBuffer, FIRST, firstPartition, 10, 4, 10, 14);
        assertQueueState(sharedBuffer, SECOND, secondPartition, 0, 10, 0, 10);

        //
        // finish the buffer
        assertFalse(sharedBuffer.isFinished());
        sharedBuffer.setNoMorePages();
        assertQueueState(sharedBuffer, FIRST, firstPartition, 10, 4, 10, 14);
        assertQueueState(sharedBuffer, SECOND, secondPartition, 0, 10, 0, 10);
        sharedBuffer.abort(SECOND);
        assertQueueClosed(sharedBuffer, SECOND, 10);

        // not fully finished until all pages are consumed
        assertFalse(sharedBuffer.isFinished());

        // remove a page, not finished
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 5, sizeOfPages(1), NO_WAIT), bufferResult(5, createPage(5)));
        assertQueueState(sharedBuffer, FIRST, firstPartition, 9, 5, 9, 14);
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
        assertQueueState(sharedBuffer, FIRST, firstPartition, 8, 6, 8, 14);
        // acknowledge all pages from the first partition, should transition to finished state
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
        sharedBuffer.abort(FIRST);
        assertQueueClosed(sharedBuffer, FIRST, 14);
        assertFinished(sharedBuffer);
    }

    public static BufferResult getBufferResult(SharedOutputBuffer sharedBuffer, OutputBufferId bufferId, long sequenceId, DataSize maxSize, Duration maxWait)
    {
        CompletableFuture<BufferResult> future = sharedBuffer.get(bufferId, sequenceId, maxSize);
        return getFuture(future, maxWait);
    }

    public static BufferResult getFuture(CompletableFuture<BufferResult> future, Duration maxWait)
    {
        return tryGetFutureValue(future, (int) maxWait.toMillis(), TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void testDuplicateRequests()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(10));
        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(sharedBuffer, createPage(i));
        }

        // add a queue
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(BROADCAST);
        outputBuffers = outputBuffers.withBuffer(FIRST, 0);
        sharedBuffer.setOutputBuffers(outputBuffers);
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 3, 0, 3, 3);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 3, 0, 3, 3);

        // get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 3, 0, 3, 3);

        // acknowledge the pages
        sharedBuffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);

        // attempt to get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, false));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, FIRST, DEFAULT_PARTITION, 0, 3, 3, 3);
    }

    @Test
    public void testAddQueueAfterNoMoreQueues()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(10));
        assertFalse(sharedBuffer.isFinished());

        // tell buffer no more queues will be added
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withNoMoreBufferIds());
        assertFalse(sharedBuffer.isFinished());

        // set no more queues a second time to assure that we don't get an exception or such
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withNoMoreBufferIds());
        assertFalse(sharedBuffer.isFinished());

        // set no more queues a third time to assure that we don't get an exception or such
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withNoMoreBufferIds());
        assertFalse(sharedBuffer.isFinished());

        try {
            OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(BROADCAST)
                    .withBuffer(FOO, 0)
                    .withNoMoreBufferIds();

            sharedBuffer.setOutputBuffers(outputBuffers);
            fail("Expected IllegalStateException from addQueue after noMoreQueues has been called");
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testAddQueueAfterDestroy()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(10));
        assertFalse(sharedBuffer.isFinished());

        // destroy buffer
        sharedBuffer.destroy();
        assertFinished(sharedBuffer);

        // set no more queues to assure that we don't get an exception or such
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withNoMoreBufferIds());
        assertFinished(sharedBuffer);

        // set no more queues a second time to assure that we don't get an exception or such
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withNoMoreBufferIds());
        assertFinished(sharedBuffer);

        // add queue calls after finish should be ignored
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withBuffer(FOO, 0).withNoMoreBufferIds());
    }

    @Test
    public void testGetBeforeCreate()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(10));
        assertFalse(sharedBuffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        CompletableFuture<BufferResult> future = sharedBuffer.get(FIRST, (long) 0, sizeOfPages(1));
        assertFalse(future.isDone());

        // add a page and verify the future is not complete
        addPage(sharedBuffer, createPage(33));
        assertFalse(future.isDone());

        // add the buffer and verify the future completed
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withBuffer(FIRST, 0));
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(33)));
    }

    @Test
    public void testAbortBeforeCreate()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(10));
        assertFalse(sharedBuffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        CompletableFuture<BufferResult> future = sharedBuffer.get(FIRST, (long) 0, sizeOfPages(1));
        assertFalse(future.isDone());

        // abort that buffer
        sharedBuffer.abort(FIRST);

        // add a page and verify the future is not complete
        addPage(sharedBuffer, createPage(33));
        assertFalse(future.isDone());

        // add the buffer and verify we did not get the page
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withBuffer(FIRST, 0));
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));

        // verify that a normal read returns a closed empty result
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testAddStateMachine()
            throws Exception
    {
        // add after finish
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(10));
        sharedBuffer.setNoMorePages();
        addPage(sharedBuffer, createPage(0));
        addPage(sharedBuffer, createPage(0));
        assertEquals(sharedBuffer.getInfo().getTotalPagesSent(), 0);

        // add after destroy
        sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(10));
        sharedBuffer.destroy();
        addPage(sharedBuffer, createPage(0));
        addPage(sharedBuffer, createPage(0));
        assertEquals(sharedBuffer.getInfo().getTotalPagesSent(), 0);
    }

    @Test
    public void testAbort()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 10; i++) {
            addPage(sharedBuffer, createPage(i));
        }
        sharedBuffer.setNoMorePages();

        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(BROADCAST)
                .withBuffer(FIRST, 0);
        sharedBuffer.setOutputBuffers(outputBuffers);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        sharedBuffer.abort(FIRST);
        assertQueueClosed(sharedBuffer, FIRST, 0);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, FIRST, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));

        outputBuffers = outputBuffers.withBuffer(SECOND, 0).withNoMoreBufferIds();
        sharedBuffer.setOutputBuffers(outputBuffers);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, SECOND, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        sharedBuffer.abort(SECOND);
        assertQueueClosed(sharedBuffer, SECOND, 0);
        assertFinished(sharedBuffer);
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, SECOND, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFinishClosesEmptyQueues()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(10));
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST)
                .withBuffer(FIRST, 0)
                .withBuffer(SECOND, 0));

        // finish while queues are empty
        sharedBuffer.setNoMorePages();

        assertQueueState(sharedBuffer, FIRST, 0, 0, 0, 0, 0);
        assertQueueState(sharedBuffer, SECOND, 0, 0, 0, 0, 0);

        sharedBuffer.abort(FIRST);
        sharedBuffer.abort(SECOND);

        assertQueueClosed(sharedBuffer, FIRST, 0);
        assertQueueClosed(sharedBuffer, SECOND, 0);
    }

    @Test
    public void testAbortFreesReader()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(5));
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withBuffer(QUEUE, 0));
        assertFalse(sharedBuffer.isFinished());

        // attempt to get a page
        CompletableFuture<BufferResult> future = sharedBuffer.get(QUEUE, 0, sizeOfPages(10));

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
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));
    }

    @Test
    public void testFinishFreesReader()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(5));
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST).withBuffer(QUEUE, 0));
        assertFalse(sharedBuffer.isFinished());

        // attempt to get a page
        CompletableFuture<BufferResult> future = sharedBuffer.get(QUEUE, 0, sizeOfPages(10));

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
        assertQueueState(sharedBuffer, QUEUE, 0, 0, 1, 1, 1);
        sharedBuffer.abort(QUEUE);
        assertQueueClosed(sharedBuffer, QUEUE, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));
    }

    @Test
    public void testFinishFreesWriter()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(5));
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST)
                .withBuffer(QUEUE, 0)
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

        // verify we are still blocked because the buffer is full
        assertFalse(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // finish the query
        sharedBuffer.setNoMorePages();
        assertFalse(sharedBuffer.isFinished());

        // verify futures are complete
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);

        // get and acknowledge the last 6 pages
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, QUEUE, 1, sizeOfPages(100), NO_WAIT),
                bufferResult(1, createPage(1), createPage(2), createPage(3), createPage(4), createPage(5), createPage(6)));
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, QUEUE, 7, sizeOfPages(100), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 7, true));

        sharedBuffer.abort(QUEUE);

        // verify finished
        assertFinished(sharedBuffer);
    }

    @Test
    public void testDestroyFreesReader()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(5));
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST)
                .withBuffer(QUEUE, 0)
                .withNoMoreBufferIds());
        assertFalse(sharedBuffer.isFinished());

        // attempt to get a page
        CompletableFuture<BufferResult> future = sharedBuffer.get(QUEUE, 0, sizeOfPages(10));

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
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));
    }

    @Test
    public void testDestroyFreesWriter()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(5));
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST)
                .withBuffer(QUEUE, 0)
                .withNoMoreBufferIds());
        assertFalse(sharedBuffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(sharedBuffer, createPage(i));
        }

        // add two pages to the buffer queue
        ListenableFuture<?> firstEnqueuePage = enqueuePage(sharedBuffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(sharedBuffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, QUEUE, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        sharedBuffer.get(QUEUE, 1, sizeOfPages(1)).cancel(true);

        // verify we are still blocked because the buffer is full
        assertFalse(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // destroy the buffer (i.e., cancel the query)
        sharedBuffer.destroy();
        assertFinished(sharedBuffer);

        // verify the futures are completed
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);
    }

    @Test
    public void testBufferCompletion()
            throws Exception
    {
        SharedOutputBuffer sharedBuffer = new SharedOutputBuffer(TASK_ID, TASK_INSTANCE_ID, stateNotificationExecutor, retainedSizeOfPages(5));
        sharedBuffer.setOutputBuffers(createInitialEmptyOutputBuffers(BROADCAST)
                .withBuffer(QUEUE, 0)
                .withNoMoreBufferIds());

        assertFalse(sharedBuffer.isFinished());

        // fill the buffer
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Page page = createPage(i);
            addPage(sharedBuffer, page);
            pages.add(page);
        }

        sharedBuffer.setNoMorePages();

        // get and acknowledge 5 pages
        assertBufferResultEquals(TYPES, getBufferResult(sharedBuffer, QUEUE, 0, sizeOfPages(5), MAX_WAIT), bufferResult(0, pages));

        // buffer is not finished
        assertFalse(sharedBuffer.isFinished());

        // there are no more pages and no more buffers, but buffer is not finished because it didn't receive an acknowledgement yet
        assertFalse(sharedBuffer.isFinished());

        // ask the buffer to finish
        sharedBuffer.abort(QUEUE);

        // verify that the buffer is finished
        assertTrue(sharedBuffer.isFinished());
    }

    private static ListenableFuture<?> enqueuePage(SharedOutputBuffer sharedBuffer, Page page)
    {
        return enqueuePage(sharedBuffer, page, DEFAULT_PARTITION);
    }

    private static ListenableFuture<?> enqueuePage(SharedOutputBuffer sharedBuffer, Page page, int partition)
    {
        ListenableFuture<?> future = sharedBuffer.enqueue(partition, page);
        assertFalse(future.isDone());
        return future;
    }

    private static void addPage(SharedOutputBuffer sharedBuffer, Page page)
    {
        addPage(sharedBuffer, page, DEFAULT_PARTITION);
    }

    private static void addPage(SharedOutputBuffer sharedBuffer, Page page, int partition)
    {
        assertTrue(sharedBuffer.enqueue(partition, page).isDone());
    }

    private static void assertQueueState(
            SharedOutputBuffer sharedBuffer,
            OutputBufferId bufferId,
            int partition,
            int bufferedPages,
            int pagesSent,
            int pageBufferBufferedPages,
            int pageBufferPagesSent)
    {
        assertEquals(
                getBufferInfo(sharedBuffer, bufferId),
                new BufferInfo(
                        bufferId,
                        false,
                        bufferedPages,
                        pagesSent,
                        new PageBufferInfo(
                                partition,
                                pageBufferBufferedPages,
                                retainedSizeOfPages(pageBufferBufferedPages).toBytes(),
                                pageBufferPagesSent, // every page has one row
                                pageBufferPagesSent)));
    }

    @SuppressWarnings("ConstantConditions")
    private static void assertQueueClosed(SharedOutputBuffer sharedBuffer, OutputBufferId bufferId, int pagesSent)
    {
        BufferInfo bufferInfo = getBufferInfo(sharedBuffer, bufferId);
        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getPagesSent(), pagesSent);
        assertEquals(bufferInfo.isFinished(), true);
    }

    private static BufferInfo getBufferInfo(SharedOutputBuffer sharedBuffer, OutputBufferId bufferId)
    {
        for (BufferInfo bufferInfo : sharedBuffer.getInfo().getBuffers()) {
            if (bufferInfo.getBufferId().equals(bufferId)) {
                return bufferInfo;
            }
        }
        return null;
    }

    private static void assertFinished(SharedOutputBuffer sharedBuffer)
            throws Exception
    {
        assertTrue(sharedBuffer.isFinished());
        for (BufferInfo bufferInfo : sharedBuffer.getInfo().getBuffers()) {
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
}
