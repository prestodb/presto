/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.operator.Page;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.execution.BufferResult.bufferResult;
import static com.facebook.presto.execution.BufferResult.emptyResults;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSharedBuffer
{
    private static final Duration NO_WAIT = new Duration(0, TimeUnit.MILLISECONDS);
    private static final Duration MAX_WAIT = new Duration(1, TimeUnit.SECONDS);
    private static final DataSize PAGE_SIZE = createPage(42).getDataSize();

    private static Page createPage(int i)
    {
        return new Page(BlockAssertions.createLongsBlock(i));
    }

    public static DataSize sizeOfPages(int count)
    {
        return new DataSize(PAGE_SIZE.toBytes() * count, Unit.BYTE);
    }

    @Test
    public void testInvalidConstructorArg()
            throws Exception
    {
        try {
            new SharedBuffer(new DataSize(0, Unit.BYTE));
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testSimple()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            assertTrue(sharedBuffer.offer(createPage(i)));
        }

        // add a queue
        sharedBuffer.addQueue("first");
        assertQueueState(sharedBuffer, "first", 3, 0);

        // get the three elements
        assertBufferResultEquals(sharedBuffer.get("first", 0, 10, NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, "first", 3, 0);

        // try to get some more pages (acknowledge first three pages)
        assertBufferResultEquals(sharedBuffer.get("first", 3, 10, NO_WAIT), emptyResults(3, false));
        // pages now acknowledged
        assertQueueState(sharedBuffer, "first", 0, 3);

        // fill the buffer (we already added 3 pages)
        for (int i = 3; i < 10; i++) {
            assertTrue(sharedBuffer.offer(createPage(i)));
        }
        assertQueueState(sharedBuffer, "first", 7, 3);

        // try to add one more page, which should fail
        assertFalse(sharedBuffer.offer(createPage(99)));

        // remove a page
        assertBufferResultEquals(sharedBuffer.get("first", 3, 1, NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, "first", 7, 3);

        // try to add one more page, which should fail
        assertFalse(sharedBuffer.offer(createPage(99)));

        //
        // add another buffer and verify it sees all pages
        sharedBuffer.addQueue("second");
        assertQueueState(sharedBuffer, "second", 10, 0);
        assertBufferResultEquals(sharedBuffer.get("second", 0, 10, NO_WAIT), bufferResult(0, createPage(0),
                createPage(1),
                createPage(2),
                createPage(3),
                createPage(4),
                createPage(5),
                createPage(6),
                createPage(7),
                createPage(8),
                createPage(9)));
        // page not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, "second", 10, 0);
         // acknowledge the 10 pages
        assertBufferResultEquals(sharedBuffer.get("second", 10, 10, NO_WAIT), emptyResults(10, false));
        assertQueueState(sharedBuffer, "second", 0, 10);

        //
        // tell shared buffer there will be no more queues
        sharedBuffer.noMoreQueues();

        // since both queues consumed the first three pages, we should be able to add 3 more pages
        // although the first queue fetched the 4th page, the page has not been acknowledged yet
        for (int i = 10; i < 13; i++) {
            assertTrue(sharedBuffer.offer(createPage(i)));
        }
        assertFalse(sharedBuffer.offer(createPage(99)));
        assertQueueState(sharedBuffer, "first", 10, 3);
        assertQueueState(sharedBuffer, "second", 3, 10);

        // remove a page from the first queue
        assertBufferResultEquals(sharedBuffer.get("first", 4, 1, NO_WAIT), bufferResult(4, createPage(4)));
        assertQueueState(sharedBuffer, "first", 9, 4);
        assertQueueState(sharedBuffer, "second", 3, 10);

        // try to add one more page, which should work since the first queue is the largest queue
        assertTrue(sharedBuffer.offer(createPage(13)));
        assertFalse(sharedBuffer.offer(createPage(99)));
        assertQueueState(sharedBuffer, "first", 10, 4);
        assertQueueState(sharedBuffer, "second", 4, 10);

        //
        // finish the buffer
        assertFalse(sharedBuffer.isFinished());
        sharedBuffer.finish();
        assertQueueState(sharedBuffer, "first", 10, 4);
        assertQueueState(sharedBuffer, "second", 4, 10);

        // not fully finished until all pages are consumed
        assertFalse(sharedBuffer.isFinished());

        // remove a page, not finished
        assertBufferResultEquals(sharedBuffer.get("first", 5, 1, NO_WAIT), bufferResult(5, createPage(5)));
        assertQueueState(sharedBuffer, "first", 9, 5);
        assertQueueState(sharedBuffer, "second", 4, 10);
        assertFalse(sharedBuffer.isFinished());

        // remove all remaining pages from first queue, should not be finished
        BufferResult x = sharedBuffer.get("first", 6, 10, NO_WAIT);
        assertBufferResultEquals(x, bufferResult(6, createPage(6),
                createPage(7),
                createPage(8),
                createPage(9),
                createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(sharedBuffer, "first", 8, 6);
        assertBufferResultEquals(sharedBuffer.get("first", 14, 10, NO_WAIT), emptyResults(14, false));
        assertQueueClosed(sharedBuffer, "first", 14);
        assertQueueState(sharedBuffer, "second", 4, 10);
        assertFalse(sharedBuffer.isFinished());

        // remove all remaining pages from second queue, should be finished
        assertBufferResultEquals(sharedBuffer.get("second", 10, 10, NO_WAIT), bufferResult(10, createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(sharedBuffer, "second", 4, 10);
        assertBufferResultEquals(sharedBuffer.get("second", 14, 10, NO_WAIT), emptyResults(14, false));
        assertQueueClosed(sharedBuffer, "first", 14);
        assertQueueClosed(sharedBuffer, "second", 14);
        assertFinished(sharedBuffer);

        assertBufferResultEquals(sharedBuffer.get("first", 14, 10, NO_WAIT), emptyResults(14, true));
        assertBufferResultEquals(sharedBuffer.get("second", 14, 10, NO_WAIT), emptyResults(14, true));
    }

    @Test
    public void testDuplicateRequests()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            assertTrue(sharedBuffer.offer(createPage(i)));
        }

        // add a queue
        sharedBuffer.addQueue("first");
        assertQueueState(sharedBuffer, "first", 3, 0);

        // get the three elements
        assertBufferResultEquals(sharedBuffer.get("first", 0, 10, NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, "first", 3, 0);

        // get the three elements again
        assertBufferResultEquals(sharedBuffer.get("first", 0, 10, NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, "first", 3, 0);

        // acknowledge the pages
        sharedBuffer.acknowledge("first", 3);

        // attempt to get the three elements again
        assertBufferResultEquals(sharedBuffer.get("first", 0, 10, NO_WAIT), emptyResults(3, false));
        // pages not acknowledged yet so state is the same
        assertQueueState(sharedBuffer, "first", 0, 3);
    }

    @Test
    public void testAddQueueAfterNoMoreQueues()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(10));
        assertFalse(sharedBuffer.isFinished());

        // tell buffer no more queues will be added
        sharedBuffer.noMoreQueues();
        assertFalse(sharedBuffer.isFinished());

        // call no more queues a second time to assure that we don't get an exception or such
        sharedBuffer.noMoreQueues();
        assertFalse(sharedBuffer.isFinished());

        // call no more queues a second time to assure that we don't get an exception or such
        sharedBuffer.noMoreQueues();
        assertFalse(sharedBuffer.isFinished());

        try {
            sharedBuffer.addQueue("foo");
            fail("Expected IllegalStateException from addQueue after noMoreQueues has been called");
        }
        catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testAddQueueAfterDestroy()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(10));
        assertFalse(sharedBuffer.isFinished());

        // destroy buffer
        sharedBuffer.destroy();
        assertFinished(sharedBuffer);

        // call no more queues a second time to assure that we don't get an exception or such
        sharedBuffer.noMoreQueues();
        assertFinished(sharedBuffer);

        // call no more queues a second time to assure that we don't get an exception or such
        sharedBuffer.noMoreQueues();
        assertFinished(sharedBuffer);

        // add queue calls after finish should be ignored
        sharedBuffer.addQueue("foo");
    }

    @Test
    public void testOperationsOnUnknownQueues()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(10));
        assertFalse(sharedBuffer.isFinished());

        // verify operations on unknown queue throw an exception
        try {
            sharedBuffer.get("unknown", 0, 1, NO_WAIT);
            fail("Expected NoSuchBufferException from operation on unknown queue");
        }
        catch (NoSuchBufferException expected) {
        }

        // abort on unknown buffer is allowed
        sharedBuffer.abort("unknown");

        // finish buffer and try operations again
        sharedBuffer.finish();
        try {
            sharedBuffer.get("unknown", 0, 1, NO_WAIT);
            fail("Expected NoSuchBufferException from operation on unknown queue");
        }
        catch (NoSuchBufferException expected) {
        }

        // abort on unknown buffer is allowed
        sharedBuffer.abort("unknown");

        // set no more queues and try operations again
        sharedBuffer.noMoreQueues();
        try {
            sharedBuffer.get("unknown", 0, 1, NO_WAIT);
            fail("Expected NoSuchBufferException from operation on unknown queue");
        }
        catch (NoSuchBufferException expected) {
        }

        // abort on unknown buffer is allowed
        sharedBuffer.abort("unknown");


        // destroy and try operations again
        sharedBuffer.destroy();
        try {
            sharedBuffer.get("unknown", 0, 1, NO_WAIT);
            fail("Expected NoSuchBufferException from operation on unknown queue");
        }
        catch (NoSuchBufferException expected) {
        }

        // abort on unknown buffer is allowed
        sharedBuffer.abort("unknown");
    }

    @Test
    public void testAddStateMachine()
            throws Exception
    {
        // add after finish
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(10));
        sharedBuffer.finish();
        assertFalse(sharedBuffer.offer(createPage(0)));
        assertFalse(sharedBuffer.add(createPage(0)));

        // add after destroy
        sharedBuffer = new SharedBuffer(sizeOfPages(10));
        sharedBuffer.destroy();
        assertFalse(sharedBuffer.offer(createPage(0)));
        assertFalse(sharedBuffer.add(createPage(0)));
    }

    @Test
    public void testAbort()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 10; i++) {
            assertTrue(sharedBuffer.offer(createPage(i)));
        }
        sharedBuffer.finish();

        sharedBuffer.addQueue("first");
        assertBufferResultEquals(sharedBuffer.get("first", 0, 1, NO_WAIT), bufferResult(0, createPage(0)));
        sharedBuffer.abort("first");
        assertQueueClosed(sharedBuffer, "first", 0);
        assertBufferResultEquals(sharedBuffer.get("first", 1, 1, NO_WAIT), emptyResults(1, true));

        sharedBuffer.addQueue("second");
        sharedBuffer.noMoreQueues();
        assertBufferResultEquals(sharedBuffer.get("second", 0, 1, NO_WAIT), bufferResult(0, createPage(0)));
        sharedBuffer.abort("second");
        assertQueueClosed(sharedBuffer, "second", 0);
        assertFinished(sharedBuffer);
        assertBufferResultEquals(sharedBuffer.get("second", 1, 1, NO_WAIT), emptyResults(0, true));
    }

    @Test
    public void testFinishClosesEmptyQueues()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(10));
        sharedBuffer.addQueue("first");
        sharedBuffer.addQueue("second");

        // finish while queues are empty
        sharedBuffer.finish();

        assertQueueClosed(sharedBuffer, "first", 0);
        assertQueueClosed(sharedBuffer, "second", 0);
    }

    @Test
    public void testAbortFreesReader()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(5));
        sharedBuffer.addQueue("queue");
        assertFalse(sharedBuffer.isFinished());

        ExecutorService executor = Executors.newCachedThreadPool();

        // exec thread to get two pages
        GetPagesJob getPagesJob = new GetPagesJob(sharedBuffer, 0, 2, 1);
        executor.submit(getPagesJob);
        getPagesJob.waitForStarted();

        // "verify" thread is blocked
        getPagesJob.assertBlockedWithCount(0);

        // add one page
        assertTrue(sharedBuffer.offer(createPage(0)));

        // verify thread got one page and is blocked
        getPagesJob.assertBlockedWithCount(1);

        // abort the buffer
        sharedBuffer.abort("queue");
        assertQueueClosed(sharedBuffer, "queue", 1);

        // verify thread is released
        getPagesJob.waitForFinished();

        // verify thread only got one page
        assertEquals(getPagesJob.getElements().size(), 1);
    }

    @Test
    public void testFinishFreesReader()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(5));
        sharedBuffer.addQueue("queue");
        assertFalse(sharedBuffer.isFinished());

        ExecutorService executor = Executors.newCachedThreadPool();

        // exec thread to get two pages
        GetPagesJob getPagesJob = new GetPagesJob(sharedBuffer, 0, 2, 1);
        executor.submit(getPagesJob);
        getPagesJob.waitForStarted();

        // "verify" thread is blocked
        getPagesJob.assertBlockedWithCount(0);

        // add one item
        assertTrue(sharedBuffer.offer(createPage(0)));

        // verify thread got one page and is blocked
        getPagesJob.assertBlockedWithCount(1);

        // finish the query
        sharedBuffer.finish();
        assertQueueClosed(sharedBuffer, "queue", 1);

        // verify thread is released
        getPagesJob.waitForFinished();

        // verify thread only got one page
        assertEquals(getPagesJob.getElements().size(), 1);
    }

    @Test
    public void testFinishFreesWriter()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(5));
        sharedBuffer.addQueue("queue");
        sharedBuffer.noMoreQueues();
        assertFalse(sharedBuffer.isFinished());

        ExecutorService executor = Executors.newCachedThreadPool();

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            assertTrue(sharedBuffer.offer(createPage(i)));
        }

        // exec thread to add two pages
        AddPagesJob addPagesJob = new AddPagesJob(sharedBuffer, createPage(2), createPage(3));
        executor.submit(addPagesJob);
        addPagesJob.waitForStarted();

        // "verify" thread is blocked
        addPagesJob.assertBlockedWithCount(2);

        // get one page
        assertEquals(sharedBuffer.get("queue", 0, 1, MAX_WAIT).size(), 1);
        sharedBuffer.acknowledge("queue", 1);

        // "verify" thread is blocked again with one remaining page
        addPagesJob.assertBlockedWithCount(1);

        // finish the query
        sharedBuffer.finish();
        assertFalse(sharedBuffer.isFinished());

        // verify thread is released
        addPagesJob.waitForFinished();

        // get the last 5 page
        assertEquals(sharedBuffer.get("queue", 1, 100, MAX_WAIT).size(), 5);
        sharedBuffer.acknowledge("queue", 5);

        // verify finished
        assertFinished(sharedBuffer);
    }

    @Test
    public void testDestroyFreesReader()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(5));
        sharedBuffer.addQueue("queue");
        assertFalse(sharedBuffer.isFinished());

        ExecutorService executor = Executors.newCachedThreadPool();

        // exec thread to get two pages
        GetPagesJob getPagesJob = new GetPagesJob(sharedBuffer, 0, 2, 1);
        executor.submit(getPagesJob);
        getPagesJob.waitForStarted();

        // "verify" thread is blocked
        getPagesJob.assertBlockedWithCount(0);

        // add one page
        assertTrue(sharedBuffer.offer(createPage(0)));

        // verify thread got one page and is blocked
        getPagesJob.assertBlockedWithCount(1);

        // destroy the buffer
        sharedBuffer.destroy();
        assertQueueClosed(sharedBuffer, "queue", 1);

        // verify thread is released
        getPagesJob.waitForFinished();

        // verify thread only got one page
        assertEquals(getPagesJob.getElements().size(), 1);
    }

    @Test
    public void testDestroyFreesWriter()
            throws Exception
    {
        SharedBuffer sharedBuffer = new SharedBuffer(sizeOfPages(5));
        sharedBuffer.addQueue("queue");
        sharedBuffer.noMoreQueues();
        assertFalse(sharedBuffer.isFinished());

        ExecutorService executor = Executors.newCachedThreadPool();

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            assertTrue(sharedBuffer.offer(createPage(i)));
        }

        // exec thread to add two page
        AddPagesJob addPagesJob = new AddPagesJob(sharedBuffer, createPage(2), createPage(3));
        executor.submit(addPagesJob);
        addPagesJob.waitForStarted();

        // "verify" thread is blocked
        addPagesJob.assertBlockedWithCount(2);

        // get one page
        assertEquals(sharedBuffer.get("queue", 0, 1, MAX_WAIT).size(), 1);
        sharedBuffer.acknowledge("queue", 1);

        // "verify" thread is blocked again with one remaining page
        addPagesJob.assertBlockedWithCount(1);

        // cancel the query
        sharedBuffer.destroy();
        assertFinished(sharedBuffer);

        // verify thread is released
        addPagesJob.waitForFinished();
    }

    private void assertQueueState(SharedBuffer sharedBuffer, String queueId, int size, int pagesSent)
    {
        assertEquals(getBufferInfo(sharedBuffer, queueId), new BufferInfo(queueId, false, size, pagesSent));
    }

    private void assertQueueClosed(SharedBuffer sharedBuffer, String queueId, int pagesSent)
    {
        assertEquals(getBufferInfo(sharedBuffer, queueId), new BufferInfo(queueId, true, 0, pagesSent));
    }

    private BufferInfo getBufferInfo(SharedBuffer sharedBuffer, String queueId)
    {
        for (BufferInfo bufferInfo : sharedBuffer.getInfo().getBuffers()) {
            if (bufferInfo.getBufferId().equals(queueId)) {
                return bufferInfo;
            }
        }
        return null;
    }

    private void assertFinished(SharedBuffer sharedBuffer)
            throws Exception
    {
        assertTrue(sharedBuffer.isFinished());
        for (BufferInfo bufferInfo : sharedBuffer.getInfo().getBuffers()) {
            assertTrue(bufferInfo.isFinished());
            assertEquals(bufferInfo.getBufferedPages(), 0);
        }
    }

    private void assertBufferResultEquals(BufferResult actual, BufferResult expected)
    {
        assertEquals(actual.getElements().size(), expected.getElements().size());
        assertEquals(actual.getStartingSequenceId(), expected.getStartingSequenceId());
        for (int i = 0; i < actual.getElements().size(); i++) {
            Page actualPage = actual.getElements().get(i);
            Page expectedPage = expected.getElements().get(i);
            assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
            for (int channel = 0; channel < actualPage.getChannelCount(); channel++) {
                assertBlockEquals(actualPage.getBlock(channel), expectedPage.getBlock(channel));
            }

        }
        assertEquals(actual.isBufferClosed(), expected.isBufferClosed());
    }

    private static class GetPagesJob implements Runnable
    {
        private final SharedBuffer sharedBuffer;
        private final int pagesToGet;
        private final int batchSize;
        private long sequenceId;

        private final AtomicReference<FailedQueryException> failedQueryException = new AtomicReference<>();

        private final CopyOnWriteArrayList<Page> elements = new CopyOnWriteArrayList<>();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);

        private GetPagesJob(SharedBuffer sharedBuffer, long startingSequenceId, int pagesToGet, int batchSize)
        {
            this.sharedBuffer = sharedBuffer;
            this.sequenceId = startingSequenceId;
            this.pagesToGet = pagesToGet;
            this.batchSize = batchSize;
        }

        public List<Page> getElements()
        {
            return ImmutableList.copyOf(elements);
        }

        /**
         * Do our best to assure the thread is blocked.
         */
        public void assertBlockedWithCount(int expectedBlockSize)
        {
            // the best we can do is to verify the count hasn't changed in after sleeping for a bit

            assertTrue(isStarted());
            assertTrue(!isFinished());

            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            assertEquals(elements.size(), expectedBlockSize);
            assertTrue(isStarted());
            assertTrue(!isFinished());
        }

        private boolean isFinished()
        {
            return finished.getCount() == 0;
        }

        private boolean isStarted()
        {
            return started.getCount() == 0;
        }

        public void waitForStarted()
                throws InterruptedException
        {
            assertTrue(started.await(1, TimeUnit.SECONDS), "Job did not start with in 1 second");
        }

        public void waitForFinished()
                throws InterruptedException
        {
            long wait = (long) (MAX_WAIT.toMillis() * 3);
            assertTrue(finished.await(wait, TimeUnit.MILLISECONDS), "Job did not finish with in " + wait + " ms");
        }

        @Override
        public void run()
        {
            started.countDown();
            try {
                while (elements.size() < pagesToGet) {
                    try {
                        BufferResult result = sharedBuffer.get("queue", sequenceId, batchSize, MAX_WAIT);
                        assertTrue(!result.isEmpty());
                        this.elements.addAll(result.getElements());
                        sequenceId = result.getStartingSequenceId() + result.getElements().size();
                        sharedBuffer.acknowledge("queue", sequenceId);
                    }
                    catch (FailedQueryException e) {
                        failedQueryException.set(e);
                        break;
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                }
            }
            finally {
                finished.countDown();
            }
        }
    }

    private static class AddPagesJob implements Runnable
    {
        private final SharedBuffer sharedBuffer;
        private final ArrayBlockingQueue<Page> elements;

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);

        private AddPagesJob(SharedBuffer sharedBuffer, Page... elements)
        {
            this.sharedBuffer = sharedBuffer;
            this.elements = new ArrayBlockingQueue<>(elements.length);
            Collections.addAll(this.elements, elements);
        }

        /**
         * Do our best to assure the thread is blocked.
         */
        public void assertBlockedWithCount(int expectedBlockSize)
        {
            // the best we can do is to verify the count hasn't changed in after sleeping for a bit

            assertTrue(isStarted());
            assertTrue(!isFinished());

            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            assertEquals(elements.size(), expectedBlockSize);
            assertTrue(isStarted());
            assertTrue(!isFinished());
        }

        private boolean isFinished()
        {
            return finished.getCount() == 0;
        }

        private boolean isStarted()
        {
            return started.getCount() == 0;
        }

        public void waitForStarted()
                throws InterruptedException
        {
            assertTrue(started.await(1, TimeUnit.SECONDS), "Job did not start with in 1 second");
        }

        public void waitForFinished()
                throws InterruptedException
        {
            long wait = (long) (MAX_WAIT.toMillis() * 3);
            assertTrue(finished.await(wait, TimeUnit.MILLISECONDS), "Job did not finish with in " + wait + " ms");
        }

        @Override
        public void run()
        {
            started.countDown();
            try {
                for (Page element = elements.peek(); element != null; element = elements.peek()) {
                    try {
                        sharedBuffer.add(element);
                        assertNotNull(elements.poll());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                }
            }
            finally {
                finished.countDown();
            }
        }
    }
}
