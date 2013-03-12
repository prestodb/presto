/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSharedBuffer
{
    private static final Duration NO_WAIT = new Duration(0, TimeUnit.MILLISECONDS);
    private static final Duration MAX_WAIT = new Duration(1, TimeUnit.SECONDS);

    @Test
    public void testInvalidConstructorArg()
            throws Exception
    {
        try {
            new SharedBuffer<>(0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException e) {

        }
        try {
            new SharedBuffer<>(-1);
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testSimple()
            throws Exception
    {
        SharedBuffer<Integer> sharedBuffer = new SharedBuffer<>(10);

        // add three items
        for (int i = 0; i < 3; i++) {
            assertTrue(sharedBuffer.offer(i));
        }

        // add a queue
        sharedBuffer.addQueue("first");
        assertQueueState(sharedBuffer, "first", 3);

        // get the three elements
        assertEquals(sharedBuffer.get("first", 10, NO_WAIT), ImmutableList.of(0, 1, 2));
        assertQueueState(sharedBuffer, "first", 0);

        // try to get some more pages
        assertEquals(sharedBuffer.get("first", 10, NO_WAIT), ImmutableList.of());

        // fill the buffer (we already added 3 pages)
        for (int i = 3; i < 10; i++) {
            assertTrue(sharedBuffer.offer(i));
        }
        assertQueueState(sharedBuffer, "first", 7);

        // try to add one more page, which should fail
        assertFalse(sharedBuffer.offer(99));

        // remove a page
        assertEquals(sharedBuffer.get("first", 1, NO_WAIT), ImmutableList.of(3));
        assertQueueState(sharedBuffer, "first", 6);

        // try to add one more page, which should fail
        assertFalse(sharedBuffer.offer(99));

        //
        // add another buffer and verify it sees all pages
        sharedBuffer.addQueue("second");
        assertQueueState(sharedBuffer, "second", 10);
        assertEquals(sharedBuffer.get("second", 10, NO_WAIT), ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        assertQueueState(sharedBuffer, "second", 0);

        //
        // tell shared buffer there will be no more queues
        sharedBuffer.noMoreQueues();

        // since both queues consumed the first four pages, we should be able to add 4 more pages
        for (int i = 10; i < 14; i++) {
            assertTrue(sharedBuffer.offer(i));
        }
        assertFalse(sharedBuffer.offer(99));
        assertQueueState(sharedBuffer, "first", 10);
        assertQueueState(sharedBuffer, "second", 4);

        // remove a page from the first queue
        assertEquals(sharedBuffer.get("first", 1, NO_WAIT), ImmutableList.of(4));
        assertQueueState(sharedBuffer, "first", 9);
        assertQueueState(sharedBuffer, "second", 4);

        // try to add one more page, which should work since the first queue is the largest queue
        assertTrue(sharedBuffer.offer(14));
        assertFalse(sharedBuffer.offer(99));
        assertQueueState(sharedBuffer, "first", 10);
        assertQueueState(sharedBuffer, "second", 5);

        //
        // finish the buffer
        assertFalse(sharedBuffer.isFinished());
        sharedBuffer.finish();
        assertQueueState(sharedBuffer, "first", 10);
        assertQueueState(sharedBuffer, "second", 5);

        // not fully finished until all pages are consumed
        assertFalse(sharedBuffer.isFinished());

        // remove a page, not finished
        assertEquals(sharedBuffer.get("first", 1, NO_WAIT), ImmutableList.of(5));
        assertQueueState(sharedBuffer, "first", 9);
        assertQueueState(sharedBuffer, "second", 5);
        assertFalse(sharedBuffer.isFinished());

        // remove all remaining pages from first queue, should not be finished
        assertEquals(sharedBuffer.get("first", 10, NO_WAIT), ImmutableList.of(6, 7, 8, 9, 10, 11, 12, 13, 14));
        assertQueueClosed(sharedBuffer, "first");
        assertQueueState(sharedBuffer, "second", 5);
        assertFalse(sharedBuffer.isFinished());

        // remove all remaining pages from second queue, should be finished
        assertEquals(sharedBuffer.get("second", 10, NO_WAIT), ImmutableList.of(10, 11, 12, 13, 14));
        assertQueueClosed(sharedBuffer, "first");
        assertQueueClosed(sharedBuffer, "second");
        assertFinished(sharedBuffer);

        assertEquals(sharedBuffer.get("first", 10, NO_WAIT), ImmutableList.of());
        assertEquals(sharedBuffer.get("second", 10, NO_WAIT), ImmutableList.of());
    }

    @Test
    public void testAddQueueAfterNoMoreQueues()
            throws Exception
    {
        SharedBuffer<Integer> sharedBuffer = new SharedBuffer<>(10);
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
        SharedBuffer<Integer> sharedBuffer = new SharedBuffer<>(10);
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

        try {
            sharedBuffer.addQueue("foo");
            fail("Expected IllegalStateException from addQueue after noMoreQueues has been called");
        }
        catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testOperationsOnUnknownQueues()
            throws Exception
    {
        SharedBuffer<Integer> sharedBuffer = new SharedBuffer<>(10);
        assertFalse(sharedBuffer.isFinished());

        // verify operations on unknown queue throw an exception
        try {
            sharedBuffer.get("unknown", 1, NO_WAIT);
            fail("Expected NoSuchBufferException from operation on unknown queue");
        }
        catch (NoSuchBufferException expected) {
        }

        // abort on unknown buffer is allowed
        sharedBuffer.abort("unknown");

        // finish buffer and try operations again
        sharedBuffer.finish();
        try {
            sharedBuffer.get("unknown", 1, NO_WAIT);
            fail("Expected NoSuchBufferException from operation on unknown queue");
        }
        catch (NoSuchBufferException expected) {
        }

        // abort on unknown buffer is allowed
        sharedBuffer.abort("unknown");

        // set no more queues and try operations again
        sharedBuffer.noMoreQueues();
        try {
            sharedBuffer.get("unknown", 1, NO_WAIT);
            fail("Expected NoSuchBufferException from operation on unknown queue");
        }
        catch (NoSuchBufferException expected) {
        }

        // abort on unknown buffer is allowed
        sharedBuffer.abort("unknown");


        // destroy and try operations again
        sharedBuffer.destroy();
        try {
            sharedBuffer.get("unknown", 1, NO_WAIT);
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
        SharedBuffer<Integer> sharedBuffer = new SharedBuffer<>(10);
        sharedBuffer.finish();
        assertFalse(sharedBuffer.offer(0));
        assertFalse(sharedBuffer.add(0));

        // add after destroy
        sharedBuffer = new SharedBuffer<>(10);
        sharedBuffer.destroy();
        assertFalse(sharedBuffer.offer(0));
        assertFalse(sharedBuffer.add(0));
    }

    @Test
    public void testAbort()
            throws Exception
    {
        SharedBuffer<Integer> sharedBuffer = new SharedBuffer<>(10);

        // fill the buffer
        for (int i = 0; i < 10; i++) {
            assertTrue(sharedBuffer.offer(i));
        }
        sharedBuffer.finish();

        sharedBuffer.addQueue("first");
        assertEquals(sharedBuffer.get("first", 1, NO_WAIT), ImmutableList.of(0));
        sharedBuffer.abort("first");
        assertQueueClosed(sharedBuffer, "first");
        assertEquals(sharedBuffer.get("first", 1, NO_WAIT), ImmutableList.of());

        sharedBuffer.addQueue("second");
        sharedBuffer.noMoreQueues();
        assertEquals(sharedBuffer.get("second", 1, NO_WAIT), ImmutableList.of(0));
        sharedBuffer.abort("second");
        assertQueueClosed(sharedBuffer, "second");
        assertFinished(sharedBuffer);
        assertEquals(sharedBuffer.get("second", 1, NO_WAIT), ImmutableList.of());
    }

    @Test
    public void testFinishClosesEmptyQueues()
            throws Exception
    {
        SharedBuffer<Integer> sharedBuffer = new SharedBuffer<>(10);
        sharedBuffer.addQueue("first");
        sharedBuffer.addQueue("second");

        // finish while queues are empty
        sharedBuffer.finish();

        assertQueueClosed(sharedBuffer, "first");
        assertQueueClosed(sharedBuffer, "second");
    }

    @Test
    public void testAbortFreesReader()
            throws Exception
    {
        SharedBuffer<Integer> sharedBuffer = new SharedBuffer<>(5);
        sharedBuffer.addQueue("queue");
        assertFalse(sharedBuffer.isFinished());

        ExecutorService executor = Executors.newCachedThreadPool();

        // exec thread to get two pages
        GetPagesJob<Integer> getPagesJob = new GetPagesJob<>(sharedBuffer, 2, 1);
        executor.submit(getPagesJob);
        getPagesJob.waitForStarted();

        // "verify" thread is blocked
        getPagesJob.assertBlockedWithCount(0);

        // add one page
        assertTrue(sharedBuffer.offer(0));

        // verify thread got one page and is blocked
        getPagesJob.assertBlockedWithCount(1);

        // abort the buffer
        sharedBuffer.abort("queue");
        assertQueueClosed(sharedBuffer, "queue");

        // verify thread is released
        getPagesJob.waitForFinished();

        // verify thread only got one page
        assertEquals(getPagesJob.getElements().size(), 1);
    }

    @Test
    public void testFinishFreesReader()
            throws Exception
    {
        SharedBuffer<Integer> sharedBuffer = new SharedBuffer<>(5);
        sharedBuffer.addQueue("queue");
        assertFalse(sharedBuffer.isFinished());

        ExecutorService executor = Executors.newCachedThreadPool();

        // exec thread to get two pages
        GetPagesJob<Integer> getPagesJob = new GetPagesJob<>(sharedBuffer, 2, 1);
        executor.submit(getPagesJob);
        getPagesJob.waitForStarted();

        // "verify" thread is blocked
        getPagesJob.assertBlockedWithCount(0);

        // add one item
        assertTrue(sharedBuffer.offer(0));

        // verify thread got one page and is blocked
        getPagesJob.assertBlockedWithCount(1);

        // finish the query
        sharedBuffer.finish();
        assertQueueClosed(sharedBuffer, "queue");

        // verify thread is released
        getPagesJob.waitForFinished();

        // verify thread only got one page
        assertEquals(getPagesJob.getElements().size(), 1);
    }

    @Test
    public void testFinishFreesWriter()
            throws Exception
    {
        SharedBuffer<Integer> sharedBuffer = new SharedBuffer<>(5);
        sharedBuffer.addQueue("queue");
        sharedBuffer.noMoreQueues();
        assertFalse(sharedBuffer.isFinished());

        ExecutorService executor = Executors.newCachedThreadPool();

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            assertTrue(sharedBuffer.offer(i));
        }

        // exec thread to add two pages
        AddPagesJob<Integer> addPagesJob = new AddPagesJob<>(sharedBuffer, 2, 3);
        executor.submit(addPagesJob);
        addPagesJob.waitForStarted();

        // "verify" thread is blocked
        addPagesJob.assertBlockedWithCount(2);

        // get one page
        assertEquals(sharedBuffer.get("queue", 1, MAX_WAIT).size(), 1);

        // "verify" thread is blocked again with one remaining page
        addPagesJob.assertBlockedWithCount(1);

        // finish the query
        sharedBuffer.finish();
        assertFalse(sharedBuffer.isFinished());

        // verify thread is released
        addPagesJob.waitForFinished();

        // get the last 5 page
        assertEquals(sharedBuffer.get("queue", 100, MAX_WAIT).size(), 5);

        // verify finished
        assertFinished(sharedBuffer);
    }

    @Test
    public void testDestroyFreesReader()
            throws Exception
    {
        SharedBuffer<Integer> sharedBuffer = new SharedBuffer<>(5);
        sharedBuffer.addQueue("queue");
        assertFalse(sharedBuffer.isFinished());

        ExecutorService executor = Executors.newCachedThreadPool();

        // exec thread to get two pages
        GetPagesJob<Integer> getPagesJob = new GetPagesJob<>(sharedBuffer, 2, 1);
        executor.submit(getPagesJob);
        getPagesJob.waitForStarted();

        // "verify" thread is blocked
        getPagesJob.assertBlockedWithCount(0);

        // add one page
        assertTrue(sharedBuffer.offer(0));

        // verify thread got one page and is blocked
        getPagesJob.assertBlockedWithCount(1);

        // destroy the buffer
        sharedBuffer.destroy();
        assertQueueClosed(sharedBuffer, "queue");

        // verify thread is released
        getPagesJob.waitForFinished();

        // verify thread only got one page
        assertEquals(getPagesJob.getElements().size(), 1);
    }

    @Test
    public void testDestroyFreesWriter()
            throws Exception
    {
        SharedBuffer<Integer> sharedBuffer = new SharedBuffer<>(5);
        sharedBuffer.addQueue("queue");
        sharedBuffer.noMoreQueues();
        assertFalse(sharedBuffer.isFinished());

        ExecutorService executor = Executors.newCachedThreadPool();

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            assertTrue(sharedBuffer.offer(i));
        }

        // exec thread to add two page
        AddPagesJob<Integer> addPagesJob = new AddPagesJob<>(sharedBuffer, 2, 3);
        executor.submit(addPagesJob);
        addPagesJob.waitForStarted();

        // "verify" thread is blocked
        addPagesJob.assertBlockedWithCount(2);

        // get one page
        assertEquals(sharedBuffer.get("queue", 1, MAX_WAIT).size(), 1);

        // "verify" thread is blocked again with one remaining page
        addPagesJob.assertBlockedWithCount(1);

        // cancel the query
        sharedBuffer.destroy();
        assertFinished(sharedBuffer);

        // verify thread is released
        addPagesJob.waitForFinished();
    }

    private void assertQueueState(SharedBuffer<?> sharedBuffer, String queueId, int size)
    {
        assertEquals(getBufferInfo(sharedBuffer, queueId), new BufferInfo(queueId, false, size));
    }

    private void assertQueueClosed(SharedBuffer<?> sharedBuffer, String queueId)
    {
        assertEquals(getBufferInfo(sharedBuffer, queueId), new BufferInfo(queueId, true, 0));
    }

    private BufferInfo getBufferInfo(SharedBuffer<?> sharedBuffer, String queueId)
    {
        for (BufferInfo bufferInfo : sharedBuffer.getInfo()) {
            if (bufferInfo.getBufferId().equals(queueId)) {
                return bufferInfo;
            }
        }
        return null;
    }

    private void assertFinished(SharedBuffer<?> sharedBuffer)
            throws Exception
    {
        assertTrue(sharedBuffer.isFinished());
        for (BufferInfo bufferInfo : sharedBuffer.getInfo()) {
            assertTrue(bufferInfo.isFinished());
            assertEquals(bufferInfo.getBufferedPages(), 0);
        }
    }

    private static class GetPagesJob<T> implements Runnable
    {
        private final SharedBuffer<T> sharedBuffer;
        private final int pagesToGet;
        private final int batchSize;

        private final AtomicReference<FailedQueryException> failedQueryException = new AtomicReference<>();

        private final CopyOnWriteArrayList<T> elements = new CopyOnWriteArrayList<>();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);

        private GetPagesJob(SharedBuffer<T> sharedBuffer, int pagesToGet, int batchSize)
        {
            this.sharedBuffer = sharedBuffer;
            this.pagesToGet = pagesToGet;
            this.batchSize = batchSize;
        }

        public List<T> getElements()
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
                        List<T> elements = sharedBuffer.get("queue", batchSize, MAX_WAIT);
                        assertTrue(!elements.isEmpty());
                        this.elements.addAll(elements);
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

    private static class AddPagesJob<T> implements Runnable
    {
        private final SharedBuffer<T> sharedBuffer;
        private final ArrayBlockingQueue<T> elements;

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);

        @SafeVarargs
        private AddPagesJob(SharedBuffer<T> sharedBuffer, T... elements)
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
                for (T element = elements.peek(); element != null; element = elements.peek()) {
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
