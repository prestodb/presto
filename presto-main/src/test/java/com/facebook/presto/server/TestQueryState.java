/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Page;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestQueryState
{
    private ExecutorService executor;

    @BeforeMethod
    protected void setUp()
            throws Exception
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
    }

    @Test
    public void testInvalidConstruction()
            throws Exception
    {
        try {
            new QueryState(0, 4);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {

        }
        try {
            new QueryState(4, 0);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testNormalExecution()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 20);
        assertRunning(queryState);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            queryState.addPage(createLongPage(i, i));
        }

        // verify pages are in correct order
        assertRunning(queryState);

        List<Page> nextPages = queryState.getNextPages(2);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 0);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 1);

        assertRunning(queryState);

        nextPages = queryState.getNextPages(2);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 2);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 3);

        assertRunning(queryState);

        nextPages = queryState.getNextPages(2);
        assertEquals(nextPages.size(), 1);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 4);

        assertRunning(queryState);

        // add one more page
        int position = 9;
        int value = 9;
        queryState.addPage(createLongPage(position, value));

        // mark source as finished
        queryState.sourceFinished();

        assertRunning(queryState);

        // get the last page and assure the query is finished
        nextPages = queryState.getNextPages(2);
        assertEquals(nextPages.size(), 1);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 9);
        assertFinished(queryState);

        // attempt to add more pages
        try {
            queryState.addPage(createLongPage(22, 22));
            fail("expected IllegalStateException");
        }
        catch (IllegalStateException e) {
        }
        assertFinished(queryState);

        // mark source as finished again
        queryState.sourceFinished();
        assertFinished(queryState);

        // try to fail the query and verify it doesn't work
        queryState.queryFailed(new RuntimeException());
        assertFinished(queryState);

        // try to cancel the query and verify it doesn't work
        queryState.cancel();
        assertFinished(queryState);
    }

    private Page createLongPage(int position, int value)
    {
        return new Page(createLongsBlock(value));
    }

    @Test
    public void testFailedExecution()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 20);
        assertRunning(queryState);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            queryState.addPage(createLongPage(i, i));
        }

        // verify pages are in correct order
        assertRunning(queryState);

        List<Page> nextPages = queryState.getNextPages(2);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 0);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 1);

        assertRunning(queryState);

        nextPages = queryState.getNextPages(2);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 2);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 3);

        assertRunning(queryState);

        // Fail query with one page in the buffer
        RuntimeException exception = new RuntimeException("failed");
        queryState.queryFailed(exception);
        assertFailed(queryState, exception);

        // attempt to add more pages
        queryState.addPage(createLongPage(22, 22));
        assertFailed(queryState, exception);

        // fail the query again
        RuntimeException anotherException = new RuntimeException("failed again");
        queryState.queryFailed(anotherException);
        assertFailed(queryState, exception, anotherException);

        // try to cancel the finished query and verify it doesn't work
        queryState.cancel();
        assertFailed(queryState, exception, anotherException);

        // try to cancel the query again and verify it doesn't work
        queryState.cancel();
        assertFailed(queryState, exception, anotherException);

        // try to finish the query and verify it doesn't work
        queryState.sourceFinished();
        assertFailed(queryState, exception, anotherException);
    }

    @Test
    public void testCanceledExecution()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 20);
        assertRunning(queryState);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            queryState.addPage(createLongPage(i, i));
        }

        // verify pages are in correct order
        assertRunning(queryState);

        List<Page> nextPages = queryState.getNextPages(2);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 0);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 1);

        assertRunning(queryState);

        nextPages = queryState.getNextPages(2);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 2);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 3);

        assertRunning(queryState);

        // Cancel query with one page in the buffer
        queryState.cancel();
        assertCanceled(queryState);

        // attempt to add more pages
        queryState.addPage(createLongPage(22, 22));
        assertCanceled(queryState);

        // cancel the query again
        queryState.cancel();
        assertCanceled(queryState);

        // try to fail the finished query and verify it doesn't work
        queryState.queryFailed(new RuntimeException());
        assertCanceled(queryState);

        // try to finish the query and verify it doesn't work
        queryState.sourceFinished();
        assertCanceled(queryState);
    }

    @Test
    public void testMultiSourceNormalExecution()
            throws Exception
    {
        QueryState queryState = new QueryState(3, 20);
        assertRunning(queryState);

        // add some pages
        queryState.addPage(createLongPage(0, 0));
        queryState.addPage(createLongPage(1, 1));

        // verify pages are in correct order
        assertRunning(queryState);

        List<Page> nextPages = queryState.getNextPages(2);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 0);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 1);
        assertRunning(queryState);

        // finish first sources
        queryState.sourceFinished();
        assertRunning(queryState);

        // add one more page
        queryState.addPage(createLongPage(9, 9));

        // finish second source
        queryState.sourceFinished();
        assertRunning(queryState);

        // the page
        nextPages = queryState.getNextPages(2);
        assertEquals(nextPages.size(), 1);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 9);
        assertRunning(queryState);

        // finish last source, and verify the query is finished since there are no buffered pages
        queryState.sourceFinished();
        assertFinished(queryState);

        // attempt to add more pages
        try {
            queryState.addPage(createLongPage(22, 22));
            fail("expected IllegalStateException");
        }
        catch (IllegalStateException e) {
        }
        assertFinished(queryState);

        // mark source as finished again
        queryState.sourceFinished();
        assertFinished(queryState);

        // try to fail the query and verify it doesn't work
        queryState.queryFailed(new RuntimeException());
        assertFinished(queryState);

        // try to cancel the query and verify it doesn't work
        queryState.cancel();
        assertFinished(queryState);
    }

    @Test
    public void testBufferSizeNormal()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 5);
        assertRunning(queryState);

        // exec thread to get two pages
        GetPagesJob getPagesJob = new GetPagesJob(queryState, 2, 1);
        executor.submit(getPagesJob);
        getPagesJob.waitForStarted();

        // "verify" thread is blocked
        getPagesJob.assertBlockedWithCount(0);

        // add one page
        queryState.addPage(createLongPage(0, 0));

        // verify thread got one page and is blocked
        getPagesJob.assertBlockedWithCount(1);

        // add one page
        queryState.addPage(createLongPage(1, 1));

        // verify thread is released
        getPagesJob.waitForFinished();

        // verify thread got one page
        assertEquals(getPagesJob.getPages().size(), 2);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            queryState.addPage(createLongPage(i, i));
        }

        // exec thread to add two more pages
        AddPagesJob addPagesJob = new AddPagesJob(queryState, createLongPage(2, 2), createLongPage(3, 3));
        executor.submit(addPagesJob);
        addPagesJob.waitForStarted();

        // "verify" thread is blocked
        addPagesJob.assertBlockedWithCount(2);

        // get one page
        assertEquals(queryState.getNextPages(1).size(), 1);

        // "verify" thread is blocked again with one remaining page
        addPagesJob.assertBlockedWithCount(1);

        // get one page
        assertEquals(queryState.getNextPages(1).size(), 1);

        // verify thread is released
        addPagesJob.waitForFinished();

        // verify thread added one page
        assertEquals(addPagesJob.getPages().size(), 0);
    }

    @Test
    public void testCancelFreesReader()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 5);
        assertRunning(queryState);

        ExecutorService executor = Executors.newCachedThreadPool();

        // exec thread to get two pages
        GetPagesJob getPagesJob = new GetPagesJob(queryState, 2, 1);
        executor.submit(getPagesJob);
        getPagesJob.waitForStarted();

        // "verify" thread is blocked
        getPagesJob.assertBlockedWithCount(0);

        // add one page
        queryState.addPage(createLongPage(0, 0));

        // verify thread got one page and is blocked
        getPagesJob.assertBlockedWithCount(1);

        // cancel the query
        queryState.cancel();
        assertCanceled(queryState);

        // verify thread is released
        getPagesJob.waitForFinished();

        // verify thread only got one page
        assertEquals(getPagesJob.getPages().size(), 1);
    }

    @Test
    public void testCancelFreesWriter()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 5);
        assertRunning(queryState);

        ExecutorService executor = Executors.newCachedThreadPool();

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            queryState.addPage(createLongPage(i, i));
        }

        // exec thread to add two pages
        AddPagesJob addPagesJob = new AddPagesJob(queryState, createLongPage(2, 2), createLongPage(3, 3));
        executor.submit(addPagesJob);
        addPagesJob.waitForStarted();

        // "verify" thread is blocked
        addPagesJob.assertBlockedWithCount(2);

        // get one page
        assertEquals(queryState.getNextPages(1).size(), 1);

        // "verify" thread is blocked again with one remaining page
        addPagesJob.assertBlockedWithCount(1);

        // cancel the query
        queryState.cancel();
        assertCanceled(queryState);

        // verify thread is released
        addPagesJob.waitForFinished();
    }

    @Test
    public void testFailFreesReader()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 5);
        assertRunning(queryState);

        ExecutorService executor = Executors.newCachedThreadPool();

        // exec thread to get two pages
        GetPagesJob getPagesJob = new GetPagesJob(queryState, 2, 1);
        executor.submit(getPagesJob);
        getPagesJob.waitForStarted();

        // "verify" thread is blocked
        getPagesJob.assertBlockedWithCount(0);

        // add one page
        queryState.addPage(createLongPage(0, 0));

        // verify thread got one page and is blocked
        getPagesJob.assertBlockedWithCount(1);

        // fail the query
        RuntimeException exception = new RuntimeException("failed");
        queryState.queryFailed(exception);
        assertFailed(queryState, exception);

        // verify thread is released
        getPagesJob.waitForFinished();

        // verify thread only got one page
        assertEquals(getPagesJob.getPages().size(), 1);
        assertFailedQuery(getPagesJob.getFailedQueryException(), exception);
    }

    @Test
    public void testFailFreesWriter()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 5);
        assertRunning(queryState);

        ExecutorService executor = Executors.newCachedThreadPool();

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            queryState.addPage(createLongPage(i, i));
        }

        // exec thread to add two page
        AddPagesJob addPagesJob = new AddPagesJob(queryState, createLongPage(2, 2), createLongPage(3, 3));
        executor.submit(addPagesJob);
        addPagesJob.waitForStarted();

        // "verify" thread is blocked
        addPagesJob.assertBlockedWithCount(2);

        // get one page
        assertEquals(queryState.getNextPages(1).size(), 1);

        // "verify" thread is blocked again with one remaining page
        addPagesJob.assertBlockedWithCount(1);

        // fail the query
        RuntimeException exception = new RuntimeException("failed");
        queryState.queryFailed(exception);
        assertFailed(queryState, exception);

        // verify thread is released
        addPagesJob.waitForFinished();
    }

    private static class GetPagesJob implements Runnable
    {
        private final QueryState queryState;
        private final int pagesToGet;
        private final int batchSize;

        private final AtomicReference<FailedQueryException> failedQueryException = new AtomicReference<>();

        private final CopyOnWriteArrayList<Page> pages = new CopyOnWriteArrayList<>();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);

        private GetPagesJob(QueryState queryState, int pagesToGet, int batchSize)
        {
            this.queryState = queryState;
            this.pagesToGet = pagesToGet;
            this.batchSize = batchSize;
        }

        public List<Page> getPages()
        {
            return ImmutableList.copyOf(pages);
        }

        public FailedQueryException getFailedQueryException()
        {
            return failedQueryException.get();
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

            assertEquals(pages.size(), expectedBlockSize);
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
            assertTrue(finished.await(1, TimeUnit.SECONDS), "Job did not finish with in 1 second");
        }

        @Override
        public void run()
        {
            started.countDown();
            try {
                while (pages.size() < pagesToGet) {
                    try {
                        List<Page> pages = queryState.getNextPages(batchSize);
                        assertTrue(!pages.isEmpty());
                        this.pages.addAll(pages);
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
        private final QueryState queryState;
        private final ArrayBlockingQueue<Page> pages;

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);

        private AddPagesJob(QueryState queryState, Page... pages)
        {
            this.queryState = queryState;
            this.pages = new ArrayBlockingQueue<>(pages.length);
            Collections.addAll(this.pages, pages);
        }

        public List<Page> getPages()
        {
            return ImmutableList.copyOf(pages);
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

            assertEquals(pages.size(), expectedBlockSize);
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
            assertTrue(finished.await(1, TimeUnit.SECONDS), "Job did not finish with in 1 second");
        }

        @Override
        public void run()
        {
            started.countDown();
            try {
                for (Page page = pages.peek(); page != null; page = pages.peek()) {
                    try {
                        queryState.addPage(page);
                        assertNotNull(pages.poll());
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

    private void assertRunning(QueryState queryState)
    {
        assertFalse(queryState.isDone());
        assertFalse(queryState.isCanceled());
        assertFalse(queryState.isFailed());
    }

    private void assertFinished(QueryState queryState)
            throws Exception
    {
        assertTrue(queryState.isDone());
        assertFalse(queryState.isCanceled());
        assertFalse(queryState.isFailed());

        // getNextPages should return an empty list
        for (int loop = 0; loop < 5; loop++) {
            List<Page> nextPages = queryState.getNextPages(2);
            assertNotNull(nextPages);
            assertEquals(nextPages.size(), 0);
        }
    }

    private void assertFailed(QueryState queryState, Throwable... expectedCauses)
            throws Exception
    {
        assertTrue(queryState.isDone());
        assertFalse(queryState.isCanceled());
        assertTrue(queryState.isFailed());

        // getNextPages should throw an exception
        for (int loop = 0; loop < 5; loop++) {
            try {
                queryState.getNextPages(2);
                fail("expected FailedQueryException");
            }
            catch (FailedQueryException e) {
                assertFailedQuery(e, expectedCauses);
            }
        }
    }

    private void assertFailedQuery(FailedQueryException failedQueryException, Throwable... expectedCauses)
    {
        assertNotNull(failedQueryException);
        Throwable[] suppressed = failedQueryException.getSuppressed();
        assertEquals(suppressed.length, expectedCauses.length);
        for (int i = 0; i < suppressed.length; i++) {
            assertSame(suppressed[i], expectedCauses[i]);
        }
    }

    private void assertCanceled(QueryState queryState)
            throws Exception
    {
        assertTrue(queryState.isDone());
        assertTrue(queryState.isCanceled());
        assertFalse(queryState.isFailed());

        // getNextPages should return an empty list
        for (int loop = 0; loop < 5; loop++) {
            List<Page> nextPages = queryState.getNextPages(2);
            assertNotNull(nextPages);
            assertEquals(nextPages.size(), 0);
        }
    }

    private static long getPageOnlyValue(Page page)
    {
        BlockCursor cursor = page.getBlock(0).cursor();
        assertTrue(cursor.advanceNextPosition());
        long value = cursor.getLong(0);
        assertFalse(cursor.advanceNextPosition());
        return value;
    }
}
