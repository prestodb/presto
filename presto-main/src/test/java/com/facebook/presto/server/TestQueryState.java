/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.execution.FailedQueryException;
import com.facebook.presto.execution.PageBuffer;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.units.Duration;
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
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestQueryState
{
    private static final ImmutableList<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_LONG);

    private static final Duration MAX_WAIT = new Duration(1, TimeUnit.SECONDS);

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
            new PageBuffer("bufferId", TUPLE_INFOS, 0, 4);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {

        }
        try {
            new PageBuffer("bufferId", ImmutableList.of(SINGLE_LONG), 4, 0);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testNormalExecution()
            throws Exception
    {
        PageBuffer outputBuffer = new PageBuffer("bufferId", ImmutableList.of(SINGLE_LONG), 1, 20);
        assertRunning(outputBuffer);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            outputBuffer.addPage(createLongPage(i));
        }

        // verify pages are in correct order
        assertRunning(outputBuffer);

        List<Page> nextPages = outputBuffer.getNextPages(2, MAX_WAIT);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 0);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 1);

        assertRunning(outputBuffer);

        nextPages = outputBuffer.getNextPages(2, MAX_WAIT);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 2);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 3);

        assertRunning(outputBuffer);

        nextPages = outputBuffer.getNextPages(2, MAX_WAIT);
        assertEquals(nextPages.size(), 1);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 4);

        assertRunning(outputBuffer);

        // add one more page
        int value = 9;
        outputBuffer.addPage(createLongPage(value));

        // mark source as finished
        outputBuffer.sourceFinished();

        assertRunning(outputBuffer);

        // get the last page and assure the query is finished
        nextPages = outputBuffer.getNextPages(2, MAX_WAIT);
        assertEquals(nextPages.size(), 1);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 9);
        assertFinished(outputBuffer);

        // attempt to add more pages
        assertFalse(outputBuffer.addPage(createLongPage(22)));
        assertFinished(outputBuffer);

        // mark source as finished again
        outputBuffer.sourceFinished();
        assertFinished(outputBuffer);

        // try to fail the query and verify it doesn't work
        outputBuffer.queryFailed(new RuntimeException());
        assertFinished(outputBuffer);

        // try to finish the query and verify it doesn't work
        outputBuffer.finish();
        assertFinished(outputBuffer);
    }

    private Page createLongPage(int value)
    {
        return new Page(createLongsBlock(value));
    }

    @Test
    public void testFailedExecution()
            throws Exception
    {
        PageBuffer pageBuffer = new PageBuffer("bufferId", ImmutableList.of(SINGLE_LONG), 1, 20);
        assertRunning(pageBuffer);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            pageBuffer.addPage(createLongPage(i));
        }

        // verify pages are in correct order
        assertRunning(pageBuffer);

        List<Page> nextPages = pageBuffer.getNextPages(2, MAX_WAIT);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 0);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 1);

        assertRunning(pageBuffer);

        nextPages = pageBuffer.getNextPages(2, MAX_WAIT);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 2);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 3);

        assertRunning(pageBuffer);

        // Fail query with one page in the buffer
        RuntimeException exception = new RuntimeException("failed");
        pageBuffer.queryFailed(exception);
        assertFailed(pageBuffer, exception);

        // attempt to add more pages
        pageBuffer.addPage(createLongPage(22));
        assertFailed(pageBuffer, exception);

        // fail the query again
        RuntimeException anotherException = new RuntimeException("failed again");
        pageBuffer.queryFailed(anotherException);
        assertFailed(pageBuffer, exception, anotherException);

        // try to finish the finished query and verify it doesn't work
        pageBuffer.finish();
        assertFailed(pageBuffer, exception, anotherException);

        // try to finish the query again and verify it doesn't work
        pageBuffer.finish();
        assertFailed(pageBuffer, exception, anotherException);

        // try to finish the query and verify it doesn't work
        pageBuffer.sourceFinished();
        assertFailed(pageBuffer, exception, anotherException);
    }

    @Test
    public void testEarlyFinishExecution()
            throws Exception
    {
        PageBuffer pageBuffer = new PageBuffer("bufferId", TUPLE_INFOS, 1, 20);
        assertRunning(pageBuffer);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            pageBuffer.addPage(createLongPage(i));
        }

        // verify pages are in correct order
        assertRunning(pageBuffer);

        List<Page> nextPages = pageBuffer.getNextPages(2, MAX_WAIT);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 0);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 1);

        assertRunning(pageBuffer);

        nextPages = pageBuffer.getNextPages(2, MAX_WAIT);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 2);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 3);

        assertRunning(pageBuffer);

        // Finish query with one page in the buffer
        pageBuffer.finish();
        assertFinished(pageBuffer);

        // attempt to add more pages
        pageBuffer.addPage(createLongPage(22));
        assertFinished(pageBuffer);

        // finish the query again
        pageBuffer.finish();
        assertFinished(pageBuffer);

        // try to fail the finished query and verify it doesn't work
        pageBuffer.queryFailed(new RuntimeException());
        assertFinished(pageBuffer);

        // try to finish the query and verify it doesn't work
        pageBuffer.sourceFinished();
        assertFinished(pageBuffer);
    }

    @Test
    public void testMultiSourceNormalExecution()
            throws Exception
    {
        PageBuffer pageBuffer = new PageBuffer("bufferId", TUPLE_INFOS, 3, 20);
        assertRunning(pageBuffer);

        // add some pages
        pageBuffer.addPage(createLongPage(0));
        pageBuffer.addPage(createLongPage(1));

        // verify pages are in correct order
        assertRunning(pageBuffer);

        List<Page> nextPages = pageBuffer.getNextPages(2, MAX_WAIT);
        assertEquals(nextPages.size(), 2);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 0);
        assertEquals(getPageOnlyValue(nextPages.get(1)), 1);
        assertRunning(pageBuffer);

        // finish first sources
        pageBuffer.sourceFinished();
        assertRunning(pageBuffer);

        // add one more page
        pageBuffer.addPage(createLongPage(9));

        // finish second source
        pageBuffer.sourceFinished();
        assertRunning(pageBuffer);

        // the page
        nextPages = pageBuffer.getNextPages(2, MAX_WAIT);
        assertEquals(nextPages.size(), 1);
        assertEquals(getPageOnlyValue(nextPages.get(0)), 9);
        assertRunning(pageBuffer);

        // finish last source, and verify the query is finished since there are no buffered pages
        pageBuffer.sourceFinished();
        assertFinished(pageBuffer);

        // attempt to add more pages
        assertFalse(pageBuffer.addPage(createLongPage(22)));
        assertFinished(pageBuffer);

        // mark source as finished again
        pageBuffer.sourceFinished();
        assertFinished(pageBuffer);

        // try to fail the query and verify it doesn't work
        pageBuffer.queryFailed(new RuntimeException());
        assertFinished(pageBuffer);

        // try to finish the query and verify it doesn't work
        pageBuffer.finish();
        assertFinished(pageBuffer);
    }

    @Test
    public void testBufferSizeNormal()
            throws Exception
    {
        PageBuffer pageBuffer = new PageBuffer("bufferId", TUPLE_INFOS, 1, 5);
        assertRunning(pageBuffer);

        // exec thread to get two pages
        GetPagesJob getPagesJob = new GetPagesJob(pageBuffer, 2, 1);
        executor.submit(getPagesJob);
        getPagesJob.waitForStarted();

        // "verify" thread is blocked
        getPagesJob.assertBlockedWithCount(0);

        // add one page
        pageBuffer.addPage(createLongPage(0));

        // verify thread got one page and is blocked
        getPagesJob.assertBlockedWithCount(1);

        // add one page
        pageBuffer.addPage(createLongPage(1));

        // verify thread is released
        getPagesJob.waitForFinished();

        // verify thread got one page
        assertEquals(getPagesJob.getPages().size(), 2);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            pageBuffer.addPage(createLongPage(i));
        }

        // exec thread to add two more pages
        AddPagesJob addPagesJob = new AddPagesJob(pageBuffer, createLongPage(2), createLongPage(3));
        executor.submit(addPagesJob);
        addPagesJob.waitForStarted();

        // "verify" thread is blocked
        addPagesJob.assertBlockedWithCount(2);

        // get one page
        assertEquals(pageBuffer.getNextPages(1, MAX_WAIT).size(), 1);

        // "verify" thread is blocked again with one remaining page
        addPagesJob.assertBlockedWithCount(1);

        // get one page
        assertEquals(pageBuffer.getNextPages(1, MAX_WAIT).size(), 1);

        // verify thread is released
        addPagesJob.waitForFinished();

        // verify thread added one page
        assertEquals(addPagesJob.getPages().size(), 0);
    }

    @Test
    public void testFinishFreesReader()
            throws Exception
    {
        PageBuffer pageBuffer = new PageBuffer("bufferId", TUPLE_INFOS, 1, 5);
        assertRunning(pageBuffer);

        ExecutorService executor = Executors.newCachedThreadPool();

        // exec thread to get two pages
        GetPagesJob getPagesJob = new GetPagesJob(pageBuffer, 2, 1);
        executor.submit(getPagesJob);
        getPagesJob.waitForStarted();

        // "verify" thread is blocked
        getPagesJob.assertBlockedWithCount(0);

        // add one page
        pageBuffer.addPage(createLongPage(0));

        // verify thread got one page and is blocked
        getPagesJob.assertBlockedWithCount(1);

        // finish the query
        pageBuffer.finish();
        assertFinished(pageBuffer);

        // verify thread is released
        getPagesJob.waitForFinished();

        // verify thread only got one page
        assertEquals(getPagesJob.getPages().size(), 1);
    }

    @Test
    public void testFinishFreesWriter()
            throws Exception
    {
        PageBuffer pageBuffer = new PageBuffer("bufferId", TUPLE_INFOS, 1, 5);
        assertRunning(pageBuffer);

        ExecutorService executor = Executors.newCachedThreadPool();

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            pageBuffer.addPage(createLongPage(i));
        }

        // exec thread to add two pages
        AddPagesJob addPagesJob = new AddPagesJob(pageBuffer, createLongPage(2), createLongPage(3));
        executor.submit(addPagesJob);
        addPagesJob.waitForStarted();

        // "verify" thread is blocked
        addPagesJob.assertBlockedWithCount(2);

        // get one page
        assertEquals(pageBuffer.getNextPages(1, MAX_WAIT).size(), 1);

        // "verify" thread is blocked again with one remaining page
        addPagesJob.assertBlockedWithCount(1);

        // finish the query
        pageBuffer.finish();
        assertFinished(pageBuffer);

        // verify thread is released
        addPagesJob.waitForFinished();
    }

    @Test
    public void testFailFreesReader()
            throws Exception
    {
        PageBuffer pageBuffer = new PageBuffer("bufferId", TUPLE_INFOS, 1, 5);
        assertRunning(pageBuffer);

        ExecutorService executor = Executors.newCachedThreadPool();

        // exec thread to get two pages
        GetPagesJob getPagesJob = new GetPagesJob(pageBuffer, 2, 1);
        executor.submit(getPagesJob);
        getPagesJob.waitForStarted();

        // "verify" thread is blocked
        getPagesJob.assertBlockedWithCount(0);

        // add one page
        pageBuffer.addPage(createLongPage(0));

        // verify thread got one page and is blocked
        getPagesJob.assertBlockedWithCount(1);

        // fail the query
        RuntimeException exception = new RuntimeException("failed");
        pageBuffer.queryFailed(exception);
        assertFailed(pageBuffer, exception);

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
        PageBuffer pageBuffer = new PageBuffer("bufferId", TUPLE_INFOS, 1, 5);
        assertRunning(pageBuffer);

        ExecutorService executor = Executors.newCachedThreadPool();

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            pageBuffer.addPage(createLongPage(i));
        }

        // exec thread to add two page
        AddPagesJob addPagesJob = new AddPagesJob(pageBuffer, createLongPage(2), createLongPage(3));
        executor.submit(addPagesJob);
        addPagesJob.waitForStarted();

        // "verify" thread is blocked
        addPagesJob.assertBlockedWithCount(2);

        // get one page
        assertEquals(pageBuffer.getNextPages(1, MAX_WAIT).size(), 1);

        // "verify" thread is blocked again with one remaining page
        addPagesJob.assertBlockedWithCount(1);

        // fail the query
        RuntimeException exception = new RuntimeException("failed");
        pageBuffer.queryFailed(exception);
        assertFailed(pageBuffer, exception);

        // verify thread is released
        addPagesJob.waitForFinished();
    }

    private static class GetPagesJob implements Runnable
    {
        private final PageBuffer pageBuffer;
        private final int pagesToGet;
        private final int batchSize;

        private final AtomicReference<FailedQueryException> failedQueryException = new AtomicReference<>();

        private final CopyOnWriteArrayList<Page> pages = new CopyOnWriteArrayList<>();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);

        private GetPagesJob(PageBuffer pageBuffer, int pagesToGet, int batchSize)
        {
            this.pageBuffer = pageBuffer;
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
                        List<Page> pages = pageBuffer.getNextPages(batchSize, MAX_WAIT);
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
        private final PageBuffer pageBuffer;
        private final ArrayBlockingQueue<Page> pages;

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);

        private AddPagesJob(PageBuffer pageBuffer, Page... pages)
        {
            this.pageBuffer = pageBuffer;
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
                        pageBuffer.addPage(page);
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

    private void assertRunning(PageBuffer pageBuffer)
    {
        assertFalse(pageBuffer.isDone());
        assertFalse(pageBuffer.isFailed());
    }

    private void assertFinished(PageBuffer pageBuffer)
            throws Exception
    {
        assertTrue(pageBuffer.isDone());
        assertFalse(pageBuffer.isFailed());

        // getNextPages should return an empty list
        for (int loop = 0; loop < 5; loop++) {
            List<Page> nextPages = pageBuffer.getNextPages(2, MAX_WAIT);
            assertNotNull(nextPages);
            assertEquals(nextPages.size(), 0);
        }
    }

    private void assertFailed(PageBuffer pageBuffer, Throwable... expectedCauses)
            throws Exception
    {
        assertTrue(pageBuffer.isDone());
        assertTrue(pageBuffer.isFailed());

        // getNextPages should throw an exception
        for (int loop = 0; loop < 5; loop++) {
            try {
                pageBuffer.getNextPages(2, MAX_WAIT);
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

    private static long getPageOnlyValue(Page page)
    {
        BlockCursor cursor = page.getBlock(0).cursor();
        assertTrue(cursor.advanceNextPosition());
        long value = cursor.getLong(0);
        assertFalse(cursor.advanceNextPosition());
        return value;
    }
}
