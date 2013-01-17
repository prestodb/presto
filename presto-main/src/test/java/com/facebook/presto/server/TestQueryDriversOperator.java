/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.execution.PageBuffer;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.server.MockQueryManager.TUPLE_INFOS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestQueryDriversOperator
{
    @Test
    public void testNormalExecution()
            throws Exception
    {
        List<Page> pages = createPages();
        int expectedCount = 0;
        for (Page page : pages) {
            expectedCount += page.getPositionCount();
        }

        ExecutorService executor = Executors.newCachedThreadPool();
        try {

            QueryDriversOperator operator = new QueryDriversOperator(10,
                    TUPLE_INFOS,
                    new StaticQueryDriverProvider(executor, pages),
                    new StaticQueryDriverProvider(executor, pages),
                    new StaticQueryDriverProvider(executor, pages)
            );

            int count = 0;
            PageIterator iterator = operator.iterator(new OperatorStats());
            while (iterator.hasNext()) {
                Page page = iterator.next();
                BlockCursor cursor = page.getBlock(0).cursor();
                while (cursor.advanceNextPosition()) {
                    count++;
                }
            }
            assertEquals(count, expectedCount * 3);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test(enabled = false) // TODO fix this test
    public void testCancel()
            throws Exception
    {
        List<Page> pages = createPages();
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            StaticQueryDriverProvider provider = new StaticQueryDriverProvider(executor, pages);
            QueryDriversOperator operator = new QueryDriversOperator(1, TUPLE_INFOS, provider, provider, provider);

            int count = 0;
            PageIterator iterator = operator.iterator(new OperatorStats());
            while (count < 20 && iterator.hasNext()) {
                Page page = iterator.next();
                BlockCursor cursor = page.getBlock(0).cursor();
                while (count < 20 && cursor.advanceNextPosition()) {
                    count++;
                }
            }
            assertEquals(count, 20);

            // verify we have more data
            assertTrue(iterator.hasNext());

            // verify all producers are not finished
            IdentityHashMap<StaticQueryDriver, Integer> driverPagesAdded = new IdentityHashMap<>();
            for (StaticQueryDriver driver : provider.getCreatedDrivers()) {
                assertFalse(driver.isDone());
                driverPagesAdded.put(driver, driver.getPagesAdded());
            }

            // cancel the iterator
            iterator.close();

            // verify all producers are finished, and no additional pages have been added
            for (StaticQueryDriver driver : provider.getCreatedDrivers()) {
                assertTrue(driver.isDone());
                assertEquals(driver.getPagesAdded(), (int) driverPagesAdded.get(driver));
            }

            // drain any pages buffered in the iterator implementation
            while (iterator.hasNext()) {
                iterator.next();
            }
            assertFalse(iterator.hasNext());
        }
        finally {
            executor.shutdownNow();
        }
    }

    private List<Page> createPages()
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        List<String> data = ImmutableList.of("apple", "banana", "cherry", "date");
        for (int i = 0; i < 12; i++) {
            pages.add(new Page(createStringsBlock(Iterables.concat(Collections.nCopies(i + 1, data)))));
        }
        return pages.build();
    }

    private class StaticQueryDriverProvider implements QueryDriverProvider
    {
        private final ExecutorService executor;
        private final List<Page> pages;
        private final List<StaticQueryDriver> createdDrivers = new ArrayList<>();

        private StaticQueryDriverProvider(ExecutorService executor, List<Page> pages)
        {
            this.executor = executor;
            this.pages = pages;
        }

        public List<StaticQueryDriver> getCreatedDrivers()
        {
            return ImmutableList.copyOf(createdDrivers);
        }

        @Override
        public QueryDriver create(PageBuffer outputBuffer)
        {
            StaticQueryDriver driver = new StaticQueryDriver(executor, outputBuffer, pages);
            createdDrivers.add(driver);
            return driver;
        }
    }

    private class StaticQueryDriver implements QueryDriver
    {
        private final ExecutorService executor;
        private final PageBuffer outputBuffer;
        private final List<Page> pages;
        private Future<?> jobFuture;
        private AddPagesJob job;

        public StaticQueryDriver(ExecutorService executor, PageBuffer outputBuffer, List<Page> pages)
        {
            this.executor = executor;
            this.outputBuffer = outputBuffer;
            this.pages = ImmutableList.copyOf(pages);
        }

        public synchronized int getPagesAdded()
        {
            return job.getPagesAdded();
        }

        @Override
        public synchronized void start()
        {
            job = new AddPagesJob(outputBuffer, pages);
            jobFuture = executor.submit(job);
        }

        @Override
        public synchronized boolean isDone()
        {
            return jobFuture.isDone();
        }

        @Override
        public synchronized void abort()
        {
            jobFuture.cancel(true);
        }
    }

    private static class AddPagesJob implements Runnable
    {
        private final PageBuffer outputBuffer;
        private final List<Page> pages;
        private final AtomicInteger pagesAdded = new AtomicInteger();

        private AddPagesJob(PageBuffer outputBuffer, List<Page> pages)
        {
            this.outputBuffer = outputBuffer;
            this.pages = ImmutableList.copyOf(pages);
        }

        public int getPagesAdded()
        {
            return pagesAdded.get();
        }

        @Override
        public void run()
        {
            try {
                for (Page page : pages) {
                    try {
                        if (outputBuffer.addPage(page)) {
                            pagesAdded.incrementAndGet();
                        }
                    }
                    catch (InterruptedException e) {
                        outputBuffer.queryFailed(e);
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                }
            }
            finally {
                outputBuffer.sourceFinished();
            }
        }
    }
}
