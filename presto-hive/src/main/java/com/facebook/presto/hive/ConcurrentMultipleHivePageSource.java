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
package com.facebook.presto.hive;

import com.facebook.presto.common.Page;
import com.facebook.presto.spi.ConnectorPageSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;

public class ConcurrentMultipleHivePageSource
        implements ConnectorPageSource
{
    private final List<ConnectorPageSource> delegates;
    private final LinkedBlockingQueue<Optional<Page>> resultQueue;
    private final ExecutorService executorService;
    private int currentPageIndex;

    public ConcurrentMultipleHivePageSource(
            List<ConnectorPageSource> delegates,
            int parallelism)
    {
        this.delegates = requireNonNull(delegates, "delegate is null");
        resultQueue = new LinkedBlockingQueue<>(parallelism * 5);
        executorService = Executors.newFixedThreadPool(parallelism, daemonThreadsNamed("concurrent-small-file-read-%s"));
        startFetchPages(parallelism);
    }

    @Override
    public long getCompletedBytes()
    {
        return delegates.stream().mapToLong(ConnectorPageSource::getCompletedBytes).sum();
    }

    @Override
    public long getCompletedPositions()
    {
        return delegates.stream().mapToLong(ConnectorPageSource::getCompletedPositions).sum();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegates.stream().mapToLong(ConnectorPageSource::getReadTimeNanos).sum();
    }

    @Override
    public boolean isFinished()
    {
        return delegates.stream().allMatch(ConnectorPageSource::isFinished) && resultQueue.isEmpty();
    }

    @Override
    public Page getNextPage()
    {
        Optional<Page> dataPage = Optional.empty();
        if (isFinished()) {
            return null;
        }
        try {
            dataPage = resultQueue.take();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        return dataPage == null ? null : dataPage.orElseGet(() -> null);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegates.stream().mapToLong(ConnectorPageSource::getSystemMemoryUsage).sum();
    }

    @Override
    public void close() throws IOException
    {
        try {
            for (ConnectorPageSource delegate : delegates) {
                delegate.close();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            executorService.shutdown();
        }
    }

    public synchronized void startFetchPages(int num)
    {
        while (currentPageIndex < delegates.size() && num-- > 0) {
            executorService.submit(new FetchPagesTask(delegates.get(currentPageIndex++)));
        }
    }

    private class FetchPagesTask
            implements Runnable
    {
        private ConnectorPageSource delegate;

        public FetchPagesTask(ConnectorPageSource deledage)
        {
            this.delegate = deledage;
        }

        @Override
        public void run()
        {
            try {
                while (!delegate.isFinished()) {
                    Page dataPage = delegate.getNextPage();
                    if (dataPage != null) {
                        dataPage = dataPage.getLoadedPage();
                    }
                    resultQueue.put(dataPage == null ? Optional.empty() : Optional.of(dataPage));
                }
                if (delegate.isFinished()) {
                    synchronized (ConcurrentMultipleHivePageSource.this) {
                        if (currentPageIndex < delegates.size()) {
                            executorService.submit(new FetchPagesTask(delegates.get(currentPageIndex++)));
                        }
                    }
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
