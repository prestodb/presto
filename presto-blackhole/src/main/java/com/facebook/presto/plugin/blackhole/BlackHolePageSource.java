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
package com.facebook.presto.plugin.blackhole;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.Page;
import com.facebook.presto.spi.ConnectorPageSource;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.concurrent.CompletableFuture;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.concurrent.MoreFutures.toCompletableFuture;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class BlackHolePageSource
        implements ConnectorPageSource
{
    private final Page page;
    private int pagesLeft;
    private final ListeningScheduledExecutorService executorService;
    private final long pageProcessingDelayInMillis;
    private long completedBytes;
    private long completedPositions;
    private final long memoryUsageBytes;
    private boolean closed;
    private CompletableFuture<Page> currentPage;

    BlackHolePageSource(Page page, int count, ListeningScheduledExecutorService executorService, Duration pageProcessingDelay)
    {
        this.page = requireNonNull(page, "page is null");
        checkArgument(count >= 0, "count is negative");
        this.pagesLeft = count;
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.pageProcessingDelayInMillis = requireNonNull(pageProcessingDelay, "pageProcessingDelay is null").toMillis();
        this.memoryUsageBytes = page.getSizeInBytes();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }

        if (currentPage != null) {
            Page page = getFutureValue(currentPage);
            currentPage = null;
            return page;
        }

        pagesLeft--;
        completedBytes += page.getSizeInBytes();
        completedPositions += page.getPositionCount();

        if (pageProcessingDelayInMillis == 0) {
            return page;
        }
        else {
            currentPage = toCompletableFuture(executorService.schedule(() -> page, pageProcessingDelayInMillis, MILLISECONDS));
            return null;
        }
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        if (currentPage != null) {
            return currentPage;
        }
        return NOT_BLOCKED;
    }

    @Override
    public boolean isFinished()
    {
        return closed || (pagesLeft == 0 && currentPage == null);
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return memoryUsageBytes;
    }
}
