/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.operator.Page;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class PageBuffer
{
    public static enum BufferState
    {
        /**
         * Additional pages can be added to the buffer.
         */
        OPEN,
        /**
         * No more pages can be added to the buffer, and buffer is waiting for the buffered pages to be consumed.
         */
        FINISHING,
        /**
         * There are no pages in the buffer and no more pages can be added to the buffer.
         */
        FINISHED
    }

    private final String bufferId;
    private final BlockingQueue<Page> buffer;

    private AtomicReference<BufferState> state = new AtomicReference<>(BufferState.OPEN);

    public PageBuffer(String bufferId, int pageBufferMax)
    {
        this.bufferId = bufferId;
        buffer = new ArrayBlockingQueue<>(pageBufferMax);
    }

    public PageBufferInfo getBufferInfo()
    {
        return new PageBufferInfo(bufferId, state.get(), buffer.size());
    }

    /**
     * A buffer is finished when it has been closed and all pages have been consumed,
     * or the buffer is destroyed.
     */
    public boolean isFinished()
    {
        return state.get() == BufferState.FINISHED;
    }

    /**
     * Moves the buffer to the finishing state where no more pages can be added.
     */
    public void finish()
    {
        // transition to finishing
        state.compareAndSet(BufferState.OPEN, BufferState.FINISHING);

        // if buffer is empty transition to finished
        if (buffer.isEmpty()) {
            state.set(BufferState.FINISHED);

            // clear the buffer in case there was a failed race
            buffer.clear();
        }
    }

    /**
     * Discards all pages in the buffer and marks it as finished.
     */
    public void cancel()
    {
        state.set(BufferState.FINISHED);
        buffer.clear();
    }

    public void addPage(Page page)
            throws InterruptedException
    {
        // try to add the page while the buffer is open
        do {
            if (state.get() != BufferState.OPEN) {
                return;
            }
        } while (!buffer.offer(page, 1, SECONDS));


        // if the buffer was destroyed while pages were being added, clean up the buffers to be safe
        if (isFinished()) {
            buffer.clear();
        }
    }

    /**
     * Gets the next pages from the buffer.  The caller will block until at least one page is available, the
     * buffer is destroyed, or the max wait period elapses.
     *
     * @return between 0 and {@code maxPageCount} pages
     * @throws InterruptedException if the thread is interrupted while waiting for pages to be buffered
     */
    public List<Page> getNextPages(int maxPageCount, Duration maxWait)
            throws InterruptedException
    {
        Preconditions.checkArgument(maxPageCount > 0, "maxPageCount must be at least 1");
        Preconditions.checkNotNull(maxWait, "maxWait is null");

        List<Page> pages = new ArrayList<>(maxPageCount);
        if (!isFinished()){
            // wait for a single page
            Page page = buffer.poll((long) maxWait.convertTo(NANOSECONDS), NANOSECONDS);
            if (page != null) {
                pages.add(page);

                // fill the output list with the immediately available pages
                buffer.drainTo(pages, maxPageCount - 1);

                // mark the buffer as finished if it is not open and we consumed all of the pages
                if (state.get() == BufferState.FINISHING && buffer.isEmpty()) {
                    state.set(BufferState.FINISHED);
                }
            }
        }
        return ImmutableList.copyOf(pages);
    }

    public static Function<PageBuffer, BufferState> stateGetter()
    {
        return new Function<PageBuffer, BufferState>()
        {
            @Override
            public BufferState apply(PageBuffer pageBuffer)
            {
                return pageBuffer.state.get();
            }
        };
    }

    public static Function<PageBuffer, PageBufferInfo> infoGetter()
    {
        return new Function<PageBuffer, PageBufferInfo>()
        {
            @Override
            public PageBufferInfo apply(PageBuffer pageBuffer)
            {
                return pageBuffer.getBufferInfo();
            }
        };
    }
}
