/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * QueryState contains the current state of the query and output buffer.
 */
@ThreadSafe
public class QueryState
{
    private static enum State
    {
        RUNNING,
        FINISHED,
        CANCELED,
        FAILED
    }

    @GuardedBy("pageBuffer")
    private final ArrayDeque<Page> pageBuffer;

    @GuardedBy("pageBuffer")
    private State state = State.RUNNING;

    @GuardedBy("pageBuffer")
    private final List<Throwable> causes = new ArrayList<>();

    @GuardedBy("pageBuffer")
    private int sourceCount;

    private final Semaphore notFull;
    private final Semaphore notEmpty;

    public QueryState(int sourceCount, int pageBufferMax)
    {
        Preconditions.checkArgument(sourceCount > 0, "sourceCount must be at least 1");
        Preconditions.checkArgument(pageBufferMax > 0, "pageBufferMax must be at least 1");

        this.sourceCount = sourceCount;
        this.pageBuffer = new ArrayDeque<>(pageBufferMax);
        this.notFull = new Semaphore(pageBufferMax);
        this.notEmpty = new Semaphore(0);
    }

    public boolean isDone()
    {
        synchronized (pageBuffer) {
            return state != State.RUNNING;
        }
    }

    public boolean isFailed()
    {
        synchronized (pageBuffer) {
            return state == State.FAILED;
        }
    }

    public boolean isCanceled()
    {
        synchronized (pageBuffer) {
            return state == State.CANCELED;
        }
    }

    /**
     * Marks a source as finished and drop all buffered pages.  Once all sources are finished, no more pages can be added to the buffer.
     */
    public void cancel()
    {
        synchronized (pageBuffer) {
            if (state != State.RUNNING) {
                return;
            }

            state = State.CANCELED;
            sourceCount = 0;
            pageBuffer.clear();
            // free up threads quickly
            notEmpty.release();
            notFull.release();
        }
    }

    /**
     * Marks a source as finished.  Once all sources are finished, no more pages can be added to the buffer.
     */
    public void sourceFinished()
    {
        synchronized (pageBuffer) {
            if (state != State.RUNNING) {
                return;
            }
            sourceCount--;
            if (sourceCount == 0) {
                if (pageBuffer.isEmpty()) {
                    state = State.FINISHED;
                }
                // free up threads quickly
                notEmpty.release();
                notFull.release();
            }
        }
    }

    /**
     * Marks the query as failed and finished.  Once the query if failed, no more pages can be added to the buffer.
     */
    public void queryFailed(Throwable cause)
    {
        synchronized (pageBuffer) {
            // if query is already done, nothing can be done here
            if (state == State.CANCELED || state == State.FINISHED) {
                causes.add(cause);
                return;
            }
            state = State.FAILED;
            causes.add(cause);
            sourceCount = 0;
            pageBuffer.clear();
            // free up threads quickly
            notEmpty.release();
            notFull.release();
        }
    }

    /**
     * Add a page to the buffer.  The buffers space is limited, so the caller will be blocked until
     * space is available in the buffer.
     *
     * @return true if the page was added; false if the query has already been canceled or failed
     * @throws InterruptedException if the thread is interrupted while waiting for buffer space to be freed
     * @throws IllegalStateException if the query is finished
     */
    public boolean addPage(Page page)
            throws InterruptedException
    {
        // acquire write permit
        while (!isDone() && !notFull.tryAcquire(1, TimeUnit.SECONDS)) {
        }

        synchronized (pageBuffer) {
            // don't throw an exception if the query was canceled or failed as the caller may not be aware of this
            if (state == State.CANCELED || state == State.FAILED) {
                // release an additional thread blocked in the code above
                // all blocked threads will be release due to the chain reaction
                notFull.release();
                return false;
            }

            // if all sources are finished throw an exception
            if (sourceCount == 0) {
                // release an additional thread blocked in the code above
                // all blocked threads will be release due to the chain reaction
                notFull.release();
                throw new IllegalStateException("All sources are finished");
            }
            pageBuffer.addLast(page);
            notEmpty.release();
        }
        return true;
    }

    /**
     * Gets the next pages from the buffer.  The caller will page until at least one page is available, the
     * query is canceled, or the query fails.
     *
     * @return one to masPageCount pages if the query is not done; no page if the query is done
     * @throws FailedQueryException if the query failed
     * @throws InterruptedException if the thread is interrupted while waiting for pages to be buffered
     */
    public List<Page> getNextPages(int maxPageCount)
            throws InterruptedException
    {
        Preconditions.checkArgument(maxPageCount > 0, "pageBufferMax must be at least 1");

        // block until first page is available
        while (!isDone() && !notEmpty.tryAcquire(1, TimeUnit.SECONDS)) {
        }

        synchronized (pageBuffer) {
            // verify state
            if (state == State.CANCELED || state == State.FINISHED) {
                // release an additional thread blocked in the code above
                // all blocked threads will be release due to the chain reaction
                notEmpty.release();
                return ImmutableList.of();
            }
            if (state == State.FAILED) {
                // release an additional thread blocked in the code above
                // all blocked threads will be release due to the chain reaction
                notEmpty.release();
                // todo remove this when airlift log prints suppressed exceptions
                FailedQueryException failedQueryException = new FailedQueryException(causes);
                failedQueryException.printStackTrace(System.err);
                throw failedQueryException;
            }

            // acquire all available pages up to the limit
            ImmutableList.Builder<Page> nextPages = ImmutableList.builder();
            int count = 0;
            // use a do while because we have "reserved" a page in the above acquire call
            do {
                if (pageBuffer.isEmpty()) {
                    // there is one extra permit when all sources are finished
                    Preconditions.checkState(sourceCount == 0, "A read permit was acquired but no pages are available");

                    // release an additional thread blocked in the code above
                    // all blocked threads will be release due to the chain reaction
                    notEmpty.release();
                    break;
                }
                else {
                    nextPages.add(pageBuffer.removeFirst());
                }
                count++;
                // tryAcquire can fail even if more pages are available, because the pages may have been "reserved" by the above acquire call
            } while (count < maxPageCount && notEmpty.tryAcquire());

            // allow pages to be replaced
            notFull.release(count);

            // check for end condition
            List<Page> pages = nextPages.build();
            if (sourceCount == 0 && pageBuffer.isEmpty()) {
                state = State.FINISHED;
            }

            return pages;
        }
    }
}
