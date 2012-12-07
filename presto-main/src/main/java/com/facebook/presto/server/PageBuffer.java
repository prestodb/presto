/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

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
public class PageBuffer
{
    private static final Logger log = Logger.get(PageBuffer.class);

    public static enum State
    {
        PREPARING(false),
        RUNNING(false),
        FINISHED(true),
        CANCELED(true),
        FAILED(true);

        private final boolean doneState;

        private State(boolean doneState)
        {
            this.doneState = doneState;
        }

        public boolean isDone() {
            return doneState;
        }

        public static Predicate<State> inDoneState()
        {
            return new Predicate<State>()
            {
                @Override
                public boolean apply(State state)
                {
                    return state.isDone();
                }
            };
        }
    }

    private final List<TupleInfo> tupleInfos;

    @GuardedBy("pageBuffer")
    private final ArrayDeque<Page> pageBuffer;

    @GuardedBy("pageBuffer")
    private State state = State.PREPARING;

    @GuardedBy("pageBuffer")
    private final List<Throwable> causes = new ArrayList<>();

    @GuardedBy("pageBuffer")
    private int sourceCount;

    private final Semaphore notFull;
    private final Semaphore notEmpty;

    public PageBuffer(List<TupleInfo> tupleInfos, int sourceCount, int pageBufferMax)
    {
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        Preconditions.checkArgument(sourceCount > 0, "sourceCount must be at least 1");
        Preconditions.checkArgument(pageBufferMax > 0, "pageBufferMax must be at least 1");

        this.tupleInfos = tupleInfos;
        this.sourceCount = sourceCount;
        this.pageBuffer = new ArrayDeque<>(pageBufferMax);
        this.notFull = new Semaphore(pageBufferMax);
        this.notEmpty = new Semaphore(0);
    }

    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    public State getState()
    {
        synchronized (pageBuffer) {
            return state;
        }
    }

    public boolean isDone()
    {
        synchronized (pageBuffer) {
            return state.isDone();
        }
    }

    public boolean isFailed()
    {
        synchronized (pageBuffer) {
            return state == State.FAILED;
        }
    }

    public int getBufferedPageCount()
    {
        synchronized (pageBuffer) {
            return pageBuffer.size();
        }
    }

    /**
     * Marks a source as finished and drop all buffered pages.  Once all sources are finished, no more pages can be added to the buffer.
     */
    public void finish()
    {
        synchronized (pageBuffer) {
            if (isDone()) {
                return;
            }

            state = State.FINISHED;
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
            if (isDone()) {
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
        log.error(cause, "Query buffer failed");
        synchronized (pageBuffer) {
            // if query is already done, nothing can be done here
            if (isDone()) {
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
     */
    public boolean addPage(Page page)
            throws InterruptedException
    {
        // acquire write permit
        while (!isDone() && !notFull.tryAcquire(1, TimeUnit.SECONDS)) {
        }

        synchronized (pageBuffer) {
            // don't throw an exception if the query was canceled or failed as the caller may not be aware of this
            if (state.isDone()) {
                // release an additional thread blocked in the code above
                // all blocked threads will be release due to the chain reaction
                notFull.release();
                return false;
            }

            state = State.RUNNING;
            pageBuffer.addLast(page);
            notEmpty.release();
        }
        return true;
    }

    /**
     * Gets the next pages from the buffer.  The caller will block until at least one page is available, the
     * query is canceled, the query fails or the max wait period elapses.
     *
     * @return between 0 and {@code maxPageCount} pages if the query is not done
     * @throws FailedQueryException if the query failed
     * @throws InterruptedException if the thread is interrupted while waiting for pages to be buffered
     */
    public List<Page> getNextPages(int maxPageCount, Duration maxWait)
            throws InterruptedException
    {
        Preconditions.checkArgument(maxPageCount > 0, "pageBufferMax must be at least 1");

        long start = System.nanoTime();
        long maxWaitNanos = (long) maxWait.convertTo(TimeUnit.NANOSECONDS);

        // block until first page is available
        while (!isDone() && !notEmpty.tryAcquire(1, TimeUnit.SECONDS)) {
            if (System.nanoTime() - start  > maxWaitNanos) {
                return ImmutableList.of();
            }
        }

        synchronized (pageBuffer) {
            if (state == State.FAILED) {
                // release an additional thread blocked in the code above
                // all blocked threads will be release due to the chain reaction
                notEmpty.release();
                // todo remove this when airlift log prints suppressed exceptions
                FailedQueryException failedQueryException = new FailedQueryException(causes);
                failedQueryException.printStackTrace(System.err);
                throw failedQueryException;
            }

            // verify state
            if (state.isDone()) {
                // release an additional thread blocked in the code above
                // all blocked threads will be release due to the chain reaction
                notEmpty.release();
                return ImmutableList.of();
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

    public static Function<PageBuffer, State> stateGetter()
    {
        return new Function<PageBuffer, State>()
        {
            @Override
            public State apply(PageBuffer pageBuffer)
            {
                return pageBuffer.getState();
            }
        };
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("state", state)
                .add("pageBuffer", pageBuffer.size())
                .add("sourceCount", sourceCount)
                .add("causes", causes)
                .add("notFull", notFull.availablePermits())
                .add("notEmpty", notEmpty.availablePermits())
                .add("tupleInfos", tupleInfos)
                .toString();
    }
}
