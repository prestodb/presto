/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * QueryState contains the current state of the query and output buffer.
 */
@ThreadSafe
public class QueryState
{
    public static enum State
    {
        PREPARING,
        RUNNING,
        FINISHED,
        CANCELED,
        FAILED
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

    private final int splits;
    private final AtomicInteger startedSplits = new AtomicInteger();
    private final AtomicInteger completedSplits = new AtomicInteger();

    private final AtomicLong splitCpuMillis = new AtomicLong();

    private final AtomicLong inputDataSize = new AtomicLong();
    private final AtomicLong inputPositions = new AtomicLong();

    private final AtomicLong completedDataSize = new AtomicLong();
    private final AtomicLong completedPositions = new AtomicLong();

    private final Semaphore notFull;
    private final Semaphore notEmpty;

    public QueryState(List<TupleInfo> tupleInfos, int sourceCount, int pageBufferMax)
    {
        this(tupleInfos, sourceCount, pageBufferMax, 0);
    }

    public QueryState(List<TupleInfo> tupleInfos, int sourceCount, int pageBufferMax, int splits)
    {
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        Preconditions.checkArgument(sourceCount > 0, "sourceCount must be at least 1");
        Preconditions.checkArgument(pageBufferMax > 0, "pageBufferMax must be at least 1");

        this.tupleInfos = tupleInfos;
        this.sourceCount = sourceCount;
        this.pageBuffer = new ArrayDeque<>(pageBufferMax);
        this.splits = splits;
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
            return state == State.FINISHED || state == State.CANCELED || state == State.FAILED;
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

    public int getSplits()
    {
        return splits;
    }

    public int getStartedSplits()
    {
        return Ints.min(startedSplits.get(), completedSplits.get(), splits);
    }

    public int getCompletedSplits()
    {
        return Ints.min(completedSplits.get(), splits);
    }

    public int getBufferedPageCount()
    {
        synchronized (pageBuffer) {
            return pageBuffer.size();
        }
    }

    public Duration getSplitCpu()
    {
        return new Duration(splitCpuMillis.get(), TimeUnit.MILLISECONDS);
    }

    public DataSize getInputDataSize()
    {
        return new DataSize(inputDataSize.get(), Unit.BYTE);
    }

    public long getInputPositions()
    {
        return inputPositions.get();
    }

    public DataSize getCompletedDataSize()
    {
        return new DataSize(completedDataSize.get(), Unit.BYTE);
    }

    public long getCompletedPositions()
    {
        return completedPositions.get();
    }

    public void splitStarted()
    {
        startedSplits.incrementAndGet();
    }

    public void splitCompleted()
    {
        completedSplits.incrementAndGet();
    }

    public void addSplitCpuTime(Duration duration)
    {
        splitCpuMillis.addAndGet((long) duration.toMillis());
    }

    public void addInputPositions(long inputPositions)
    {
        this.inputPositions.addAndGet(inputPositions);
    }

    public void addInputDataSize(DataSize inputDataSize)
    {
        this.inputDataSize.addAndGet(inputDataSize.toBytes());
    }

    public void addCompletedPositions(long completedPositions)
    {
        this.completedPositions.addAndGet(completedPositions);
    }

    public void addCompletedDataSize(DataSize completedDataSize)
    {
        this.completedDataSize.addAndGet(completedDataSize.toBytes());
    }

    public QueryInfo toQueryInfo(String queryId)
    {
        return toQueryInfo(queryId, ImmutableMap.<String, List<QueryInfo>>of());
    }

    public QueryInfo toQueryInfo(String queryId, Map<String, List<QueryInfo>> stages)
    {
        return new QueryInfo(queryId,
                getTupleInfos(),
                getState(),
                getBufferedPageCount(),
                getSplits(),
                getStartedSplits(),
                getCompletedSplits(),
                (long) getSplitCpu().toMillis(),
                getInputDataSize().toBytes(),
                getInputPositions(),
                getCompletedDataSize().toBytes(),
                getCompletedPositions(),
                stages);
    }

    /**
     * Marks a source as finished and drop all buffered pages.  Once all sources are finished, no more pages can be added to the buffer.
     */
    public void cancel()
    {
        synchronized (pageBuffer) {
            if (isDone()) {
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
            state = State.RUNNING;
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
