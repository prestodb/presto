/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkState;

//
// This code was forked from Guava
//
public abstract class AbstractPageIterator
        implements PageIterator
{
    private final Logger log = Logger.get(this.getClass());

    private enum State
    {
        /**
         * We have computed the next element and haven't returned it yet.
         */
        READY,

        /**
         * We haven't yet computed or have already returned the element.
         */
        NOT_READY,

        /**
         * We have reached the end of the data and are finished.
         */
        DONE,

        /**
         * We've suffered an exception and are kaput.
         */
        FAILED,
    }

    private final List<TupleInfo> tupleInfos;
    private State state = State.NOT_READY;
    private Page next;

    protected AbstractPageIterator(Iterable<TupleInfo> tupleInfos)
    {
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        this.tupleInfos = ImmutableList.copyOf(tupleInfos);
    }

    @Override
    public final int getChannelCount()
    {
        return tupleInfos.size();
    }

    @Override
    public final List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public final void close()
    {
        endOfData();
    }

    protected abstract void doClose();


    /**
     * Returns the next page. <b>Note:</b> the implementation must call {@link
     * #endOfData()} when there are no elements left in the iteration. Failure to
     * do so could result in an infinite loop.
     * <p/>
     * <p>The initial invocation of {@link #hasNext()} or {@link #next()} calls
     * this method, as does the first invocation of {@code hasNext} or {@code
     * next} following each successful call to {@code next}. Once the
     * implementation either invokes {@code endOfData} or throws an exception,
     * {@code computeNext} is guaranteed to never be called again.
     * <p/>
     * <p>If this method throws an exception, it will propagate outward to the
     * {@code hasNext} or {@code next} invocation that invoked this method. Any
     * further attempts to use the iterator will result in an {@link
     * IllegalStateException}.
     * <p/>
     * <p>The implementation of this method may not invoke the {@code hasNext},
     * {@code next}, or {@link #peek()} methods on this instance; if it does, an
     * {@code IllegalStateException} will result.
     *
     * @return the next element if there was one. If {@code endOfData} was called
     *         during execution, the return value will be ignored.
     * @throws RuntimeException if any unrecoverable error happens. This exception
     * will propagate outward to the {@code hasNext()}, {@code next()}, or
     * {@code peek()} invocation that invoked this method. Any further
     * attempts to use the iterator will result in an
     * {@link IllegalStateException}.
     */
    protected abstract Page computeNext();

    /**
     * Implementations of {@link #computeNext} <b>must</b> invoke this method when
     * there are no elements left in the iteration.
     *
     * @return {@code null}; a convenience so your {@code computeNext}
     *         implementation can use the simple statement {@code return endOfData();}
     */
    protected final Page endOfData()
    {
        state = State.DONE;
        doClose();
        return null;
    }

    @Override
    public final boolean hasNext()
    {
        checkState(state != State.FAILED);
        switch (state) {
            case DONE:
                return false;
            case READY:
                return true;
            default:
        }
        return tryToComputeNext();
    }

    private boolean tryToComputeNext()
    {
        state = State.FAILED; // temporary pessimism
        boolean sawThrowable = false;
        try {
            next = computeNext();
            if (state != State.DONE) {
                state = State.READY;
                Preconditions.checkNotNull(next, "next page is null");
                return true;
            }
            return false;
        }
        catch (Throwable t) {
            sawThrowable = true;
            throw t;
        }
        finally {
            if (state != State.READY) {
                try {
                    doClose();
                }
                catch (Throwable t) {
                    if (!sawThrowable) {
                        throw t;
                    }
                    else {
                        log.warn(t, "While closing iterator");
                    }
                }
            }
        }
    }

    @Override
    public final Page next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        state = State.NOT_READY;
        return next;
    }

    /**
     * Returns the next page in the iteration without advancing the iteration,
     * according to the contract of {@link com.google.common.collect.PeekingIterator#peek()}.
     * <p/>
     * <p>Implementations of {@code AbstractIterator} that wish to expose this
     * functionality should implement {@code PeekingIterator}.
     */
    public final Page peek()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return next;
    }

    @Override
    public final void remove()
    {
        throw new UnsupportedOperationException();
    }
}
