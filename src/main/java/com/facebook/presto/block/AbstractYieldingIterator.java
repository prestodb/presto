/*
 * Copyright (C) 2007 The Guava Authors
 * Copyright 2004-present Facebook. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.block;

import com.google.common.collect.UnmodifiableIterator;

import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkState;

// NOTE: this was forked from Guava
public abstract class AbstractYieldingIterator<T extends TupleStream>
        extends UnmodifiableIterator<T>
        implements YieldingIterator<T>
{
    private State state = State.NOT_READY;

    /**
     * Constructor for use by subclasses.
     */
    protected AbstractYieldingIterator()
    {
    }

    private enum State
    {
        /**
         * We have computed the next element and haven't returned it yet.
         */
        READY,

        /**
         * We have attempted to compute next and the implementation has
         * indicated that the caller must yield before calling next.
         */
        MUST_YIELD,

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

    private T next;

    /**
     * Returns the next element. <b>Note:</b> the implementation must call {@link
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
    protected abstract T computeNext();

    /**
     * Implementations of {@link #computeNext} <b>must</b> invoke this method when
     * there are no elements left in the iteration.
     *
     * @return {@code null}; a convenience so your {@code computeNext}
     *         implementation can use the simple statement {@code return endOfData();}
     */
    protected final T setMustYield()
    {
        state = State.MUST_YIELD;
        return null;
    }

    /**
     * Implementations of {@link #computeNext} can invoke this method to notify caller
     * they must yield before calling next.
     *
     * @return {@code null}; a convenience so your {@code computeNext}
     *         implementation can use the simple statement {@code return endOfData();}
     */
    protected final T endOfData()
    {
        state = State.DONE;
        return null;
    }

    @Override
    public final boolean canAdvance()
    {
        checkState(state != State.FAILED);
        switch (state) {
            case DONE:
                return false;
            case READY:
                return true;
            default:
        }

        tryToComputeNext();
        return state == State.READY;
    }

    @Override
    public final boolean mustYield()
    {
        checkState(state != State.FAILED);
        switch (state) {
            case DONE:
                return false;
            case READY:
                return false;
            default:
        }

        tryToComputeNext();
        return state == State.MUST_YIELD;
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

        tryToComputeNext();
        checkState(state != State.MUST_YIELD, "Iterator must yield");
        return state == State.READY;
    }

    private void tryToComputeNext()
    {
        state = State.FAILED; // temporary pessimism
        next = computeNext();
        if (state != State.MUST_YIELD && state != State.DONE) {
            state = State.READY;
        }
    }

    @Override
    public final T next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        state = State.NOT_READY;
        return next;
    }

    /**
     * Returns the next element in the iteration without advancing the iteration,
     * according to the contract of {@link com.google.common.collect.PeekingIterator#peek()}.
     * <p/>
     * <p>Implementations of {@code AbstractIterator} that wish to expose this
     * functionality should implement {@code PeekingIterator}.
     */
    public final T peek()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return next;
    }
}
