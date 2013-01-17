/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.GuardedBy;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The the page iterator from this operator will block until the gate is unlocked, or close is called.
 */
public class BlockingOperator
        implements Operator
{
    private final Operator delegate;
    private final Gate gate;

    public BlockingOperator(Operator delegate)
    {
        this(delegate, new Gate());
    }

    public BlockingOperator(Operator delegate, Gate gate)
    {
        this.delegate = delegate;
        this.gate = gate;
    }

    public Gate getGate()
    {
        return gate;
    }

    @Override
    public int getChannelCount()
    {
        return delegate.getChannelCount();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return delegate.getTupleInfos();
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return new BlockingPageIterator(delegate.iterator(operatorStats), gate);
    }

    private final class BlockingPageIterator
            extends AbstractPageIterator
    {

        private final PageIterator delegate;
        private final Gate gate;

        private BlockingPageIterator(PageIterator delegate, Gate gate)
        {
            super(delegate.getTupleInfos());
            this.delegate = delegate;
            this.gate = gate;
        }

        @Override
        protected Page computeNext()
        {
            // wait for the gate to be unlocked
            try {
                gate.awaitUnlock();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }

            if (!delegate.hasNext()) {
                return endOfData();
            }
            return delegate.next();
        }

        @Override
        protected void doClose()
        {
            delegate.close();
            gate.unlock();
        }
    }

    public static class Gate
    {
        private final Lock lock = new ReentrantLock();
        private final Condition waitForOpen = lock.newCondition();
        private final Condition waitForWaiters = lock.newCondition();

        @GuardedBy("lock")
        private boolean open;
        @GuardedBy("lock")
        private int waiters;

        public void lock()
        {
            lock.lock();
            try {
                open = false;
            }
            finally {
                lock.unlock();
            }
        }

        public void unlock()
        {
            lock.lock();
            try {
                open = true;
                waitForOpen.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        public void awaitUnlock()
                throws InterruptedException
        {
            lock.lock();
            try {
                waiters++;
                waitForWaiters.signalAll();
                while (!open) {
                    waitForOpen.await();
                }
            }
            finally {
                waiters--;
                lock.unlock();
            }
        }

        public void awaitWaiters(int expectedWaiters)
                throws InterruptedException
        {
            lock.lock();
            try {
                while (waiters < expectedWaiters) {
                    waitForWaiters.await();
                }
            }
            finally {
                lock.unlock();
            }
        }
    }
}
