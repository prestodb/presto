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
package com.facebook.presto.accumulo.io;

import javax.annotation.Nonnull;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BoundedPriorityBlockingQueue<E>
        extends AbstractQueue<E>
        implements BlockingQueue<E>
{
    private final PriorityQueue<E> queue;
    private final int maxCapacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    public BoundedPriorityBlockingQueue(int initialCapacity, Comparator<? super E> comparator, int maxCapacity)
    {
        queue = new PriorityQueue<>(initialCapacity, comparator);
        this.maxCapacity = maxCapacity;
    }

    @Nonnull
    @Override
    public Iterator<E> iterator()
    {
        lock.lock();
        try {
            return new PriorityQueue<>(queue).iterator();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public int size()
    {
        lock.lock();
        try {
            return queue.size();
        }
        finally {
            lock.unlock();
        }
    }

    public void put(E element)
            throws InterruptedException
    {
        lock.lockInterruptibly();
        try {
            try {
                while (queue.size() == maxCapacity) {
                    notFull.await();
                }
            }
            catch (InterruptedException e) {
                notFull.signal();
                throw e;
            }
            offer(element);
        }
        finally {
            lock.unlock();
        }
    }

    public boolean offer(E element, long timeout, @Nonnull TimeUnit unit)
            throws InterruptedException
    {
        lock.lockInterruptibly();
        try {
            if (queue.size() < maxCapacity) {
                return offer(element);
            }
            notEmpty.await(timeout, unit);
            return offer(element);
        }
        finally {
            lock.unlock();
        }
    }

    public E take()
            throws InterruptedException
    {
        lock.lockInterruptibly();
        try {
            try {
                while (queue.size() == 0) {
                    notEmpty.await();
                }
            }
            catch (InterruptedException e) {
                notEmpty.signal();
                throw e;
            }
            return poll();
        }
        finally {
            lock.unlock();
        }
    }

    public E poll(long timeout, @Nonnull TimeUnit unit)
            throws InterruptedException
    {
        lock.lockInterruptibly();
        try {
            E element = poll();
            if (element != null) {
                return element;
            }
            notEmpty.await(timeout, unit);
            return poll();
        }
        finally {
            lock.unlock();
        }
    }

    public int remainingCapacity()
    {
        lock.lock();
        try {
            return maxCapacity - queue.size();
        }
        finally {
            lock.unlock();
        }
    }

    public int drainTo(@Nonnull Collection<? super E> objects)
    {
        requireNonNull(objects, "objects is null");
        checkArgument(objects != this, "collection cannot be 'this'");

        lock.lock();
        try {
            int numAdded = 0;
            while (size() > 0) {
                objects.add(poll());
                ++numAdded;
            }
            return numAdded;
        }
        finally {
            lock.unlock();
        }
    }

    public int drainTo(@Nonnull Collection<? super E> objects, int maxElements)
    {
        requireNonNull(objects, "objects is null");
        checkArgument(objects != this, "collection cannot be 'this'");

        lock.lock();
        try {
            int numAdded = 0;
            while (size() > 0 && numAdded < maxElements) {
                objects.add(poll());
                ++numAdded;
            }
            return numAdded;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean offer(@Nonnull E element)
    {
        lock.lock();
        try {
            if (queue.size() == maxCapacity) {
                return false;
            }
            queue.offer(element);
            notEmpty.signal();
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    public E poll()
    {
        lock.lock();
        try {
            E element = queue.poll();
            if (element != null) {
                notFull.signal();
            }
            return element;
        }
        finally {
            lock.unlock();
        }
    }

    public E peek()
    {
        lock.lock();
        try {
            return queue.peek();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(Object o)
    {
        lock.lock();
        try {
            return queue.contains(o);
        }
        finally {
            lock.unlock();
        }
    }

    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull T[] ts)
    {
        lock.lock();
        try {
            return queue.toArray(ts);
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public String toString()
    {
        lock.lock();
        try {
            return queue.toString();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void clear()
    {
        lock.lock();
        try {
            queue.clear();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(Object o)
    {
        lock.lock();
        try {
            return queue.remove(o);
        }
        finally {
            lock.unlock();
        }
    }
}
