/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class SharedBuffer<T>
{
    public static enum QueueState
    {
        /**
         * Additional queues can be added.
         */
        OPEN,
        /**
         * No more queues can be added.
         */
        NO_MORE_QUEUES,
        /**
         * No more queues can be added and all elements have been consumed.
         */
        FINISHED
    }

    @GuardedBy("this")
    private final LinkedList<T> masterQueue;
    @GuardedBy("this")
    private long masterSequenceId;
    @GuardedBy("this")
    private Map<String, NamedQueue> namedQueues = new HashMap<>();
    @GuardedBy("this")
    private final SortedSet<NamedQueue> openQueuesBySequenceId = new TreeSet<>();
    @GuardedBy("this")
    private QueueState state = QueueState.OPEN;

    /**
     * If true, no more elements can be added to the queue.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    private final Semaphore notFull;

    public SharedBuffer(int capacity)
    {
        Preconditions.checkArgument(capacity > 0, "Capacity must be at least 1");
        masterQueue = new LinkedList<>();
        notFull = new Semaphore(capacity);
    }

    public synchronized boolean isFinished()
    {
        return state == QueueState.FINISHED;
    }

    public synchronized SharedBufferInfo getInfo()
    {
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (NamedQueue namedQueue : namedQueues.values()) {
            infos.add(new BufferInfo(namedQueue.getQueueId(), namedQueue.isFinished(), namedQueue.size()));
        }
        return new SharedBufferInfo(state, infos.build());
    }

    public synchronized void addQueue(String queueId)
    {
        Preconditions.checkNotNull(queueId, "queueId is null");
        Preconditions.checkArgument(!namedQueues.containsKey(queueId), "Queue %s already exists", queueId);
        Preconditions.checkState(state == QueueState.OPEN, "%s is not OPEN", SharedBuffer.class.getSimpleName());
        NamedQueue namedQueue = new NamedQueue(queueId);
        namedQueues.put(queueId, namedQueue);
        openQueuesBySequenceId.add(namedQueue);
    }

    public synchronized void noMoreQueues()
    {
        namedQueues = ImmutableMap.copyOf(namedQueues);
        if (state != QueueState.OPEN) {
            return;
        }

        state = QueueState.NO_MORE_QUEUES;

        updateState();
    }

    public boolean add(T element)
            throws InterruptedException
    {
        Preconditions.checkNotNull(element, "element is null");

        // acquire write permit
        while (!closed.get() && !notFull.tryAcquire(1, TimeUnit.SECONDS)) {
        }

        return addInternal(element);
    }

    public boolean offer(T element)
            throws InterruptedException
    {
        Preconditions.checkNotNull(element, "element is null");

        if (closed.get()) {
            return false;
        }

        // acquire write permit
        if (!notFull.tryAcquire()) {
            return false;
        }

        return addInternal(element);
    }

    private synchronized boolean addInternal(T element)
    {
        // don't throw an exception if the queue was closed as the caller may not be aware of this
        if (closed.get()) {
            // release an additional thread blocked in the code above
            // all blocked threads will be release due to the chain reaction
            notFull.release();
            return false;
        }

        // add element
        masterQueue.add(element);

        // notify consumers an element has arrived
        this.notifyAll();

        return true;
    }

    public synchronized List<T> get(String outputId, int maxCount, Duration maxWait)
            throws InterruptedException
    {
        Preconditions.checkNotNull(outputId, "outputId is null");
        Preconditions.checkNotNull(maxWait, "maxWait is null");

        NamedQueue namedQueue = namedQueues.get(outputId);
        if (namedQueue == null) {
            throw new NoSuchBufferException(outputId, namedQueues.keySet());
        }

        if (state == QueueState.FINISHED) {
            return ImmutableList.of();
        }

        // wait for elements to arrive
        if (namedQueue.isEmpty()) {
            long remainingNanos = (long) maxWait.convertTo(NANOSECONDS);
            long end = System.nanoTime() + remainingNanos;
            while (remainingNanos > 0 && namedQueue.isEmpty()) {
                // wait for timeout or notification
                NANOSECONDS.timedWait(this, remainingNanos);
                remainingNanos = end - System.nanoTime();
            }
        }

        if (state == QueueState.FINISHED) {
            return ImmutableList.of();
        }

        // remove queue from set before calling getElements because getElements changes
        // the sequence number of the queue which is used for identity comparison in the
        // sorted set
        openQueuesBySequenceId.remove(namedQueue);

        // get the elements
        List<T> elements = namedQueue.getElements(maxCount);

        // only add back the queue if it is still open
        if (!closed.get() || !namedQueue.isEmpty()) {
            openQueuesBySequenceId.add(namedQueue);
        } else {
            namedQueue.setFinished();
        }

        updateState();

        return elements;
    }

    public synchronized void abort(String outputId)
    {
        Preconditions.checkNotNull(outputId, "outputId is null");
        NamedQueue namedQueue = namedQueues.get(outputId);
        if (namedQueue == null) {
            return;
        }
        namedQueue.setFinished();
        openQueuesBySequenceId.remove(namedQueue);

        // notify readers that they may have been aborted
        this.notifyAll();

        updateState();
    }

    private synchronized void updateState()
    {
        if (closed.get()) {
            // remove all empty queues
            for (Iterator<NamedQueue> iterator = openQueuesBySequenceId.iterator(); iterator.hasNext(); ) {
                NamedQueue namedQueue = iterator.next();
                if (namedQueue.isEmpty()) {
                    namedQueue.setFinished();
                    iterator.remove();
                }
            }
        }

        if (state == QueueState.NO_MORE_QUEUES && !openQueuesBySequenceId.isEmpty()) {
            // advance master sequence id
            long oldMasterSequenceId = masterSequenceId;
            masterSequenceId = openQueuesBySequenceId.iterator().next().getSequenceId();

            // drop consumed elements
            int elementsToRemove = Ints.checkedCast(masterSequenceId - oldMasterSequenceId);
            Preconditions.checkState(elementsToRemove >= 0,
                    "Master sequence id moved backwards: oldMasterSequenceId=%s, newMasterSequenceId=%s",
                    oldMasterSequenceId,
                    masterSequenceId);
            for (int i = 0; i < elementsToRemove; i++) {
                masterQueue.removeFirst();
            }
            notFull.release(elementsToRemove);
        }

        if (state == QueueState.NO_MORE_QUEUES && closed.get() && openQueuesBySequenceId.isEmpty()) {
            state = QueueState.FINISHED;
        }
    }

    /**
     * Marks the output as complete.  After this method is called no more data can be added but there may still be buffered output pages.
     */
    public synchronized void finish()
    {
        closed.set(true);

        // notify readers that there may be no more data
        this.notifyAll();

        // the output will only transition to finished if it isn't already marked as failed or cancel
        updateState();
    }

    /**
     * Destroys the queue, discarding all pages.
     */
    public synchronized void destroy()
    {
        closed.set(true);
        state = QueueState.FINISHED;
        for (NamedQueue namedQueue : openQueuesBySequenceId) {
            namedQueue.setFinished();
        }
        openQueuesBySequenceId.clear();

        // wake up any blocked writers
        notFull.release(masterQueue.size());

        // clear the buffer
        masterQueue.clear();

        // notify readers that the buffer has been destroyed
        this.notifyAll();
    }

    @NotThreadSafe
    private class NamedQueue
            implements Comparable<NamedQueue>
    {
        private final String queueId;

        private long sequenceId;
        private boolean finished;

        private NamedQueue(String queueId)
        {
            this.queueId = queueId;
        }

        public String getQueueId()
        {
            return queueId;
        }

        public boolean isFinished()
        {
            Preconditions.checkState(Thread.holdsLock(SharedBuffer.this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());

            return finished;
        }

        public void setFinished()
        {
            Preconditions.checkState(Thread.holdsLock(SharedBuffer.this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());

            finished = true;
        }

        public boolean isEmpty()
        {
            return size() == 0;
        }

        public long getSequenceId()
        {
            Preconditions.checkState(Thread.holdsLock(SharedBuffer.this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());

            return sequenceId;
        }

        public int size()
        {
            Preconditions.checkState(Thread.holdsLock(SharedBuffer.this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());

            if (finished) {
                return 0;
            }

            int listOffset = Ints.checkedCast(sequenceId - masterSequenceId);
            if (listOffset >= masterQueue.size()) {
                return 0;
            }
            return masterQueue.size() - listOffset;
        }

        public List<T> getElements(int maxElementCount)
        {
            Preconditions.checkState(Thread.holdsLock(SharedBuffer.this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());

            if (finished) {
                return ImmutableList.of();
            }

            int listOffset = Ints.checkedCast(sequenceId - masterSequenceId);
            if (listOffset >= masterQueue.size()) {
                return ImmutableList.of();
            }

            List<T> elements = masterQueue.subList(listOffset, Math.min(listOffset + maxElementCount, masterQueue.size()));
            sequenceId += elements.size();
            return ImmutableList.copyOf(elements);
        }

        @Override
        public int compareTo(NamedQueue other)
        {
            Preconditions.checkState(Thread.holdsLock(SharedBuffer.this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());

            return ComparisonChain.start()
                    .compare(this.sequenceId, other.sequenceId)
                    .compare(this.queueId, other.queueId)
                    .result();
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("queueId", queueId)
                    .add("sequenceId", sequenceId)
                    .add("finished", finished)
                    .toString();
        }
    }
}
