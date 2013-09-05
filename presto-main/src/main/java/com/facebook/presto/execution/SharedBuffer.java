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
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.PagePartitionFunction;
import com.facebook.presto.operator.Page;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.execution.BufferResult.emptyResults;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class SharedBuffer
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
         * No more queues can be added and all pages have been consumed.
         */
        FINISHED
    }

    private final long maxBufferedBytes;

    @GuardedBy("this")
    private OutputBuffers outputBuffers;

    @GuardedBy("this")
    private long bufferedBytes;
    @GuardedBy("this")
    private final LinkedList<Page> masterQueue = new LinkedList<>();
    @GuardedBy("this")
    private final LinkedList<QueuedPage> queuedPages = new LinkedList<>();
    @GuardedBy("this")
    private long masterSequenceId;
    @GuardedBy("this")
    private Map<String, NamedQueue> namedQueues = new HashMap<>();
    @GuardedBy("this")
    private final SortedSet<NamedQueue> openQueuesBySequenceId = new TreeSet<>();
    @GuardedBy("this")
    private QueueState state = QueueState.OPEN;

    private final AtomicLong pagesAdded = new AtomicLong();

    /**
     * If true, no more pages can be added to the queue.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    public SharedBuffer(DataSize maxBufferSize, OutputBuffers outputBuffers)
    {
        checkNotNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        this.maxBufferedBytes = maxBufferSize.toBytes();

        this.outputBuffers = checkNotNull(outputBuffers, "outputBuffers is null");
        updateOutputBuffers();
    }

    public synchronized boolean isFinished()
    {
        return state == QueueState.FINISHED;
    }

    public synchronized SharedBufferInfo getInfo()
    {
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (NamedQueue namedQueue : namedQueues.values()) {
            infos.add(new BufferInfo(namedQueue.getQueueId(), namedQueue.isFinished(), namedQueue.size(), namedQueue.pagesRemoved()));
        }
        return new SharedBufferInfo(state, masterSequenceId, pagesAdded.get(), infos.build());
    }

    public synchronized void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        checkNotNull(newOutputBuffers, "newOutputBuffers is null");
        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (state == QueueState.FINISHED || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        SetView<String> missingBuffers = Sets.difference(outputBuffers.getBuffers().keySet(), newOutputBuffers.getBuffers().keySet());
        checkArgument(missingBuffers.isEmpty(), "newOutputBuffers does not have existing buffers %s", missingBuffers);
        checkArgument(!outputBuffers.isNoMoreBufferIds() || newOutputBuffers.isNoMoreBufferIds(), "Expected newOutputBuffers to have noMoreBufferIds set");
        outputBuffers = newOutputBuffers;

        updateOutputBuffers();
    }

    private synchronized void updateOutputBuffers()
    {
        for (Entry<String, PagePartitionFunction> entry : outputBuffers.getBuffers().entrySet()) {
            String bufferId = entry.getKey();
            if (!namedQueues.containsKey(bufferId)) {
                Preconditions.checkState(state == QueueState.OPEN, "%s is not OPEN", SharedBuffer.class.getSimpleName());
                NamedQueue namedQueue = new NamedQueue(bufferId, entry.getValue());
                namedQueues.put(bufferId, namedQueue);
                openQueuesBySequenceId.add(namedQueue);
            }
        }

        if (outputBuffers.isNoMoreBufferIds()) {
            namedQueues = ImmutableMap.copyOf(namedQueues);
            if (state != QueueState.OPEN) {
                return;
            }

            state = QueueState.NO_MORE_QUEUES;

            updateState();
        }
    }

    public synchronized ListenableFuture<?> enqueue(Page page)
    {
        checkNotNull(page, "page is null");

        // is the output done
        if (closed.get()) {
            return Futures.immediateFuture(true);
        }

        // is there room in the buffer
        if (bufferedBytes < maxBufferedBytes) {
            addInternal(page);
            return Futures.immediateFuture(true);
        }

        QueuedPage queuedPage = new QueuedPage(page);
        queuedPages.addLast(queuedPage);
        return queuedPage.getFuture();
    }

    private synchronized void addInternal(Page page)
    {
        // add page
        masterQueue.add(page);
        pagesAdded.incrementAndGet();
        bufferedBytes += page.getDataSize().toBytes();

        // notify consumers an page has arrived
        this.notifyAll();
    }

    @VisibleForTesting
    public synchronized void acknowledge(String outputId, long sequenceId)
    {
        checkNotNull(outputId, "outputId is null");

        NamedQueue namedQueue = namedQueues.get(outputId);
        if (namedQueue == null) {
            throw new NoSuchBufferException(outputId, namedQueues.keySet());
        }

        if (state == QueueState.FINISHED) {
            return;
        }

        // remove queue from set before calling getPages because getPages changes
        // the sequence number of the queue which is used for identity comparison in the
        // sorted set
        openQueuesBySequenceId.remove(namedQueue);

        // acknowledge the pages
        namedQueue.acknowledge(sequenceId);

        // only add back the queue if it is still open
        if (!closed.get()) {
            openQueuesBySequenceId.add(namedQueue);
        }
        else {
            namedQueue.setFinished();
        }

        updateState();
    }

    public synchronized BufferResult get(String outputId, long startingSequenceId, DataSize maxSize, Duration maxWait)
            throws InterruptedException
    {
        checkNotNull(outputId, "outputId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");
        checkNotNull(maxWait, "maxWait is null");

        NamedQueue namedQueue = namedQueues.get(outputId);
        if (namedQueue == null) {
            throw new NoSuchBufferException(outputId, namedQueues.keySet());
        }

        if (state == QueueState.FINISHED) {
            return emptyResults(namedQueue.getSequenceId(), true);
        }

        // wait for pages to arrive
        if (namedQueue.isEmpty()) {
            long remainingNanos = maxWait.roundTo(NANOSECONDS);
            long end = System.nanoTime() + remainingNanos;
            while (remainingNanos > 0 && namedQueue.isEmpty() && !namedQueue.isFinished()) {
                // wait for timeout or notification
                NANOSECONDS.timedWait(this, remainingNanos);
                remainingNanos = end - System.nanoTime();
            }
        }

        // remove queue from set before calling getPages because getPages changes
        // the sequence number of the queue which is used for identity comparison in the
        // sorted set
        openQueuesBySequenceId.remove(namedQueue);

        // get the pages
        BufferResult results = namedQueue.getPages(startingSequenceId, maxSize);

        // only add back the queue if it is still open
        if (!closed.get() || !results.isBufferClosed()) {
            openQueuesBySequenceId.add(namedQueue);
        }
        else {
            namedQueue.setFinished();
        }

        updateState();

        return results;
    }

    public synchronized void abort(String outputId)
    {
        checkNotNull(outputId, "outputId is null");
        NamedQueue namedQueue = namedQueues.get(outputId);
        if (namedQueue == null || namedQueue.isFinished()) {
            return;
        }
        namedQueue.setFinished();
        openQueuesBySequenceId.remove(namedQueue);

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
            // discard queued pages (not officially in the buffer) and waiters
            for (QueuedPage queuedPage : queuedPages) {
                queuedPage.getFuture().set(null);
            }
            queuedPages.clear();
        }

        if (state == QueueState.NO_MORE_QUEUES && !openQueuesBySequenceId.isEmpty()) {
            // advance master sequence id
            long oldMasterSequenceId = masterSequenceId;
            masterSequenceId = openQueuesBySequenceId.iterator().next().getSequenceId();

            // drop consumed pages
            int pagesToRemove = Ints.checkedCast(masterSequenceId - oldMasterSequenceId);
            Preconditions.checkState(pagesToRemove >= 0,
                    "Master sequence id moved backwards: oldMasterSequenceId=%s, newMasterSequenceId=%s",
                    oldMasterSequenceId,
                    masterSequenceId);

            for (int i = 0; i < pagesToRemove; i++) {
                Page page = masterQueue.removeFirst();
                bufferedBytes -= page.getDataSize().toBytes();
            }

            // refill buffer from queued pages
            while (!queuedPages.isEmpty() && bufferedBytes < maxBufferedBytes) {
                QueuedPage queuedPage = queuedPages.removeFirst();
                addInternal(queuedPage.getPage());
                queuedPage.getFuture().set(null);
            }
        }

        if (state == QueueState.NO_MORE_QUEUES && closed.get() && openQueuesBySequenceId.isEmpty()) {
            destroy();
        }

        this.notifyAll();
    }

    /**
     * Marks the output as complete.  After this method is called no more data can be added but there may still be buffered output pages.
     */
    public synchronized void finish()
    {
        closed.set(true);

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

        // drop all of the queues
        for (NamedQueue namedQueue : openQueuesBySequenceId) {
            namedQueue.setFinished();
        }
        openQueuesBySequenceId.clear();

        // clear the buffer
        masterQueue.clear();
        bufferedBytes = 0;

        // free queued page waiters
        for (QueuedPage queuedPage : queuedPages) {
            queuedPage.getFuture().set(null);
        }
        queuedPages.clear();

        // notify readers that the buffer has been destroyed
        this.notifyAll();
    }

    @NotThreadSafe
    private class NamedQueue
            implements Comparable<NamedQueue>
    {
        private final String queueId;
        private final PagePartitionFunction partitionFunction;

        private long sequenceId;
        private boolean finished;

        private NamedQueue(String queueId, PagePartitionFunction partitionFunction)
        {
            this.queueId = queueId;
            this.partitionFunction = partitionFunction;
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

        public long pagesRemoved()
        {
            return getSequenceId();
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
            // todo include queued pages?
            return masterQueue.size() - listOffset;
        }

        public void acknowledge(long sequenceId)
        {
            if (this.sequenceId < sequenceId) {
                this.sequenceId = sequenceId;
            }
        }

        public BufferResult getPages(long startingSequenceId, DataSize maxSize)
        {
            Preconditions.checkState(Thread.holdsLock(SharedBuffer.this), "Thread must hold a lock on the %s", SharedBuffer.class.getSimpleName());
            checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

            acknowledge(startingSequenceId);

            if (finished) {
                return emptyResults(sequenceId, true);
            }

            int listOffset = Ints.checkedCast(sequenceId - masterSequenceId);
            if (listOffset >= masterQueue.size()) {
                return emptyResults(sequenceId, false);
            }

            long maxBytes = maxSize.toBytes();

            List<Page> pages = new ArrayList<>();
            long bytes = 0;
            while (listOffset < masterQueue.size()) {
                Page page = masterQueue.get(listOffset++);
                bytes += page.getDataSize().toBytes();
                // break (and don't add) if this page would exceed the limit
                if (!pages.isEmpty() && bytes > maxBytes) {
                    break;
                }
                pages.add(page);
            }

            return new BufferResult(startingSequenceId, startingSequenceId + pages.size(), false, ImmutableList.copyOf(pages), partitionFunction);
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

    private static class QueuedPage
    {
        private final Page page;
        private final SettableFuture<?> future = SettableFuture.create();

        private QueuedPage(Page page)
        {
            this.page = page;
        }

        private Page getPage()
        {
            return page;
        }

        private SettableFuture<?> getFuture()
        {
            return future;
        }
    }
}
