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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.buffer.SerializedPageReference.PagesReleasedListener;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.SerializedPageReference.dereferencePages;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class ClientBuffer
{
    private final String taskInstanceId;
    private final OutputBufferId bufferId;
    private final PagesReleasedListener onPagesReleased;

    private final AtomicLong rowsAdded = new AtomicLong();
    private final AtomicLong pagesAdded = new AtomicLong();

    private final AtomicLong bufferedBytes = new AtomicLong();

    @GuardedBy("this")
    private final AtomicLong currentSequenceId = new AtomicLong();

    @GuardedBy("this")
    private final LinkedList<SerializedPageReference> pages = new LinkedList<>();

    @GuardedBy("this")
    private boolean noMorePages;

    // destroyed is set when the client sends a DELETE to the buffer
    // this is an acknowledgement that the client has observed the end of the buffer
    @GuardedBy("this")
    private final AtomicBoolean destroyed = new AtomicBoolean();

    @GuardedBy("this")
    private PendingRead pendingRead;

    public ClientBuffer(String taskInstanceId, OutputBufferId bufferId, PagesReleasedListener onPagesReleased)
    {
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.bufferId = requireNonNull(bufferId, "bufferId is null");
        this.onPagesReleased = requireNonNull(onPagesReleased, "onPagesReleased is null");
    }

    public BufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free so state machine updates do not hang
        //

        @SuppressWarnings("FieldAccessNotGuarded")
        boolean destroyed = this.destroyed.get();

        @SuppressWarnings("FieldAccessNotGuarded")
        long sequenceId = this.currentSequenceId.get();

        // if destroyed the buffered page count must be zero regardless of observation ordering in this lock free code
        int bufferedPages = destroyed ? 0 : Math.max(toIntExact(pagesAdded.get() - sequenceId), 0);

        PageBufferInfo pageBufferInfo = new PageBufferInfo(bufferId.getId(), bufferedPages, bufferedBytes.get(), rowsAdded.get(), pagesAdded.get());
        return new BufferInfo(bufferId, destroyed, bufferedPages, sequenceId, pageBufferInfo);
    }

    public boolean isDestroyed()
    {
        //
        // NOTE: this code must be lock free so state machine updates do not hang
        //
        @SuppressWarnings("FieldAccessNotGuarded")
        boolean destroyed = this.destroyed.get();
        return destroyed;
    }

    public void destroy()
    {
        List<SerializedPageReference> removedPages;
        PendingRead pendingRead;
        synchronized (this) {
            removedPages = ImmutableList.copyOf(pages);
            pages.clear();

            bufferedBytes.getAndSet(0);

            noMorePages = true;
            destroyed.set(true);

            pendingRead = this.pendingRead;
            this.pendingRead = null;
        }

        dereferencePages(removedPages, onPagesReleased);

        if (pendingRead != null) {
            pendingRead.completeResultFutureWithEmpty();
        }
    }

    public void enqueuePages(Collection<SerializedPageReference> pages)
    {
        PendingRead pendingRead;
        synchronized (this) {
            // ignore pages after no more pages is set
            // this can happen with limit queries
            if (noMorePages) {
                return;
            }

            addPages(pages);

            pendingRead = this.pendingRead;
            this.pendingRead = null;
        }

        // we just added a page, so process the pending read
        if (pendingRead != null) {
            processRead(pendingRead);
        }
    }

    private synchronized void addPages(Collection<SerializedPageReference> pages)
    {
        long rowCount = 0;
        long bytesAdded = 0;
        int pageCount = 0;
        for (SerializedPageReference page : pages) {
            page.addReference();
            pageCount++;
            rowCount += page.getPositionCount();
            bytesAdded += page.getRetainedSizeInBytes();
        }

        this.pages.addAll(pages);
        rowsAdded.addAndGet(rowCount);
        pagesAdded.addAndGet(pageCount);
        bufferedBytes.addAndGet(bytesAdded);
    }

    public ListenableFuture<BufferResult> getPages(long sequenceId, DataSize maxSize)
    {
        return getPages(sequenceId, maxSize, Optional.empty());
    }

    public ListenableFuture<BufferResult> getPages(long sequenceId, DataSize maxSize, Optional<PagesSupplier> pagesSupplier)
    {
        // acknowledge pages first, out side of locks to not trigger callbacks while holding the lock
        acknowledgePages(sequenceId);

        // attempt to load some data before processing the read
        pagesSupplier.ifPresent(supplier -> loadPagesIfNecessary(supplier, maxSize));

        PendingRead oldPendingRead = null;
        try {
            synchronized (this) {
                // save off the old pending read so we can abort it out side of the lock
                oldPendingRead = this.pendingRead;
                this.pendingRead = null;

                // Return results immediately if we have data, there will be no more data, or this is
                // an out of order request
                if (!pages.isEmpty() || noMorePages || sequenceId != currentSequenceId.get()) {
                    return immediateFuture(processRead(sequenceId, maxSize));
                }

                // otherwise, wait for more data to arrive
                pendingRead = new PendingRead(taskInstanceId, sequenceId, maxSize);
                return pendingRead.getResultFuture();
            }
        }
        finally {
            if (oldPendingRead != null) {
                // Each buffer is private to a single client, and each client should only have one outstanding
                // read.  Therefore, we abort the existing read since it was most likely abandoned by the client.
                oldPendingRead.completeResultFutureWithEmpty();
            }
        }
    }

    public void setNoMorePages()
    {
        PendingRead pendingRead;
        synchronized (this) {
            // ignore duplicate calls
            if (noMorePages) {
                return;
            }

            noMorePages = true;

            pendingRead = this.pendingRead;
            this.pendingRead = null;
        }

        // there will be no more pages, so process the pending read
        if (pendingRead != null) {
            processRead(pendingRead);
        }
    }

    public void loadPagesIfNecessary(PagesSupplier pagesSupplier)
    {
        requireNonNull(pagesSupplier, "pagesSupplier is null");

        // Get the max size from the current pending read, which may not be the
        // same pending read instance by the time pages are loaded but this is
        // safe since the size is rechecked before returning pages.
        DataSize maxSize;
        synchronized (this) {
            if (pendingRead == null) {
                return;
            }
            maxSize = pendingRead.getMaxSize();
        }

        boolean dataAddedOrNoMorePages = loadPagesIfNecessary(pagesSupplier, maxSize);

        if (dataAddedOrNoMorePages) {
            PendingRead pendingRead;
            synchronized (this) {
                pendingRead = this.pendingRead;
            }
            if (pendingRead != null) {
                processRead(pendingRead);
            }
        }
    }

    /**
     * If there no data, attempt to load some from the pages supplier.
     */
    private boolean loadPagesIfNecessary(PagesSupplier pagesSupplier, DataSize maxSize)
    {
        checkState(!Thread.holdsLock(this), "Can not load pages while holding a lock on this");

        boolean dataAddedOrNoMorePages;
        List<SerializedPageReference> pageReferences;
        synchronized (this) {
            if (noMorePages) {
                return false;
            }

            if (!pages.isEmpty()) {
                return false;
            }

            // The page supplier has incremented the page reference count, and addPages below also increments
            // the reference count, so we need to drop the page supplier reference. The call dereferencePage
            // is performed outside of synchronized to avoid making a callback while holding a lock.
            pageReferences = pagesSupplier.getPages(maxSize);

            // add the pages to this buffer, which will increase the reference count
            addPages(pageReferences);

            // check for no more pages
            if (!pagesSupplier.mayHaveMorePages()) {
                noMorePages = true;
            }
            dataAddedOrNoMorePages = !pageReferences.isEmpty() || noMorePages;
        }

        // sent pages will have an initial reference count, so drop it
        dereferencePages(pageReferences, onPagesReleased);

        return dataAddedOrNoMorePages;
    }

    private void processRead(PendingRead pendingRead)
    {
        checkState(!Thread.holdsLock(this), "Can not process pending read while holding a lock on this");

        if (pendingRead.getResultFuture().isDone()) {
            return;
        }

        BufferResult bufferResult = processRead(pendingRead.getSequenceId(), pendingRead.getMaxSize());
        pendingRead.getResultFuture().set(bufferResult);
    }

    /**
     * @return a result with at least one page if we have pages in buffer, empty result otherwise
     */
    private synchronized BufferResult processRead(long sequenceId, DataSize maxSize)
    {
        // When pages are added to the partition buffer they are effectively
        // assigned an id starting from zero. When a read is processed, the
        // "token" is the id of the page to start the read from, so the first
        // step of the read is to acknowledge, and drop all pages up to the
        // provided sequenceId.  Then pages starting from the sequenceId are
        // returned with the sequenceId of the next page to read.
        //
        // Since the buffer API is asynchronous there are a number of problems
        // that can occur our of order request (typically from retries due to
        // request failures):
        // - Request to read pages that have already been acknowledged.
        //   Simply, send an result with no pages and the requested sequenceId,
        //   and since the client has already acknowledge the pages, it will
        //   ignore the out of order response.
        // - Request to read after the buffer has been destroyed.  When the
        //   buffer is destroyed all pages are dropped, so the read sequenceId
        //   appears to be off the end of the queue.  Normally a read past the
        //   end of the queue would be be an error, but this specific case is
        //   detected and handled.  The client is sent an empty response with
        //   the finished flag set and next token is the max acknowledged page
        //   when the buffer is destroyed.
        //

        // if request is for pages before the current position, just return an empty result
        if (sequenceId < currentSequenceId.get()) {
            return emptyResults(taskInstanceId, sequenceId, false);
        }

        // if this buffer is finished, notify the client of this, so the client
        // will destroy this buffer
        if (pages.isEmpty() && noMorePages) {
            return emptyResults(taskInstanceId, currentSequenceId.get(), true);
        }

        // if request is for pages after the current position, there is a bug somewhere
        // a read call is always proceeded by acknowledge pages, which
        // will advance the sequence id to at least the request position, unless
        // the buffer is destroyed, and in that case the buffer will be empty with
        // no more pages set, which is checked above
        verify(sequenceId == currentSequenceId.get(), "Invalid sequence id");

        // read the new pages
        long maxBytes = maxSize.toBytes();
        List<SerializedPage> result = new ArrayList<>();
        long bytes = 0;

        for (SerializedPageReference page : pages) {
            bytes += page.getRetainedSizeInBytes();
            // break (and don't add) if this page would exceed the limit
            if (!result.isEmpty() && bytes > maxBytes) {
                break;
            }
            result.add(page.getSerializedPage());
        }
        return new BufferResult(taskInstanceId, sequenceId, sequenceId + result.size(), false, result);
    }

    /**
     * Drops pages up to the specified sequence id
     */
    public void acknowledgePages(long sequenceId)
    {
        checkArgument(sequenceId >= 0, "Invalid sequence id");
        // Fast path early-return without synchronizing
        if (destroyed.get() || sequenceId < currentSequenceId.get()) {
            return;
        }

        ImmutableList.Builder<SerializedPageReference> removedPages;
        synchronized (this) {
            if (destroyed.get()) {
                return;
            }

            // if pages have already been acknowledged, just ignore this
            long oldCurrentSequenceId = currentSequenceId.get();
            if (sequenceId < oldCurrentSequenceId) {
                return;
            }

            int pagesToRemove = toIntExact(sequenceId - oldCurrentSequenceId);
            checkArgument(pagesToRemove <= pages.size(), "Invalid sequence id");
            removedPages = ImmutableList.builderWithExpectedSize(pagesToRemove);

            long bytesRemoved = 0;
            for (int i = 0; i < pagesToRemove; i++) {
                SerializedPageReference removedPage = pages.removeFirst();
                removedPages.add(removedPage);
                bytesRemoved += removedPage.getRetainedSizeInBytes();
            }

            // update current sequence id
            verify(currentSequenceId.compareAndSet(oldCurrentSequenceId, oldCurrentSequenceId + pagesToRemove));

            // update memory tracking
            verify(bufferedBytes.addAndGet(-bytesRemoved) >= 0);
        }
        // dereference outside of synchronized to avoid making a callback while holding a lock
        dereferencePages(removedPages.build(), onPagesReleased);
    }

    @Override
    public String toString()
    {
        @SuppressWarnings("FieldAccessNotGuarded")
        long sequenceId = currentSequenceId.get();

        @SuppressWarnings("FieldAccessNotGuarded")
        boolean destroyed = this.destroyed.get();

        return toStringHelper(this)
                .add("bufferId", bufferId)
                .add("sequenceId", sequenceId)
                .add("destroyed", destroyed)
                .toString();
    }

    @Immutable
    private static class PendingRead
    {
        private final String taskInstanceId;
        private final long sequenceId;
        private final DataSize maxSize;
        private final SettableFuture<BufferResult> resultFuture = SettableFuture.create();

        private PendingRead(String taskInstanceId, long sequenceId, DataSize maxSize)
        {
            this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
            this.sequenceId = sequenceId;
            this.maxSize = maxSize;
        }

        public long getSequenceId()
        {
            return sequenceId;
        }

        public DataSize getMaxSize()
        {
            return maxSize;
        }

        public SettableFuture<BufferResult> getResultFuture()
        {
            return resultFuture;
        }

        public void completeResultFutureWithEmpty()
        {
            resultFuture.set(emptyResults(taskInstanceId, sequenceId, false));
        }
    }

    public interface PagesSupplier
    {
        /**
         * Gets pages up to the specified size limit or a single page that exceeds the size limit.
         */
        List<SerializedPageReference> getPages(DataSize maxSize);

        /**
         * @return true if more pages may be produced; false otherwise
         */
        boolean mayHaveMorePages();
    }
}
