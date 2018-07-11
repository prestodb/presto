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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.operator.WorkProcessor;
import com.facebook.presto.spi.Page;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.facebook.presto.operator.WorkProcessor.ProcessorState.blocked;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.finished;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.ofResult;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.yield;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class LocalExchangeSource
{
    private static final SettableFuture<?> NOT_EMPTY;

    static {
        NOT_EMPTY = SettableFuture.create();
        NOT_EMPTY.set(null);
    }

    private final Consumer<LocalExchangeSource> onFinish;

    private final BlockingQueue<PageReference> buffer = new LinkedBlockingDeque<>();
    private final AtomicLong bufferedBytes = new AtomicLong();

    private final Object lock = new Object();

    @GuardedBy("lock")
    private SettableFuture<?> notEmptyFuture = NOT_EMPTY;

    @GuardedBy("lock")
    private boolean finishing;

    public LocalExchangeSource(Consumer<LocalExchangeSource> onFinish)
    {
        this.onFinish = requireNonNull(onFinish, "onFinish is null");
    }

    public LocalExchangeBufferInfo getBufferInfo()
    {
        // This must be lock free to assure task info creation is fast
        // Note: the stats my be internally inconsistent
        return new LocalExchangeBufferInfo(bufferedBytes.get(), buffer.size());
    }

    void addPage(PageReference pageReference)
    {
        checkNotHoldsLock();

        boolean added = false;
        SettableFuture<?> notEmptyFuture;
        synchronized (lock) {
            // ignore pages after finish
            if (!finishing) {
                // buffered bytes must be updated before adding to the buffer to assure
                // the count does not go negative
                bufferedBytes.addAndGet(pageReference.getRetainedSizeInBytes());
                buffer.add(pageReference);
                added = true;
            }

            // we just added a page (or we are finishing) so we are not empty
            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }

        if (!added) {
            // dereference the page outside of lock
            pageReference.removePage();
        }

        // notify readers outside of lock since this may result in a callback
        notEmptyFuture.set(null);
    }

    public WorkProcessor<Page> pages()
    {
        return WorkProcessor.create(() -> {
            Page page = removePage();
            if (page == null) {
                if (isFinished()) {
                    return finished();
                }

                ListenableFuture<?> blocked = waitForReading();
                if (!blocked.isDone()) {
                    return blocked(blocked);
                }

                return yield();
            }

            return ofResult(page);
        });
    }

    public Page removePage()
    {
        checkNotHoldsLock();

        // NOTE: there is no need to acquire a lock here. The buffer is concurrent
        // and buffered bytes is not expected to be consistent with the buffer (only
        // best effort).
        PageReference pageReference = buffer.poll();
        if (pageReference == null) {
            return null;
        }

        // dereference the page outside of lock, since may trigger a callback
        Page page = pageReference.removePage();
        bufferedBytes.addAndGet(-page.getRetainedSizeInBytes());

        checkFinished();

        return page;
    }

    public ListenableFuture<?> waitForReading()
    {
        checkNotHoldsLock();

        synchronized (lock) {
            // if we need to block readers, and the current future is complete, create a new one
            if (!finishing && buffer.isEmpty() && notEmptyFuture.isDone()) {
                notEmptyFuture = SettableFuture.create();
            }
            return notEmptyFuture;
        }
    }

    public boolean isFinished()
    {
        synchronized (lock) {
            return finishing && buffer.isEmpty();
        }
    }

    public void finish()
    {
        checkNotHoldsLock();

        SettableFuture<?> notEmptyFuture;
        synchronized (lock) {
            if (finishing) {
                return;
            }
            finishing = true;

            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }

        // notify readers outside of lock since this may result in a callback
        notEmptyFuture.set(null);

        checkFinished();
    }

    public void close()
    {
        checkNotHoldsLock();

        List<PageReference> remainingPages = new ArrayList<>();
        SettableFuture<?> notEmptyFuture;
        synchronized (lock) {
            finishing = true;

            buffer.drainTo(remainingPages);
            bufferedBytes.addAndGet(-remainingPages.stream().mapToLong(PageReference::getRetainedSizeInBytes).sum());

            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }

        // free all the remaining pages
        remainingPages.forEach(PageReference::removePage);

        // notify readers outside of lock since this may result in a callback
        notEmptyFuture.set(null);

        // this will always fire the finished event
        checkState(isFinished(), "Expected buffer to be finished");
        checkFinished();
    }

    private void checkFinished()
    {
        checkNotHoldsLock();

        if (isFinished()) {
            // notify finish listener outside of lock, since it may make a callback
            // NOTE: due the race in this method, the onFinish may be called multiple times
            // it is expected that the implementer handles this (which is why this source
            // is passed to the function)
            onFinish.accept(this);
        }
    }

    private void checkNotHoldsLock()
    {
        checkState(!Thread.holdsLock(lock), "Can not execute this method while holding the lock");
    }
}
