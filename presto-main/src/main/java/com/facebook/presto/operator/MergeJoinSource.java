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
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Queue;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.google.common.base.Preconditions.checkState;

public class MergeJoinSource
        implements Closeable
{
    private final Queue<Page> pages;
    private final boolean bufferEnabled;
    private LocalMemoryContext localMemoryContext;
    private boolean finishing;
    private int currentPosition;

    // consumer is blocked initially, producer is not blocked
    private ListenableFuture<?> consumerFuture = SettableFuture.create();
    private ListenableFuture<?> producerFuture = NOT_BLOCKED;

    public MergeJoinSource(boolean bufferEnabled)
    {
        this.bufferEnabled = bufferEnabled;
        this.pages = new ArrayDeque<>();
    }

    public void setLocalMemoryContext(LocalMemoryContext localMemoryContext)
    {
        this.localMemoryContext = localMemoryContext;
    }

    public ListenableFuture<?> getConsumerFuture()
    {
        synchronized (this) {
            if (bufferEnabled && pages.size() > 0) {
                unblock(consumerFuture);
            }
            return consumerFuture;
        }
    }

    public ListenableFuture<?> getProducerFuture()
    {
        return producerFuture;
    }

    public Page peekPage()
    {
        return pages.peek();
    }

    public int getCurrentPosition()
    {
        return currentPosition;
    }

    public int advancePosition()
    {
        currentPosition++;
        return currentPosition;
    }

    public int updatePosition(int position)
    {
        currentPosition = position;
        return currentPosition;
    }

    public void releasePage()
    {
        synchronized (this) {
            long bytesReleased = pages.remove().getSizeInBytes();
            if (localMemoryContext != null && !finishing && pages.peek() != null) {
                localMemoryContext.setBytes(localMemoryContext.getBytes() - bytesReleased);
            }
            if (pages.isEmpty() && !finishing) {
                // we have no input, block consumer until we get more
                consumerFuture = SettableFuture.create();
            }
            if (!bufferEnabled) {
                // if not buffering, need to unblock producer to let it can produce more
                unblock(producerFuture);
            }
            currentPosition = 0;
        }
    }

    public void addPage(Page page)
    {
        synchronized (this) {
            // if we are finishing, either no more right input or consumer closed early
            if (finishing) {
                return;
            }

            // make sure prev data is already consumed
            if (!bufferEnabled) {
                checkState(this.pages.isEmpty());
            }

            this.pages.add(page);
            if (localMemoryContext != null) {
                localMemoryContext.setBytes(localMemoryContext.getBytes() + page.getSizeInBytes());
            }

            // unblock consumer for consuming page
            unblock(consumerFuture);

            // if not buffering, need to block producer until data is consumed
            if (!bufferEnabled) {
                producerFuture = SettableFuture.create();
            }
        }
    }

    public boolean isFinishing()
    {
        return finishing;
    }

    public void finish()
    {
        // no more right input
        synchronized (this) {
            finishing = true;
            unblock(consumerFuture);
        }
    }

    @Override
    public void close()
    {
        // consumer wants to close
        synchronized (this) {
            finishing = true;

            // consumer want to close, unblock producer
            if (producerFuture != NOT_BLOCKED) {
                unblock(producerFuture);
            }

            // clear pages
            this.pages.clear();
        }
    }

    private void unblock(ListenableFuture<?> future)
    {
        if (future != null) {
            ((SettableFuture<Boolean>) future).set(null);
        }
    }
}
