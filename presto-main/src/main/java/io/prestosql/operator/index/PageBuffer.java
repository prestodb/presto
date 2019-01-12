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
package io.prestosql.operator.index;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.spi.Page;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class PageBuffer
{
    private static final ListenableFuture<?> NOT_FULL = Futures.immediateFuture(null);

    private final int maxBufferedPages;
    private final Queue<Page> pages;
    private SettableFuture<?> settableFuture;

    public PageBuffer(int maxBufferedPages)
    {
        checkArgument(maxBufferedPages > 0, "maxBufferedPages must be at least one");
        this.maxBufferedPages = maxBufferedPages;
        this.pages = new ArrayDeque<>(maxBufferedPages);
    }

    public synchronized boolean isFull()
    {
        return pages.size() >= maxBufferedPages;
    }

    /**
     * Adds a page to the buffer.
     * Returns a ListenableFuture that is marked as done when the next page can be added.
     */
    public synchronized ListenableFuture<?> add(Page page)
    {
        checkState(!isFull(), "PageBuffer is full!");
        pages.offer(page);
        if (isFull()) {
            if (settableFuture == null) {
                settableFuture = SettableFuture.create();
            }
            return settableFuture;
        }
        return NOT_FULL;
    }

    /**
     * Return a page from the buffer, or null if none exists
     */
    public synchronized Page poll()
    {
        if (settableFuture != null) {
            settableFuture.set(null);
            settableFuture = null;
        }
        return pages.poll();
    }
}
