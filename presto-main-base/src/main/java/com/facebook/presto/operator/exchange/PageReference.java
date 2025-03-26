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

import com.facebook.presto.common.Page;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
final class PageReference
{
    private static final AtomicIntegerFieldUpdater<PageReference> REFERENCE_COUNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(PageReference.class, "referenceCount");

    private volatile int referenceCount;
    private final Page page;
    private final PageReleasedListener onPageReleased;

    public PageReference(Page page, int referenceCount, PageReleasedListener onPageReleased)
    {
        this.page = requireNonNull(page, "page is null");
        this.onPageReleased = requireNonNull(onPageReleased, "onPageReleased is null");
        checkArgument(referenceCount >= 1, "referenceCount must be at least 1");
        this.referenceCount = referenceCount;
    }

    public long getRetainedSizeInBytes()
    {
        return page.getRetainedSizeInBytes();
    }

    public Page removePage()
    {
        int referenceCount = REFERENCE_COUNT_UPDATER.decrementAndGet(this);
        checkArgument(referenceCount >= 0, "Page reference count is negative");
        if (referenceCount == 0) {
            onPageReleased.onPageReleased(page.getRetainedSizeInBytes());
        }
        return page;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("size", getRetainedSizeInBytes())
                .add("referenceCount", referenceCount)
                .toString();
    }

    interface PageReleasedListener
    {
        void onPageReleased(long releasedSizeInBytes);

        static PageReleasedListener forLocalExchangeMemoryManager(LocalExchangeMemoryManager memoryManager)
        {
            requireNonNull(memoryManager, "memoryManager is null");
            return (releasedSizeInBytes) -> memoryManager.updateMemoryUsage(-releasedSizeInBytes);
        }
    }
}
