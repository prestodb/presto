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

import com.facebook.presto.spi.Page;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class PageReference
{
    private final Page page;
    private final Runnable onFree;
    private final AtomicInteger referenceCount;

    public PageReference(Page page, int referenceCount, Runnable onFree)
    {
        this.page = requireNonNull(page, "page is null");
        this.onFree = requireNonNull(onFree, "onFree is null");
        checkArgument(referenceCount >= 1, "referenceCount must be at least 1");
        this.referenceCount = new AtomicInteger(referenceCount);
    }

    public long getSizeInBytes()
    {
        return page.getSizeInBytes();
    }

    public Page removePage()
    {
        int referenceCount = this.referenceCount.decrementAndGet();
        checkArgument(referenceCount >= 0, "Page reference count is negative");
        if (referenceCount == 0) {
            onFree.run();
        }
        return page;
    }
}
