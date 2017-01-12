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

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class SerializedPageReference
{
    private final SerializedPage serializedPage;
    private final AtomicInteger referenceCount;
    private final Runnable onDereference;

    public SerializedPageReference(SerializedPage serializedPage, int referenceCount, Runnable onDereference)
    {
        this.serializedPage = requireNonNull(serializedPage, "page is null");
        checkArgument(referenceCount > 0, "referenceCount must be at least 1");
        this.referenceCount = new AtomicInteger(referenceCount);
        this.onDereference = requireNonNull(onDereference, "onDereference is null");
    }

    public void addReference()
    {
        int oldReferences = referenceCount.getAndIncrement();
        checkState(oldReferences > 0, "Page has already been dereferenced");
    }

    public SerializedPage getSerializedPage()
    {
        return serializedPage;
    }

    public int getPositionCount()
    {
        return serializedPage.getPositionCount();
    }

    public long getRetainedSizeInBytes()
    {
        return serializedPage.getRetainedSizeInBytes();
    }

    public void dereferencePage()
    {
        int remainingReferences = referenceCount.decrementAndGet();
        checkState(remainingReferences >= 0, "Page reference count is negative");

        if (remainingReferences == 0) {
            onDereference.run();
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("referenceCount", referenceCount)
                .toString();
    }
}
