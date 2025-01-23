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

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.spi.page.SerializedPage;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
final class SerializedPageReference
{
    private static final AtomicIntegerFieldUpdater<SerializedPageReference> REFERENCE_COUNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(SerializedPageReference.class, "referenceCount");

    private final SerializedPage serializedPage;
    private final Lifespan lifespan;
    private volatile int referenceCount;

    public SerializedPageReference(SerializedPage serializedPage, int referenceCount, Lifespan lifespan)
    {
        this.serializedPage = requireNonNull(serializedPage, "page is null");
        this.lifespan = requireNonNull(lifespan, "lifespan is null");
        this.referenceCount = referenceCount;
        checkArgument(referenceCount > 0, "referenceCount must be at least 1");
    }

    public void addReference()
    {
        int oldReferences = REFERENCE_COUNT_UPDATER.getAndIncrement(this);
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

    private boolean dereferencePage()
    {
        int remainingReferences = REFERENCE_COUNT_UPDATER.decrementAndGet(this);
        checkState(remainingReferences >= 0, "Page reference count is negative");
        return remainingReferences == 0;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("referenceCount", referenceCount)
                .toString();
    }

    public static void dereferencePages(List<SerializedPageReference> serializedPageReferences, PagesReleasedListener onPagesReleased)
    {
        requireNonNull(onPagesReleased, "onPagesReleased is null");
        if (requireNonNull(serializedPageReferences, "serializedPageReferences is null").isEmpty()) {
            return;
        }
        Lifespan currentLifespan = null;
        int currentLifespanPages = 0;
        long releasedMemoryBytes = 0;
        for (SerializedPageReference serializedPageReference : serializedPageReferences) {
            if (serializedPageReference.dereferencePage()) {
                if (!serializedPageReference.lifespan.equals(currentLifespan)) {
                    if (currentLifespan != null) {
                        //  Flush the current run of pages for the same lifespan
                        onPagesReleased.onPagesReleased(currentLifespan, currentLifespanPages, releasedMemoryBytes);
                    }
                    currentLifespan = serializedPageReference.lifespan;
                    currentLifespanPages = 0;
                    releasedMemoryBytes = 0;
                }
                currentLifespanPages++;
                releasedMemoryBytes += serializedPageReference.getRetainedSizeInBytes();
            }
        }
        //  Flush pending updates if present
        if (currentLifespan != null) {
            onPagesReleased.onPagesReleased(currentLifespan, currentLifespanPages, releasedMemoryBytes);
        }
    }

    interface PagesReleasedListener
    {
        void onPagesReleased(Lifespan lifespan, int releasedPagesCount, long releasedMemorySizeInBytes);
    }
}
