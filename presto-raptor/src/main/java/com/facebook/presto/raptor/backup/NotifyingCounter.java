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
package com.facebook.presto.raptor.backup;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class NotifyingCounter
{
    private static final CompletableFuture<?> NOT_BLOCKED = CompletableFuture.completedFuture(null);

    private final long threshold;
    private long counter;
    private CompletableFuture<?> currentFuture = NOT_BLOCKED;

    public NotifyingCounter(long threshold)
    {
        checkArgument(threshold >= 0, "threshold must >= 0");
        this.threshold = threshold;
    }

    public synchronized void increment()
    {
        counter++;
        if (counter == threshold) {
            // moving above the threshold now
            currentFuture = new CompletableFuture<>();
        }
    }

    public void decrement()
    {
        CompletableFuture<?> oldFuture = null;

        synchronized (this) {
            checkState(counter > 0, "counter to decrement must > 0");
            if (threshold != 0 && counter == threshold) {
                // moving below the threshold now
                oldFuture = currentFuture;
                currentFuture = NOT_BLOCKED;
            }
            counter--;
        }

        // complete the future outside of the lock
        if (oldFuture != null) {
            oldFuture.complete(null);
        }
    }

    public synchronized CompletableFuture<?> getBelowThreshold()
    {
        return currentFuture;
    }

    public synchronized long getCount()
    {
        return counter;
    }
}
