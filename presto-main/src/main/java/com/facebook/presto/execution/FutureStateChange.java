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

import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@ThreadSafe
public class FutureStateChange<T>
{
    // Use a separate future for each listener so canceled listeners can be removed
    @GuardedBy("this")
    private final Set<CompletableFuture<T>> listeners = new HashSet<>();

    public synchronized CompletableFuture<T> createNewListener()
    {
        CompletableFuture<T> listener = new CompletableFuture<>();
        listeners.add(listener);

        // remove the listener when the future completes
        listener.whenComplete((t, throwable) -> {
            synchronized (FutureStateChange.this) {
                listeners.remove(listener);
            }
        });

        return listener;
    }

    public void complete(T newState)
    {
        Set<CompletableFuture<T>> futures;
        synchronized (this) {
            futures = ImmutableSet.copyOf(listeners);
            listeners.clear();
        }

        for (CompletableFuture<T> future : futures) {
            future.complete(newState);
        }
    }
}
