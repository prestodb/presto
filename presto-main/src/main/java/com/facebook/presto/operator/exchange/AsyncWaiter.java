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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;

class AsyncWaiter
{
    private final Object lock = new Object();

    @GuardedBy("lock")
    private SettableFuture<?> future;

    @GuardedBy("lock")
    private boolean finished;

    public ListenableFuture<?> waitFor()
    {
        synchronized (lock) {
            if (finished) {
                return Futures.immediateFuture(null);
            }
            if (future == null) {
                future = SettableFuture.create();
            }
            return future;
        }
    }

    public void notifyWaiters()
    {
        SettableFuture<?> future;
        synchronized (lock) {
            future = this.future;
            this.future = null;
        }

        if (future != null) {
            // notify outside of lock
            future.set(null);
        }
    }

    public void finishAndNotifyWaiters()
    {
        synchronized (lock) {
            finished = true;
        }

        notifyWaiters();
    }
}
