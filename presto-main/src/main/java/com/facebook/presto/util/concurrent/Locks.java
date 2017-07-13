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
package com.facebook.presto.util.concurrent;

import com.facebook.presto.util.CheckedCallable;
import com.facebook.presto.util.CheckedRunnable;

import java.util.concurrent.locks.Lock;

import static java.util.Objects.requireNonNull;

public class Locks
{
    public static <E extends Throwable> void locking(Lock lock, CheckedRunnable<E> callback)
            throws E
    {
        locking(lock, asCallable(callback));
    }

    public static <T, E extends Throwable> T locking(Lock lock, CheckedCallable<T, E> callback)
            throws E
    {
        return locking(lock, UninterruptibleLocker.INSTANCE, callback);
    }

    public static <E extends Throwable> void lockingInterruptibly(Lock lock, CheckedRunnable<E> callback)
            throws E, InterruptedException
    {
        lockingInterruptibly(lock, asCallable(callback));
    }

    public static <T, E extends Throwable> T lockingInterruptibly(Lock lock, CheckedCallable<T, E> callback)
            throws E, InterruptedException
    {
        return locking(lock, InterruptibleLocker.INSTANCE, callback);
    }

    private static <T, E extends Throwable, L extends Exception> T locking(
            Lock lock,
            Locker<L> locker,
            CheckedCallable<T, E> callback)
            throws L, E
    {
        requireNonNull(lock, "lock is null");
        requireNonNull(locker, "locker is null");
        requireNonNull(callback, "callback is null");

        locker.lock(lock);
        try {
            return callback.call();
        }
        finally {
            locker.unlock(lock);
        }
    }

    private enum UninterruptibleLocker
            implements Locker<RuntimeException>
    {
        INSTANCE;

        @Override
        public void lock(Lock lock)
        {
            lock.lock();
        }

        @Override
        public void unlock(Lock lock)
        {
            lock.unlock();
        }
    }

    private enum InterruptibleLocker
            implements Locker<InterruptedException>
    {
        INSTANCE;

        @Override
        public void lock(Lock lock)
                throws InterruptedException
        {
            lock.lockInterruptibly();
        }

        @Override
        public void unlock(Lock lock)
        {
            lock.unlock();
        }
    }

    private static <E extends Throwable> CheckedCallable<Void, E> asCallable(CheckedRunnable<E> callback)
    {
        requireNonNull(callback, "callback is null");
        return () -> {
            callback.run();
            return null;
        };
    }

    private interface Locker<LockException extends Exception>
    {
        void lock(Lock lock)
                throws LockException;

        void unlock(Lock lock);
    }

    private Locks() {}
}
