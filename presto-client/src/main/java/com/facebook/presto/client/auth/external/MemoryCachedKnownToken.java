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
package com.facebook.presto.client.auth.external;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * This KnownToken instance forces all Connections to reuse same token.
 * Every time an existing token is considered to be invalid each Connection
 * will try to obtain a new token, but only the first one will actually do the job,
 * where every other connection will be waiting on readLock
 * until obtaining new token finishes.
 * <p>
 * In general the game is to reuse same token and obtain it only once, no matter how
 * many Connections will be actively using it. It's very important as obtaining the new token
 * will take minutes, as it mostly requires user thinking time.
 */
@ThreadSafe
public class MemoryCachedKnownToken
        implements KnownToken
{
    public static final MemoryCachedKnownToken INSTANCE = new MemoryCachedKnownToken();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    private Optional<Token> knownToken = Optional.empty();

    private MemoryCachedKnownToken()
    {
    }

    @Override
    public Optional<Token> getToken()
    {
        try {
            readLock.lockInterruptibly();
            return knownToken;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public void setupToken(Supplier<Optional<Token>> tokenSource)
    {
        // Try to lock and generate new token. If some other thread (Connection) has
        // already obtained writeLock and is generating new token, then skipp this
        // to block on getToken()
        if (writeLock.tryLock()) {
            try {
                // Clear knownToken before obtaining new token, as it might fail leaving old invalid token.
                knownToken = Optional.empty();
                knownToken = tokenSource.get();
            }
            finally {
                writeLock.unlock();
            }
        }
    }
}
