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
package com.facebook.presto.memory;

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class TestingMemoryContext
        implements LocalMemoryContext
{
    private long usedBytes;
    private final long maxBytes;

    public TestingMemoryContext(long maxBytes)
    {
        this.maxBytes = maxBytes;
        this.usedBytes = 0;
    }

    @Override
    public long getBytes()
    {
        return usedBytes;
    }

    @Override
    public ListenableFuture<?> setBytes(long bytes)
    {
        usedBytes = bytes;
        return Futures.immediateFuture(null);
    }

    @Override
    public ListenableFuture<?> setBytes(long bytes, boolean enforceBroadcastMemoryLimit)
    {
        return setBytes(bytes);
    }

    @Override
    public boolean trySetBytes(long bytes)
    {
        if (usedBytes + bytes > maxBytes) {
            return false;
        }
        usedBytes += bytes;
        return true;
    }

    @Override
    public boolean trySetBytes(long bytes, boolean enforceBroadcastMemoryLimit)
    {
        return trySetBytes(bytes);
    }

    @Override
    public void close()
    {
    }
}
