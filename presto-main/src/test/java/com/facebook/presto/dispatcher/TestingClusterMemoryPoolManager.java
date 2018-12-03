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
package com.facebook.presto.dispatcher;

import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class TestingClusterMemoryPoolManager
        implements ClusterMemoryPoolManager
{
    private final long totalBytes;
    private final AtomicLong availableBytes;

    public TestingClusterMemoryPoolManager(long totalBytes)
    {
        this.totalBytes = totalBytes;
        this.availableBytes = new AtomicLong(totalBytes);
    }

    @Override
    public void addChangeListener(MemoryPoolId poolId, Consumer<MemoryPoolInfo> listener)
    {

    }

    @Override
    public long getClusterTotalMemoryReservation()
    {
        return totalBytes - availableBytes.get();
    }

    @Override
    public long getClusterMemoryBytes()
    {
        return totalBytes;
    }

    public void reserve(long bytes)
    {
        availableBytes.addAndGet(-bytes);
    }

    public void free(long bytes)
    {
        availableBytes.addAndGet(bytes);
    }
}
