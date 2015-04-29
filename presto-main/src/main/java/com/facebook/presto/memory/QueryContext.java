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

import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_MEMORY_LIMIT;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QueryContext
{
    private final long maxMemory;
    private final boolean enforceLimit;
    private final Executor executor;
    private final List<TaskContext> taskContexts = new CopyOnWriteArrayList<>();

    @GuardedBy("this")
    private long reserved;

    @GuardedBy("this")
    private MemoryPool memoryPool;

    public QueryContext(boolean enforceLimit, DataSize maxMemory, MemoryPool memoryPool, Executor executor)
    {
        this.enforceLimit = enforceLimit;
        this.maxMemory = requireNonNull(maxMemory, "maxMemory is null").toBytes();
        this.memoryPool = requireNonNull(memoryPool, "memoryPool is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    public synchronized ListenableFuture<?> reserveMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        if (reserved + bytes > maxMemory && enforceLimit) {
            throw new PrestoException(EXCEEDED_MEMORY_LIMIT, "Query exceeded local memory limit of " + new DataSize(maxMemory, DataSize.Unit.BYTE).convertToMostSuccinctDataSize());
        }
        ListenableFuture<?> future = memoryPool.reserve(bytes);
        reserved += bytes;
        return future;
    }

    public synchronized boolean tryReserveMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        if (reserved + bytes > maxMemory && enforceLimit) {
            return false;
        }
        if (memoryPool.tryReserve(bytes)) {
            reserved += bytes;
            return true;
        }
        return false;
    }

    public synchronized void freeMemory(long bytes)
    {
        checkArgument(reserved - bytes >= 0, "tried to free more memory than is reserved");
        reserved -= bytes;
        memoryPool.free(bytes);
    }

    public synchronized void setMemoryPool(MemoryPool pool)
    {
        requireNonNull(pool, "pool is null");
        if (pool.getId().equals(memoryPool.getId())) {
            // Don't unblock our tasks and thrash the pools, if this is a no-op
            return;
        }
        MemoryPool originalPool = memoryPool;
        long originalReserved = reserved;
        memoryPool = pool;
        ListenableFuture<?> future = pool.reserve(reserved);
        Futures.addCallback(future, new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result)
            {
                originalPool.free(originalReserved);
                // Unblock all the tasks, if they were waiting for memory, since we're in a new pool.
                taskContexts.stream().forEach(TaskContext::moreMemoryAvailable);
            }

            @Override
            public void onFailure(Throwable t)
            {
                originalPool.free(originalReserved);
                // Unblock all the tasks, if they were waiting for memory, since we're in a new pool.
                taskContexts.stream().forEach(TaskContext::moreMemoryAvailable);
            }
        });
    }

    public TaskContext addTaskContext(TaskStateMachine taskStateMachine, Session session, DataSize maxTaskMemory, DataSize operatorPreAllocatedMemory, boolean verboseStats, boolean cpuTimerEnabled)
    {
        TaskContext taskContext = new TaskContext(this, taskStateMachine, executor, session, maxTaskMemory, operatorPreAllocatedMemory, verboseStats, cpuTimerEnabled);
        taskContexts.add(taskContext);
        return taskContext;
    }
}
