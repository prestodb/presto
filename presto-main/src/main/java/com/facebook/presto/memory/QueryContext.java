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
import com.facebook.presto.spi.QueryId;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalLimit;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QueryContext
{
    private final QueryId queryId;
    private final Executor executor;
    private final List<TaskContext> taskContexts = new CopyOnWriteArrayList<>();
    private final MemoryPool systemMemoryPool;

    // TODO: This field should be final. However, due to the way QueryContext is constructed the memory limit is not known in advance
    @GuardedBy("this")
    private long maxMemory;

    @GuardedBy("this")
    private long reserved;

    @GuardedBy("this")
    private MemoryPool memoryPool;

    @GuardedBy("this")
    private long systemReserved;

    public QueryContext(QueryId queryId, DataSize maxMemory, MemoryPool memoryPool, MemoryPool systemMemoryPool, Executor executor)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.maxMemory = requireNonNull(maxMemory, "maxMemory is null").toBytes();
        this.memoryPool = requireNonNull(memoryPool, "memoryPool is null");
        this.systemMemoryPool = requireNonNull(systemMemoryPool, "systemMemoryPool is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    // TODO: This method should be removed, and the correct limit set in the constructor. However, due to the way QueryContext is constructed the memory limit is not known in advance
    public synchronized void setResourceOvercommit()
    {
        // Allow the query to use the entire pool. This way the worker will kill the query, if it uses the entire local general pool.
        // The coordinator will kill the query if the cluster runs out of memory.
        maxMemory = memoryPool.getMaxBytes();
    }

    public synchronized ListenableFuture<?> reserveMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        if (reserved + bytes > maxMemory) {
            throw exceededLocalLimit(succinctBytes(maxMemory));
        }
        ListenableFuture<?> future = memoryPool.reserve(queryId, bytes);
        reserved += bytes;
        return future;
    }

    public synchronized ListenableFuture<?> reserveSystemMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        ListenableFuture<?> future = systemMemoryPool.reserve(queryId, bytes);
        systemReserved += bytes;
        return future;
    }

    public synchronized boolean tryReserveMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        if (reserved + bytes > maxMemory) {
            return false;
        }
        if (memoryPool.tryReserve(queryId, bytes)) {
            reserved += bytes;
            return true;
        }
        return false;
    }

    public synchronized void freeMemory(long bytes)
    {
        checkArgument(reserved - bytes >= 0, "tried to free more memory than is reserved");
        reserved -= bytes;
        memoryPool.free(queryId, bytes);
    }

    public synchronized void freeSystemMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(systemReserved - bytes >= 0, "tried to free more system memory than is reserved");
        systemReserved -= bytes;
        systemMemoryPool.free(queryId, bytes);
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
        ListenableFuture<?> future = pool.reserve(queryId, reserved);
        Futures.addCallback(future, new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result)
            {
                originalPool.free(queryId, originalReserved);
                // Unblock all the tasks, if they were waiting for memory, since we're in a new pool.
                taskContexts.stream().forEach(TaskContext::moreMemoryAvailable);
            }

            @Override
            public void onFailure(Throwable t)
            {
                originalPool.free(queryId, originalReserved);
                // Unblock all the tasks, if they were waiting for memory, since we're in a new pool.
                taskContexts.stream().forEach(TaskContext::moreMemoryAvailable);
            }
        });
    }

    public TaskContext addTaskContext(TaskStateMachine taskStateMachine, Session session, boolean verboseStats, boolean cpuTimerEnabled)
    {
        TaskContext taskContext = new TaskContext(this, taskStateMachine, executor, session, verboseStats, cpuTimerEnabled);
        taskContexts.add(taskContext);
        return taskContext;
    }
}
