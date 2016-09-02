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
package com.facebook.presto.testing;

import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import io.airlift.units.DataSize;

import java.util.concurrent.Executor;

import static com.facebook.presto.util.Threads.checkNotSameThreadExecutor;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public final class TestingTaskContext
{
    private TestingTaskContext() {}

    public static TaskContext createTaskContext(Executor executor, Session session)
    {
        return createTaskContext(
                checkNotSameThreadExecutor(executor, "executor is null"),
                session,
                new DataSize(256, MEGABYTE));
    }

    public static TaskContext createTaskContext(Executor executor, Session session, DataSize maxMemory)
    {
        return createTaskContext(executor, session, maxMemory, new DataSize(1, MEGABYTE));
    }

    public static TaskContext createTaskContext(Executor executor, Session session, DataSize maxMemory, DataSize preallocated)
    {
        MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE));
        MemoryPool systemMemoryPool = new MemoryPool(new MemoryPoolId("testSystem"), new DataSize(1, GIGABYTE));
        QueryContext queryContext = new QueryContext(new QueryId("test_query"), maxMemory, memoryPool, systemMemoryPool, executor);
        return createTaskContext(queryContext, executor, session, preallocated);
    }

    public static TaskContext createTaskContext(QueryContext queryContext, Executor executor, Session session, DataSize preallocated)
    {
        return queryContext.addTaskContext(
                new TaskStateMachine(new TaskId("query", 0, 0), checkNotSameThreadExecutor(executor, "executor is null")),
                session,
                preallocated,
                true,
                true);
    }
}
