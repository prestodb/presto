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
import com.facebook.presto.spiller.SpillSpaceTracker;
import io.airlift.units.DataSize;

import java.util.concurrent.Executor;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public final class TestingTaskContext
{
    private TestingTaskContext() {}

    public static TaskContext createTaskContext(Executor executor, Session session)
    {
        return builder(executor, session).build();
    }

    public static TaskContext createTaskContext(Executor executor, Session session, DataSize maxMemory)
    {
        return builder(executor, session)
                .setQueryMaxMemory(maxMemory)
                .build();
    }

    public static TaskContext createTaskContext(QueryContext queryContext, Executor executor, Session session)
    {
        return queryContext.addTaskContext(
                new TaskStateMachine(new TaskId("query", 0, 0), executor),
                session,
                true,
                true);
    }

    public static Builder builder(Executor executor, Session session)
    {
        return new Builder(executor, session);
    }

    public static class Builder
    {
        private final Executor executor;
        private final Session session;
        private DataSize queryMaxMemory = new DataSize(256, MEGABYTE);
        private DataSize memoryPoolSize = new DataSize(1, GIGABYTE);
        private DataSize systemMemoryPoolSize = new DataSize(1, GIGABYTE);
        private DataSize maxSpillSize = new DataSize(1, GIGABYTE);
        private DataSize queryMaxSpillSize = new DataSize(1, GIGABYTE);

        private Builder(Executor executor, Session session)
        {
            this.executor = executor;
            this.session = session;
        }

        public Builder setQueryMaxMemory(DataSize queryMaxMemory)
        {
            this.queryMaxMemory = queryMaxMemory;
            return this;
        }

        public Builder setMemoryPoolSize(DataSize memoryPoolSize)
        {
            this.memoryPoolSize = memoryPoolSize;
            return this;
        }

        public Builder setSystemMemoryPoolSize(DataSize systemMemoryPoolSize)
        {
            this.systemMemoryPoolSize = systemMemoryPoolSize;
            return this;
        }

        public Builder setMaxSpillSize(DataSize maxSpillSize)
        {
            this.maxSpillSize = maxSpillSize;
            return this;
        }

        public Builder setQueryMaxSpillSize(DataSize queryMaxSpillSize)
        {
            this.queryMaxSpillSize = queryMaxSpillSize;
            return this;
        }

        public TaskContext build()
        {
            MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), memoryPoolSize);
            MemoryPool systemMemoryPool = new MemoryPool(new MemoryPoolId("testSystem"), systemMemoryPoolSize);
            SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(maxSpillSize);
            QueryContext queryContext = new QueryContext(
                    new QueryId("test_query"),
                    queryMaxMemory,
                    memoryPool,
                    systemMemoryPool,
                    executor,
                    queryMaxSpillSize,
                    spillSpaceTracker);

            return createTaskContext(queryContext, executor, session);
        }
    }
}
