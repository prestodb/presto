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
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.operator.TaskContext;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.OptionalInt;

/**
 * This interface and the LegacyQueryContext implementation are written to ease the removal of the system memory pool.
 * Once the system pool is removed they should both be removed.
 */
@Deprecated
public interface QueryContext
{
    ListenableFuture<?> reserveSpill(long bytes);

    void freeSpill(long bytes);

    void setResourceOvercommit();

    void setMemoryPool(MemoryPool pool);

    TaskContext getTaskContextByTaskId(TaskId taskId);

    TaskContext addTaskContext(TaskStateMachine taskStateMachine, Session session, boolean perOperatorCpuTimerEnabled, boolean cpuTimerEnabled, OptionalInt totalPartitions);

    MemoryPool getMemoryPool();

    <C, R> R accept(QueryContextVisitor<C, R> visitor, C context);

    <C, R> List<R> acceptChildren(QueryContextVisitor<C, R> visitor, C context);
}
