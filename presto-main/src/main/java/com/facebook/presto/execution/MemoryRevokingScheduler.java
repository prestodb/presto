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
package com.facebook.presto.execution;

import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.memory.TraversingQueryContextVisitor;
import com.facebook.presto.operator.OperatorContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;

import javax.inject.Inject;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public class MemoryRevokingScheduler
{
    private static final Ordering<SqlTask> ORDER_BY_CREATE_TIME = Ordering.natural().onResultOf(task -> task.getTaskInfo().getStats().getCreateTime());
    private final MemoryPool systemMemoryPool;

    @Inject
    public MemoryRevokingScheduler(LocalMemoryManager localMemoryManager)
    {
        this(requireNonNull(localMemoryManager, "localMemoryManager can not be null").getPool(LocalMemoryManager.SYSTEM_POOL));
    }

    @VisibleForTesting
    MemoryRevokingScheduler(MemoryPool systemMemoryPool)
    {
        this.systemMemoryPool = requireNonNull(systemMemoryPool, "systemMemoryPool can not be null");
    }

    public void requestSystemMemoryRevokingIfNeeded(Collection<SqlTask> sqlTasks)
    {
        long freeBytes = systemMemoryPool.getFreeBytes();
        if (freeBytes > 0) {
            return;
        }

        long remainingBytesToRevoke = -freeBytes;
        remainingBytesToRevoke -= getMemoryAlreadyBeingRevoked(sqlTasks);
        requestRevoking(remainingBytesToRevoke, sqlTasks);
    }

    private long getMemoryAlreadyBeingRevoked(Collection<SqlTask> sqlTasks)
    {
        AtomicLong memoryAlreadyBeingRevoked = new AtomicLong();
        sqlTasks.stream()
                .filter(task -> task.getTaskInfo().getTaskStatus().getState() == TaskState.RUNNING)
                .forEach(task -> task.getQueryContext().accept(new TraversingQueryContextVisitor<Void, Void>()
                {
                    @Override
                    public Void visitOperatorContext(OperatorContext operatorContext, Void context)
                    {
                        if (operatorContext.isSystemMemoryRevokingRequested()) {
                            memoryAlreadyBeingRevoked.addAndGet(operatorContext.getSystemMemoryContext().getReservedRevocableBytes());
                        }
                        return null;
                    }
                }, null));
        return memoryAlreadyBeingRevoked.get();
    }

    private void requestRevoking(long remainingBytesToRevoke, Collection<SqlTask> sqlTasks)
    {
        AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(remainingBytesToRevoke);
        sqlTasks.stream()
                .filter(task -> task.getTaskInfo().getTaskStatus().getState() == TaskState.RUNNING)
                .sorted(ORDER_BY_CREATE_TIME)
                .forEach(task -> task.getQueryContext().accept(new TraversingQueryContextVisitor<AtomicLong, Void>()
                {
                    @Override
                    public Void visitQueryContext(QueryContext queryContext, AtomicLong remainingBytesToRevoke)
                    {
                        if (remainingBytesToRevoke.get() < 0) {
                            // exit immediately if no work needs to be done
                            return null;
                        }
                        return super.visitQueryContext(queryContext, remainingBytesToRevoke);
                    }

                    @Override
                    public Void visitOperatorContext(OperatorContext operatorContext, AtomicLong remainingBytesToRevoke)
                    {
                        if (remainingBytesToRevoke.get() > 0) {
                            long operatorRevocableBytes = operatorContext.getSystemMemoryContext().getReservedRevocableBytes();
                            if (operatorRevocableBytes > 0 && !operatorContext.isSystemMemoryRevokingRequested()) {
                                operatorContext.requestSystemMemoryRevoking();
                                remainingBytesToRevoke.addAndGet(-operatorRevocableBytes);
                            }
                        }
                        return null;
                    }
                }, remainingBytesToRevokeAtomic));
    }
}
