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

import com.facebook.presto.Session;
import com.facebook.presto.TaskSource;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.concurrent.Executor;

import static com.facebook.presto.execution.SqlTaskExecution.createSqlTaskExecution;
import static com.google.common.base.Preconditions.checkNotNull;

public class SqlTaskExecutionFactory
{
    private final Executor taskNotificationExecutor;

    private final TaskExecutor taskExecutor;

    private final LocalExecutionPlanner planner;
    private final QueryMonitor queryMonitor;
    private final DataSize maxTaskMemoryUsage;
    private final DataSize operatorPreAllocatedMemory;
    private final boolean cpuTimerEnabled;

    public SqlTaskExecutionFactory(
            Executor taskNotificationExecutor,
            TaskExecutor taskExecutor,
            LocalExecutionPlanner planner,
            QueryMonitor queryMonitor,
            TaskManagerConfig config)
    {
        this(
                taskNotificationExecutor,
                taskExecutor,
                planner,
                queryMonitor,
                config.getMaxTaskMemoryUsage(),
                config.getOperatorPreAllocatedMemory(),
                config.isTaskCpuTimerEnabled());
    }

    public SqlTaskExecutionFactory(
            Executor taskNotificationExecutor,
            TaskExecutor taskExecutor,
            LocalExecutionPlanner planner,
            QueryMonitor queryMonitor,
            DataSize maxTaskMemoryUsage,
            DataSize operatorPreAllocatedMemory,
            boolean cpuTimerEnabled)
    {
        this.taskNotificationExecutor = checkNotNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor is null");
        this.planner = checkNotNull(planner, "planner is null");
        this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");
        this.maxTaskMemoryUsage = checkNotNull(maxTaskMemoryUsage, "maxTaskMemoryUsage is null");
        this.operatorPreAllocatedMemory = checkNotNull(operatorPreAllocatedMemory, "operatorPreAllocatedMemory is null");
        this.cpuTimerEnabled = checkNotNull(cpuTimerEnabled, "cpuTimerEnabled is null");
    }

    public SqlTaskExecution create(Session session, TaskStateMachine taskStateMachine, SharedBuffer sharedBuffer, PlanFragment fragment, List<TaskSource> sources)
    {
        TaskContext taskContext = new TaskContext(
                taskStateMachine,
                taskNotificationExecutor,
                session,
                checkNotNull(maxTaskMemoryUsage, "maxTaskMemoryUsage is null"),
                checkNotNull(operatorPreAllocatedMemory, "operatorPreAllocatedMemory is null"),
                cpuTimerEnabled);

        return createSqlTaskExecution(
                taskStateMachine,
                taskContext,
                sharedBuffer,
                fragment,
                sources,
                planner,
                taskExecutor,
                taskNotificationExecutor,
                queryMonitor);
    }
}
