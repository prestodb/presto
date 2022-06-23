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

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.presto.Session;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.event.SplitMonitor;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskExchangeClientManager;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.planner.HttpRemoteSourceFactory;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.PlanFragment;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.facebook.presto.SystemSessionProperties.isVerboseExceededMemoryLimitErrorsEnabled;
import static com.facebook.presto.execution.SqlTaskExecution.createSqlTaskExecution;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SqlTaskExecutionFactory
{
    private final Executor taskNotificationExecutor;

    private final TaskExecutor taskExecutor;

    private final LocalExecutionPlanner planner;
    private final BlockEncodingSerde blockEncodingSerde;
    private final OrderingCompiler orderingCompiler;
    private final SplitMonitor splitMonitor;
    private final boolean perOperatorCpuTimerEnabled;
    private final boolean cpuTimerEnabled;
    private final boolean perOperatorAllocationTrackingEnabled;
    private final boolean allocationTrackingEnabled;
    private final boolean legacyLifespanCompletionCondition;

    public SqlTaskExecutionFactory(
            Executor taskNotificationExecutor,
            TaskExecutor taskExecutor,
            LocalExecutionPlanner planner,
            BlockEncodingSerde blockEncodingSerde,
            OrderingCompiler orderingCompiler,
            SplitMonitor splitMonitor,
            TaskManagerConfig config)
    {
        this.taskNotificationExecutor = requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
        this.planner = requireNonNull(planner, "planner is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.splitMonitor = requireNonNull(splitMonitor, "splitMonitor is null");
        requireNonNull(config, "config is null");
        this.perOperatorCpuTimerEnabled = config.isPerOperatorCpuTimerEnabled();
        this.cpuTimerEnabled = config.isTaskCpuTimerEnabled();
        this.perOperatorAllocationTrackingEnabled = config.isPerOperatorAllocationTrackingEnabled();
        this.allocationTrackingEnabled = config.isTaskAllocationTrackingEnabled();
        this.legacyLifespanCompletionCondition = config.isLegacyLifespanCompletionCondition();
    }

    public SqlTaskExecution create(
            Session session,
            QueryContext queryContext,
            TaskStateMachine taskStateMachine,
            OutputBuffer outputBuffer,
            TaskExchangeClientManager taskExchangeClientManager,
            PlanFragment fragment,
            List<TaskSource> sources,
            TableWriteInfo tableWriteInfo)
    {
        TaskContext taskContext = queryContext.addTaskContext(
                taskStateMachine,
                session,
                // Plan has to be retained only if verbose memory exceeded errors are requested
                isVerboseExceededMemoryLimitErrorsEnabled(session) ? Optional.of(fragment.getRoot()) : Optional.empty(),
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled,
                perOperatorAllocationTrackingEnabled,
                allocationTrackingEnabled,
                legacyLifespanCompletionCondition);

        LocalExecutionPlan localExecutionPlan;
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskStateMachine.getTaskId())) {
            try {
                localExecutionPlan = planner.plan(
                        taskContext,
                        fragment.getRoot(),
                        fragment.getPartitioningScheme(),
                        fragment.getStageExecutionDescriptor(),
                        fragment.getTableScanSchedulingOrder(),
                        outputBuffer,
                        new HttpRemoteSourceFactory(blockEncodingSerde, taskExchangeClientManager, orderingCompiler),
                        tableWriteInfo);
            }
            catch (Throwable e) {
                // planning failed
                taskStateMachine.failed(e);
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
        return createSqlTaskExecution(
                taskStateMachine,
                taskContext,
                outputBuffer,
                sources,
                localExecutionPlan,
                taskExecutor,
                taskNotificationExecutor,
                splitMonitor);
    }
}
