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
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import io.airlift.concurrent.SetThreadName;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Executor;

import static com.facebook.presto.execution.SqlTaskExecution.createSqlTaskExecution;
import static com.facebook.presto.operator.PipelineExecutionStrategy.GROUPED_EXECUTION;
import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SqlTaskExecutionFactory
{
    private static final String VERBOSE_STATS_PROPERTY = "verbose_stats";
    private final Executor taskNotificationExecutor;

    private final TaskExecutor taskExecutor;

    private final LocalExecutionPlanner planner;
    private final QueryMonitor queryMonitor;
    private final boolean verboseStats;
    private final boolean cpuTimerEnabled;

    public SqlTaskExecutionFactory(
            Executor taskNotificationExecutor,
            TaskExecutor taskExecutor,
            LocalExecutionPlanner planner,
            QueryMonitor queryMonitor,
            TaskManagerConfig config)
    {
        this.taskNotificationExecutor = requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
        this.planner = requireNonNull(planner, "planner is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        requireNonNull(config, "config is null");
        this.verboseStats = config.isVerboseStats();
        this.cpuTimerEnabled = config.isTaskCpuTimerEnabled();
    }

    public SqlTaskExecution create(Session session, QueryContext queryContext, TaskStateMachine taskStateMachine, OutputBuffer outputBuffer, PlanFragment fragment, List<TaskSource> sources, OptionalInt totalPartitions)
    {
        boolean verboseStats = getVerboseStats(session);
        TaskContext taskContext = queryContext.addTaskContext(
                taskStateMachine,
                session,
                verboseStats,
                cpuTimerEnabled,
                totalPartitions);

        LocalExecutionPlan localExecutionPlan;
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskStateMachine.getTaskId())) {
            try {
                localExecutionPlan = planner.plan(
                        taskContext,
                        fragment.getRoot(),
                        TypeProvider.copyOf(fragment.getSymbols()),
                        fragment.getPartitioningScheme(),
                        fragment.getPipelineExecutionStrategy() == GROUPED_EXECUTION,
                        fragment.getPartitionedSources(),
                        outputBuffer);

                for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
                    Optional<PlanNodeId> sourceId = driverFactory.getSourceId();
                    if (sourceId.isPresent() && fragment.isPartitionedSources(sourceId.get())) {
                        checkArgument(fragment.getPipelineExecutionStrategy() == driverFactory.getPipelineExecutionStrategy(),
                                "Partitioned pipelines are expected to have the same execution strategy as the fragment");
                    }
                    else {
                        checkArgument(fragment.getPipelineExecutionStrategy() != UNGROUPED_EXECUTION || driverFactory.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION,
                                "When fragment execution strategy is ungrouped, all pipelines should have ungrouped execution strategy");
                    }
                }
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
                queryMonitor);
    }

    private boolean getVerboseStats(Session session)
    {
        String verboseStats = session.getSystemProperties().get(VERBOSE_STATS_PROPERTY);
        if (verboseStats == null) {
            return this.verboseStats;
        }

        try {
            return Boolean.valueOf(verboseStats.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NOT_SUPPORTED, "Invalid property '" + VERBOSE_STATS_PROPERTY + "=" + verboseStats + "'");
        }
    }
}
