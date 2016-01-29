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
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.concurrent.Executor;

import static com.facebook.presto.execution.SqlTaskExecution.createSqlTaskExecution;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class SqlTaskExecutionFactory
{
    private static final String VERBOSE_STATS_PROPERTY = "verbose_stats";
    private final Executor taskNotificationExecutor;

    private final TaskExecutor taskExecutor;

    private final LocalExecutionPlanner planner;
    private final QueryMonitor queryMonitor;
    private final DataSize operatorPreAllocatedMemory;
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
        this.operatorPreAllocatedMemory = config.getOperatorPreAllocatedMemory();
        this.verboseStats = config.isVerboseStats();
        this.cpuTimerEnabled = config.isTaskCpuTimerEnabled();
    }

    public SqlTaskExecution create(Session session, QueryContext queryContext, TaskStateMachine taskStateMachine, OutputBuffer outputBuffer, PlanFragment fragment, List<TaskSource> sources)
    {
        boolean verboseStats = getVerboseStats(session);
        TaskContext taskContext = queryContext.addTaskContext(
                taskStateMachine,
                session,
                requireNonNull(operatorPreAllocatedMemory, "operatorPreAllocatedMemory is null"),
                verboseStats,
                cpuTimerEnabled);

        return createSqlTaskExecution(
                taskStateMachine,
                taskContext,
                outputBuffer,
                fragment,
                sources,
                planner,
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
