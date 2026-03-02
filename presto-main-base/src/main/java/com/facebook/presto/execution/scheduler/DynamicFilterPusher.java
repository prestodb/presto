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
package com.facebook.presto.execution.scheduler;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.spi.plan.PlanNodeId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PUSH_TO_WORKER_COUNT;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PUSH_TO_WORKER_TASK_COUNT;
import static com.facebook.presto.common.RuntimeUnit.NONE;

/**
 * Pushes coordinator-collected dynamic filters to probe-side C++ workers
 * for row-group and row-level filtering within Velox.
 *
 * <p>Fire-and-forget: push failures do not affect query correctness (workers
 * that don't support the endpoint return 404, which is silently ignored).
 * Early splits still benefit from Phase 1 split pruning.
 */
public class DynamicFilterPusher
{
    private static final Logger log = Logger.get(DynamicFilterPusher.class);

    private final ConcurrentMap<String, ResolvedFilter> resolvedFilters = new ConcurrentHashMap<>();
    private final RuntimeStats runtimeStats;
    private final boolean extendedMetrics;
    private final ConcurrentMap<String, Boolean> pushedTaskIds = new ConcurrentHashMap<>();

    public DynamicFilterPusher(RuntimeStats runtimeStats, boolean extendedMetrics)
    {
        this.runtimeStats = runtimeStats;
        this.extendedMetrics = extendedMetrics;
    }

    /**
     * Registers a callback on the given {@link JoinDynamicFilter} so that when
     * the filter is fully resolved, its constraint is pushed to all tasks in
     * the probe-side {@link SqlStageExecution}.
     *
     * <p>Also registers a task-created listener so tasks created after filter
     * completion receive the push too.
     */
    public void startPushing(
            String filterId,
            PlanNodeId scanNodeId,
            JoinDynamicFilter joinFilter,
            SqlStageExecution probeStageExecution)
    {
        joinFilter.onFullyResolved((resolvedFilterId, constraint) -> {
            if (constraint.isAll()) {
                return;
            }
            resolvedFilters.put(resolvedFilterId, new ResolvedFilter(scanNodeId, constraint));
            for (RemoteTask task : probeStageExecution.getAllTasks()) {
                pushToTask(task, scanNodeId, resolvedFilterId, constraint);
            }
        });

        probeStageExecution.addTaskCreatedListener(task -> {
            ResolvedFilter resolved = resolvedFilters.get(filterId);
            if (resolved != null) {
                pushToTask(task, resolved.scanNodeId, filterId, resolved.constraint);
            }
        });
    }

    private void pushToTask(RemoteTask task, PlanNodeId scanNodeId, String filterId, TupleDomain<String> constraint)
    {
        try {
            task.pushDynamicFilter(scanNodeId, filterId, constraint);
            if (extendedMetrics) {
                runtimeStats.addMetricValue(DYNAMIC_FILTER_PUSH_TO_WORKER_COUNT, NONE, 1);
                if (pushedTaskIds.putIfAbsent(task.getTaskId().toString(), Boolean.TRUE) == null) {
                    runtimeStats.addMetricValue(DYNAMIC_FILTER_PUSH_TO_WORKER_TASK_COUNT, NONE, 1);
                }
            }
        }
        catch (Exception e) {
            log.warn(e, "Failed to push dynamic filter %s to task %s (fire-and-forget)", filterId, task.getTaskId());
        }
    }

    private static class ResolvedFilter
    {
        final PlanNodeId scanNodeId;
        final TupleDomain<String> constraint;

        ResolvedFilter(PlanNodeId scanNodeId, TupleDomain<String> constraint)
        {
            this.scanNodeId = scanNodeId;
            this.constraint = constraint;
        }
    }
}
