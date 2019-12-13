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
package com.facebook.presto.spark.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.SqlQueryScheduler;
import com.facebook.presto.execution.scheduler.SqlQueryScheduler.StreamingSubPlan;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.operator.StageExecutionDescriptor.StageExecutionStrategy;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.execution.scheduler.SqlQueryScheduler.extractStreamingSections;
import static com.facebook.presto.execution.scheduler.TableWriteInfo.createTableWriteInfo;
import static com.facebook.presto.operator.StageExecutionDescriptor.StageExecutionStrategy.UNGROUPED_EXECUTION;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SparkPlanPreparer
{
    private static final int SPLIT_BATCH_SIZE = 1000;

    private final SplitManager splitManager;
    private final Metadata metadata;

    @Inject
    public SparkPlanPreparer(SplitManager splitManager, Metadata metadata)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public PreparedPlan preparePlan(Session session, SubPlan plan)
    {
        SqlQueryScheduler.StreamingPlanSection streamingPlanSection = extractStreamingSections(plan);
        checkState(streamingPlanSection.getChildren().isEmpty(), "expected no materialized exchanges");
        StreamingSubPlan streamingSubPlan = streamingPlanSection.getPlan();
        return new PreparedPlan(resolveTaskSources(session, plan), createTableWriteInfo(streamingSubPlan, metadata, session));
    }

    private SubPlanWithTaskSources resolveTaskSources(Session session, SubPlan plan)
    {
        return new SubPlanWithTaskSources(
                plan.getFragment(),
                resolveTaskSources(session, plan.getFragment()),
                plan.getChildren().stream()
                        .map(children -> resolveTaskSources(session, children))
                        .collect(toImmutableList()));
    }

    private List<TaskSource> resolveTaskSources(Session session, PlanFragment fragment)
    {
        StageExecutionDescriptor stageExecutionDescriptor = fragment.getStageExecutionDescriptor();
        StageExecutionStrategy stageExecutionStrategy = stageExecutionDescriptor.getStageExecutionStrategy();
        checkArgument(stageExecutionStrategy == UNGROUPED_EXECUTION, "Unsupported stage execution strategy: %s", stageExecutionStrategy);

        List<TaskSource> sources = new ArrayList<>();
        long sequenceId = 0;
        for (TableScanNode tableScan : findTableScanNodes(fragment.getRoot())) {
            SplitSource splitSource = splitManager.getSplits(session, tableScan.getTable(), UNGROUPED_SCHEDULING);
            ImmutableSet.Builder<ScheduledSplit> scheduledSplits = ImmutableSet.builder();
            while (!splitSource.isFinished()) {
                for (Split split : getNextBatch(splitSource)) {
                    scheduledSplits.add(new ScheduledSplit(sequenceId++, tableScan.getId(), split));
                }
            }
            sources.add(new TaskSource(tableScan.getId(), scheduledSplits.build(), true));
        }
        return sources;
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll();
    }

    private static List<Split> getNextBatch(SplitSource splitSource)
    {
        return getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), SPLIT_BATCH_SIZE)).getSplits();
    }
}
