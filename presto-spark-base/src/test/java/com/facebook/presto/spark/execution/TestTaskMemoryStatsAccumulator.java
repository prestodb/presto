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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateTimer;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionStats;
import com.facebook.presto.execution.StageExecutionTaskSummary;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spark.PrestoSparkQueryExecutionFactory;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.execution.TaskState.FINISHED;
import static com.facebook.presto.execution.TaskTestUtils.PLAN_FRAGMENT;
import static com.facebook.presto.spark.execution.TaskInfoDeduplicator.getFragmentId;
import static com.facebook.presto.spark.execution.TestingTaskInfos.QUERY_ID;
import static com.facebook.presto.spark.execution.TestingTaskInfos.createTaskInfo;
import static com.facebook.presto.spark.execution.TestingTaskInfos.createTaskStats;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Ticker.systemTicker;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTaskMemoryStatsAccumulator
{
    private static final SubPlan PLAN = new SubPlan(
            createPlanFragment(0),
            ImmutableList.of(
                    new SubPlan(createPlanFragment(1), ImmutableList.of(new SubPlan(createPlanFragment(3), ImmutableList.of()))),
                    new SubPlan(createPlanFragment(2), ImmutableList.of())));
    private static final JsonCodec<StageExecutionStats> STAGE_EXECUTION_STATS_CODEC = jsonCodec(StageExecutionStats.class);

    @Test
    public void testTaskStats()
    {
        TaskStats taskStats = createTaskStats(1, 2, 3, 4);
        assertEquals(taskStats.getUserMemoryReservationInBytes(), 1);
        assertEquals(taskStats.getPeakTotalMemoryInBytes(), 2);
        assertEquals(taskStats.getPeakUserMemoryInBytes(), 3);
        assertEquals(taskStats.getPeakNodeTotalMemoryInBytes(), 4);
    }

    @Test
    public void testMatchesStageAndQueryInfo()
    {
        List<TaskInfo> taskInfos = createTaskInfos();
        Map<Integer, TaskMemoryStatsAccumulator> accumulators = new HashMap<>();
        for (TaskInfo taskInfo : taskInfos) {
            accumulators.computeIfAbsent(getFragmentId(taskInfo), ignored -> new TaskMemoryStatsAccumulator()).addTask(taskInfo);
        }

        StageInfo rootStage = PrestoSparkQueryExecutionFactory.createStageInfo(QUERY_ID, PLAN, taskInfos);
        TaskMemoryStatsAccumulator queryAccumulator = new TaskMemoryStatsAccumulator();
        for (StageInfo stage : rootStage.getAllStages()) {
            TaskMemoryStatsAccumulator accumulator = accumulators.getOrDefault(stage.getStageId().getId(), new TaskMemoryStatsAccumulator());
            StageExecutionStats stageStats = stage.getLatestAttemptExecutionInfo().getStats();
            assertEquals(accumulator.getTaskCount(), stageStats.getTotalTasks());
            assertEquals(accumulator.getTotalUserMemoryReservationInBytes(), stageStats.getPeakUserMemoryReservationInBytes());
            assertEquals(accumulator.getMaxPeakNodeTotalMemoryInBytes(), stageStats.getPeakNodeTotalMemoryReservationInBytes());
            queryAccumulator.merge(accumulator);
        }
        assertEquals(queryAccumulator.getTaskCount(), taskInfos.size());

        QueryStats queryStats = createQueryInfo(rootStage).getQueryStats();
        assertEquals(queryAccumulator.getTaskCount(), queryStats.getPeakRunningTasks());
        assertEquals(queryAccumulator.getTotalPeakUserMemoryInBytes(), queryStats.getPeakUserMemoryReservation().toBytes());
        assertEquals(queryAccumulator.getTotalPeakTotalMemoryInBytes(), queryStats.getPeakTotalMemoryReservation().toBytes());
        assertEquals(queryAccumulator.getMaxPeakUserMemoryInBytes(), queryStats.getPeakTaskUserMemory().toBytes());
        assertEquals(queryAccumulator.getMaxPeakTotalMemoryInBytes(), queryStats.getPeakTaskTotalMemory().toBytes());
        assertEquals(queryAccumulator.getMaxPeakNodeTotalMemoryInBytes(), queryStats.getPeakNodeTotalMemory().toBytes());
    }

    @Test
    public void testFragmentTaskAggregatesMatchTaskInfos()
    {
        List<TaskInfo> taskInfos = createTaskInfos();
        FragmentTaskAggregatesSink sink = new FragmentTaskAggregatesSink();
        for (TaskInfo taskInfo : taskInfos) {
            sink.addTask(getFragmentId(taskInfo), taskInfo);
        }
        Map<PlanFragmentId, FragmentTaskAggregates> fragmentTaskAggregates = sink.seal();

        StageInfo rootStage = PrestoSparkQueryExecutionFactory.createStageInfo(QUERY_ID, PLAN, taskInfos);
        StageInfo aggregatedRootStage = PrestoSparkQueryExecutionFactory.createStageInfo(QUERY_ID, PLAN, fragmentTaskAggregates);
        List<StageInfo> stages = rootStage.getAllStages();
        List<StageInfo> aggregatedStages = aggregatedRootStage.getAllStages();
        assertEquals(aggregatedStages.size(), stages.size());
        for (int i = 0; i < stages.size(); i++) {
            StageExecutionInfo executionInfo = stages.get(i).getLatestAttemptExecutionInfo();
            StageExecutionInfo aggregatedExecutionInfo = aggregatedStages.get(i).getLatestAttemptExecutionInfo();
            assertEquals(aggregatedStages.get(i).getStageId(), stages.get(i).getStageId());
            assertEquals(aggregatedExecutionInfo.getState(), executionInfo.getState());
            assertEquals(withoutSchedulingTime(aggregatedExecutionInfo.getStats()), withoutSchedulingTime(executionInfo.getStats()));
            assertTrue(aggregatedExecutionInfo.getTasks().isEmpty());
            // fragments without tasks are built as without the aggregates
            assertEquals(aggregatedExecutionInfo.getTaskSummary().map(StageExecutionTaskSummary::getTaskCount), executionInfo.getTasks().isEmpty() ? Optional.empty() : Optional.of(executionInfo.getTasks().size()));
        }

        QueryStats queryStats = createQueryInfo(rootStage).getQueryStats();
        QueryStats aggregatedQueryStats = PrestoSparkQueryExecutionFactory.createQueryInfo(
                testSessionBuilder().build(),
                "SELECT 1",
                QueryState.FINISHED,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                new QueryStateTimer(systemTicker()),
                Optional.of(aggregatedRootStage),
                WarningCollector.NOOP,
                fragmentTaskAggregates)
                .getQueryStats();
        assertEquals(aggregatedQueryStats.getPeakRunningTasks(), queryStats.getPeakRunningTasks());
        assertEquals(aggregatedQueryStats.getPeakUserMemoryReservation(), queryStats.getPeakUserMemoryReservation());
        assertEquals(aggregatedQueryStats.getPeakTotalMemoryReservation(), queryStats.getPeakTotalMemoryReservation());
        assertEquals(aggregatedQueryStats.getPeakTaskUserMemory(), queryStats.getPeakTaskUserMemory());
        assertEquals(aggregatedQueryStats.getPeakTaskTotalMemory(), queryStats.getPeakTaskTotalMemory());
        assertEquals(aggregatedQueryStats.getPeakNodeTotalMemory(), queryStats.getPeakNodeTotalMemory());
        assertEquals(aggregatedQueryStats.getTotalTasks(), queryStats.getTotalTasks());
    }

    // fragment 3 has no tasks
    private static List<TaskInfo> createTaskInfos()
    {
        Random random = new Random(42);
        List<TaskInfo> taskInfos = new ArrayList<>();
        for (int fragmentId = 0; fragmentId < 3; fragmentId++) {
            int taskCount = 1 + random.nextInt(50);
            for (int partition = 0; partition < taskCount; partition++) {
                TaskStats taskStats = createTaskStats(randomBytes(random), randomBytes(random), randomBytes(random), randomBytes(random));
                taskInfos.add(createTaskInfo(fragmentId, partition, 0, FINISHED, "task", 0, taskStats));
            }
        }
        return taskInfos;
    }

    private static QueryInfo createQueryInfo(StageInfo rootStage)
    {
        return PrestoSparkQueryExecutionFactory.createQueryInfo(
                testSessionBuilder().build(),
                "SELECT 1",
                QueryState.FINISHED,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                new QueryStateTimer(systemTicker()),
                Optional.of(rootStage),
                WarningCollector.NOOP);
    }

    private static String withoutSchedulingTime(StageExecutionStats stats)
    {
        return STAGE_EXECUTION_STATS_CODEC.toJson(stats).replaceAll("\"schedulingCompleteInMillis\"\\s*:\\s*\\d+", "");
    }

    private static long randomBytes(Random random)
    {
        return random.nextInt(1 << 30);
    }

    private static PlanFragment createPlanFragment(int id)
    {
        return new PlanFragment(
                new PlanFragmentId(id),
                PLAN_FRAGMENT.getRoot(),
                PLAN_FRAGMENT.getVariables(),
                PLAN_FRAGMENT.getPartitioning(),
                PLAN_FRAGMENT.getTableScanSchedulingOrder(),
                PLAN_FRAGMENT.getPartitioningScheme(),
                PLAN_FRAGMENT.getOutputOrderingScheme(),
                PLAN_FRAGMENT.getStageExecutionDescriptor(),
                PLAN_FRAGMENT.isOutputTableWriterFragment(),
                PLAN_FRAGMENT.getStatsAndCosts(),
                PLAN_FRAGMENT.getJsonRepresentation());
    }
}
