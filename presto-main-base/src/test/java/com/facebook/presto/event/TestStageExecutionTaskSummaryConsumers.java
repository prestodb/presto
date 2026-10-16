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
package com.facebook.presto.event;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.stats.Distribution;
import com.facebook.airlift.stats.Distribution.DistributionSnapshot;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageExecutionTaskSummary;
import com.facebook.presto.execution.StageExecutionTaskSummary.FailedTask;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageTaskStatsAggregator;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TestingTaskInfos.PipelineTemplate;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.HashCollisionsInfo;
import com.facebook.presto.operator.SplitOperatorInfo;
import com.facebook.presto.operator.TableFinishInfo;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.operator.repartition.PartitionedOutputInfo;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.eventlistener.ResourceDistribution;
import com.facebook.presto.spi.eventlistener.StageStatistics;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.planPrinter.PlanNodeStats;
import com.facebook.presto.testing.TestingHandle;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.execution.TestingTaskInfos.TASK_INFO_CODEC;
import static com.facebook.presto.execution.TestingTaskInfos.createTaskInfo;
import static com.facebook.presto.execution.TestingTaskInfos.exactJson;
import static com.facebook.presto.execution.TestingTaskInfos.operator;
import static com.facebook.presto.execution.TestingTaskInfos.pipeline;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.metadata.AbstractMockMetadata.dummyMetadata;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.planPrinter.PlanNodeStatsSummarizer.aggregateStageStats;
import static com.facebook.presto.sql.planner.planPrinter.PlanNodeStatsSummarizer.aggregateTaskStats;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonDistributedPlan;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textDistributedPlan;
import static com.facebook.presto.util.QueryInfoUtils.toStatementStats;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.abs;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestStageExecutionTaskSummaryConsumers
{
    private static final PlanBuilder PLAN_BUILDER = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), dummyMetadata());
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = createTestFunctionAndTypeManager();
    private static final QueryId QUERY_ID = new QueryId("query");
    private static final DistributionSnapshot SPLIT_DISTRIBUTION = new Distribution().snapshot();
    private static final JsonCodec<StatementStats> STATEMENT_STATS_CODEC = JsonCodec.jsonCodec(StatementStats.class);
    private static final JsonCodec<StageExecutionInfo> STAGE_EXECUTION_INFO_CODEC = JsonCodec.jsonCodec(StageExecutionInfo.class);

    private static final VariableReferenceExpression COLUMN = new VariableReferenceExpression(Optional.empty(), "column", BIGINT);
    private static final TableHandle TABLE_HANDLE = new TableHandle(
            new ConnectorId("testConnector"),
            new TestingTableHandle(),
            TestingTransactionHandle.create(),
            Optional.of(TestingHandle.INSTANCE));
    private static final Map<VariableReferenceExpression, ColumnHandle> ASSIGNMENTS = ImmutableMap.of(COLUMN, new TestingColumnHandle("column"));

    private static final TableScanNode SCAN_A = PLAN_BUILDER.tableScan(TABLE_HANDLE, ImmutableList.of(COLUMN), ASSIGNMENTS);
    private static final PlanNode FILTER_A = PLAN_BUILDER.filter(TRUE_CONSTANT, SCAN_A);
    private static final ProjectNode PROJECT_A = PLAN_BUILDER.project(FILTER_A, Assignments.of(COLUMN, COLUMN));
    private static final LimitNode LIMIT_A = PLAN_BUILDER.limit(1000, PROJECT_A);
    private static final TableScanNode SCAN_B = PLAN_BUILDER.tableScan(TABLE_HANDLE, ImmutableList.of(COLUMN), ASSIGNMENTS);
    private static final ProjectNode PROJECT_B = PLAN_BUILDER.project(SCAN_B, Assignments.of(COLUMN, COLUMN));
    private static final RemoteSourceNode REMOTE_1 = PLAN_BUILDER.remoteSource(
            new PlanNodeId("remote1"),
            ImmutableList.of(new PlanFragmentId(2), new PlanFragmentId(3)),
            ImmutableList.of(COLUMN));
    private static final LimitNode LIMIT_1 = PLAN_BUILDER.limit(100, REMOTE_1);
    private static final RemoteSourceNode REMOTE_0 = PLAN_BUILDER.remoteSource(new PlanNodeId("remote0"), ImmutableList.of(new PlanFragmentId(1)), ImmutableList.of(COLUMN));
    private static final OutputNode OUTPUT = PLAN_BUILDER.output(ImmutableList.of("column"), ImmutableList.of(COLUMN), REMOTE_0);

    private static final List<PipelineTemplate> ROOT_PIPELINES = ImmutableList.of(
            pipeline(1, true, true,
                    operator(REMOTE_0.getId().toString(), "ExchangeOperator", random -> null),
                    operator(OUTPUT.getId().toString(), "TableFinishOperator", random -> new TableFinishInfo("{}", false, new Duration(28.74, SECONDS), new Duration(1.5, SECONDS)))));
    private static final List<PipelineTemplate> INTERMEDIATE_PIPELINES = ImmutableList.of(
            pipeline(1, true, true,
                    operator(REMOTE_1.getId().toString(), "ExchangeOperator", random -> null),
                    operator(LIMIT_1.getId().toString(), "LimitOperator", random -> null),
                    operator(LIMIT_1.getId().toString(), "PartitionedOutputOperator", TestStageExecutionTaskSummaryConsumers::partitionedOutputInfo)));
    private static final List<PipelineTemplate> LEAF_A_PIPELINES = ImmutableList.of(
            pipeline(1, true, false,
                    operator(SCAN_A.getId().toString(), "TableScanOperator", random -> new SplitOperatorInfo("split-" + random.nextInt(100))),
                    operator(FILTER_A.getId().toString(), "FilterAndProjectOperator", random -> null),
                    operator(PROJECT_A.getId().toString(), "LocalExchangeSinkOperator", random -> null)),
            pipeline(1, false, true,
                    operator(PROJECT_A.getId().toString(), "LocalExchangeSourceOperator", random -> null),
                    operator(LIMIT_A.getId().toString(), "HashAggregationOperator", random -> new HashCollisionsInfo(random.nextDouble() * 1e6, random.nextDouble() * 1e9, random.nextDouble() * 1e6)),
                    operator(LIMIT_A.getId().toString(), "PartitionedOutputOperator", TestStageExecutionTaskSummaryConsumers::partitionedOutputInfo)));
    private static final List<PipelineTemplate> LEAF_B_PIPELINES = ImmutableList.of(
            pipeline(1, true, true,
                    operator(SCAN_B.getId().toString(), "TableScanOperator", random -> new SplitOperatorInfo("split-" + random.nextInt(100))),
                    operator(PROJECT_B.getId().toString(), "FilterAndProjectOperator", random -> null),
                    operator(PROJECT_B.getId().toString(), "PartitionedOutputOperator", TestStageExecutionTaskSummaryConsumers::partitionedOutputInfo)),
            pipeline(0.3, false, false,
                    operator(SCAN_B.getId().toString(), "TableScanOperator", random -> null)));

    @Test
    public void testConsumersSeeSameDataWithSummary()
    {
        for (int seed = 0; seed < 30; seed++) {
            Random random = new Random(seed);
            Map<Integer, List<byte[]>> encodedTasks = ImmutableMap.of(
                    0, encodeTasks(random, 0, 1, 0, ROOT_PIPELINES),
                    1, encodeTasks(random, 1, 2 + random.nextInt(5), 0.1, INTERMEDIATE_PIPELINES),
                    2, encodeTasks(random, 2, 10 + random.nextInt(40), 0.15, LEAF_A_PIPELINES),
                    3, encodeTasks(random, 3, 1 + random.nextInt(15), 0.15, LEAF_B_PIPELINES));

            StageInfo withTasks = rootStage(encodedTasks, false);
            StageInfo withSummaries = rootStage(encodedTasks, true);

            assertStageStatisticsEqual(stageStatistics(withSummaries), stageStatistics(withTasks));

            Optional<FailedTask> expectedFailedTask = QueryMonitor.findFailedTask(withTasks);
            Optional<FailedTask> actualFailedTask = QueryMonitor.findFailedTask(withSummaries);
            assertEquals(actualFailedTask.map(FailedTask::getTaskId), expectedFailedTask.map(FailedTask::getTaskId));
            assertEquals(actualFailedTask.map(FailedTask::getSelf), expectedFailedTask.map(FailedTask::getSelf));
            assertEquals(actualFailedTask.map(FailedTask::getNodeId), expectedFailedTask.map(FailedTask::getNodeId));

            assertEquals(textDistributedPlan(withSummaries, FUNCTION_AND_TYPE_MANAGER, TEST_SESSION, false), textDistributedPlan(withTasks, FUNCTION_AND_TYPE_MANAGER, TEST_SESSION, false));
            assertEquals(textDistributedPlan(withSummaries, FUNCTION_AND_TYPE_MANAGER, TEST_SESSION, true), textDistributedPlan(withTasks, FUNCTION_AND_TYPE_MANAGER, TEST_SESSION, true));
            assertEquals(jsonDistributedPlan(withSummaries, FUNCTION_AND_TYPE_MANAGER, TEST_SESSION), jsonDistributedPlan(withTasks, FUNCTION_AND_TYPE_MANAGER, TEST_SESSION));

            assertEquals(exactJson(byId(aggregateStageStats(getAllStages(Optional.of(withSummaries))))), exactJson(byId(aggregateStageStats(getAllStages(Optional.of(withTasks))))));

            assertEquals(STATEMENT_STATS_CODEC.toJson(toStatementStats(queryInfo(withSummaries))), STATEMENT_STATS_CODEC.toJson(toStatementStats(queryInfo(withTasks))));

            List<StageInfo> stagesWithTasks = getAllStages(Optional.of(withTasks));
            List<StageInfo> stagesWithSummaries = getAllStages(Optional.of(withSummaries));
            for (int i = 0; i < stagesWithTasks.size(); i++) {
                assertSummaryMatchesTasks(
                        stagesWithSummaries.get(i).getLatestAttemptExecutionInfo().getTaskSummary().get(),
                        stagesWithTasks.get(i).getLatestAttemptExecutionInfo().getTasks());
            }
        }
    }

    @Test
    public void testTaskSummaryLeavesTasksUnmodified()
    {
        // the join node has an input operator in both pipelines, so merging its plan node stats merges dynamic filter stats of two operators
        List<PipelineTemplate> pipelines = ImmutableList.of(
                pipeline(1, true, false,
                        operator(SCAN_B.getId().toString(), "TableScanOperator", random -> null),
                        operator("join", "HashBuilderOperator", random -> null)),
                pipeline(1, true, true,
                        operator(SCAN_A.getId().toString(), "TableScanOperator", random -> null),
                        operator("join", "LookupJoinOperator", random -> null),
                        operator(PROJECT_A.getId().toString(), "TaskOutputOperator", random -> null)));
        for (int seed = 0; seed < 20; seed++) {
            Random random = new Random(seed);
            List<byte[]> encodedTasks = encodeTasks(random, 2, 1 + random.nextInt(20), 0.1, pipelines);
            StageExecutionId stageExecutionId = new StageExecutionId(new StageId(QUERY_ID, 2), 0);
            List<TaskInfo> tasks = encodedTasks.stream().map(TASK_INFO_CODEC::fromJson).collect(toImmutableList());

            StageExecutionTaskSummary.Builder taskSummary = StageExecutionTaskSummary.builder();
            StageTaskStatsAggregator aggregator = new StageTaskStatsAggregator(new RuntimeStats());
            for (TaskInfo task : tasks) {
                taskSummary.addTask(task);
                aggregator.addTask(task);
            }
            StageExecutionTaskSummary summary = taskSummary.build();

            assertEquals(tasks.stream().map(TASK_INFO_CODEC::toJson).collect(toImmutableList()), encodedTasks.stream().map(TASK_INFO_CODEC::fromJson).map(TASK_INFO_CODEC::toJson).collect(toImmutableList()));
            List<TaskInfo> otherTasks = encodedTasks.stream().map(TASK_INFO_CODEC::fromJson).collect(toImmutableList());
            assertEquals(
                    exactJson(aggregator.build(stageExecutionId, StageExecutionState.FINISHED, 0, SPLIT_DISTRIBUTION, 0, 0, 1, 1)),
                    exactJson(StageExecutionInfo.create(stageExecutionId, StageExecutionState.FINISHED, Optional.empty(), otherTasks, 0, SPLIT_DISTRIBUTION, new RuntimeStats(), 0, 0, 1, 1).getStats()));
            assertEquals(exactJson(byId(summary.getPlanNodeStats())), exactJson(byId(aggregateTaskStats(otherTasks))));
        }
    }

    @Test
    public void testPlanNodeStatsFailureSurfacesInConsumers()
    {
        // stats of a plan node with hash collisions in one task only cannot be merged
        List<byte[]> encodedTasks = ImmutableList.of(
                TASK_INFO_CODEC.toJsonBytes(createTaskInfo(new Random(1), new TaskId(QUERY_ID.toString(), 2, 0, 0, 0), TaskState.FINISHED, "node", ImmutableList.of(pipeline(1, true, true,
                        operator(LIMIT_A.getId().toString(), "HashAggregationOperator", random -> new HashCollisionsInfo(1, 2, 3)))))),
                TASK_INFO_CODEC.toJsonBytes(createTaskInfo(new Random(2), new TaskId(QUERY_ID.toString(), 2, 0, 1, 0), TaskState.FINISHED, "node", ImmutableList.of(pipeline(1, true, true,
                        operator(LIMIT_A.getId().toString(), "HashAggregationOperator", random -> null))))));
        StageInfo withTasks = stage(2, fragment(2, LIMIT_A, SOURCE_DISTRIBUTION, SCAN_A.getId()), encodedTasks, false, ImmutableList.of());
        StageInfo withSummary = stage(2, fragment(2, LIMIT_A, SOURCE_DISTRIBUTION, SCAN_A.getId()), encodedTasks, true, ImmutableList.of());

        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> aggregateStageStats(ImmutableList.of(withTasks)));
        IllegalArgumentException actual = expectThrows(IllegalArgumentException.class, () -> aggregateStageStats(ImmutableList.of(withSummary)));
        assertEquals(actual.getMessage(), expected.getMessage());
        expectThrows(IllegalArgumentException.class, () -> textDistributedPlan(withSummary, FUNCTION_AND_TYPE_MANAGER, TEST_SESSION, false));
    }

    @Test
    public void testTaskSummaryIsNotSerialized()
    {
        Random random = new Random(1);
        StageInfo withSummaries = rootStage(
                ImmutableMap.of(
                        0, encodeTasks(random, 0, 1, 0, ROOT_PIPELINES),
                        1, encodeTasks(random, 1, 2, 0, INTERMEDIATE_PIPELINES),
                        2, encodeTasks(random, 2, 3, 0, LEAF_A_PIPELINES),
                        3, encodeTasks(random, 3, 3, 0, LEAF_B_PIPELINES)),
                true);
        StageExecutionInfo executionInfo = withSummaries.getLatestAttemptExecutionInfo();
        assertTrue(executionInfo.getTaskSummary().isPresent());
        assertTrue(executionInfo.getTasks().isEmpty());

        String json = STAGE_EXECUTION_INFO_CODEC.toJson(executionInfo);
        assertFalse(json.contains("taskSummary"), json);
        StageExecutionInfo deserialized = STAGE_EXECUTION_INFO_CODEC.fromJson(json);
        assertFalse(deserialized.getTaskSummary().isPresent());
        assertTrue(deserialized.getTasks().isEmpty());
    }

    private static List<byte[]> encodeTasks(Random random, int stageId, int taskCount, double failedFraction, List<PipelineTemplate> pipelines)
    {
        ImmutableList.Builder<byte[]> tasks = ImmutableList.builder();
        for (int task = 0; task < taskCount; task++) {
            TaskState state = random.nextDouble() < failedFraction ? TaskState.FAILED : TaskState.FINISHED;
            TaskId taskId = new TaskId(new StageExecutionId(new StageId(QUERY_ID, stageId), 0), task, state == TaskState.FAILED ? 0 : random.nextInt(2));
            tasks.add(TASK_INFO_CODEC.toJsonBytes(createTaskInfo(random, taskId, state, "node" + random.nextInt(6), pipelines)));
        }
        return tasks.build();
    }

    private static StageInfo rootStage(Map<Integer, List<byte[]>> encodedTasks, boolean summarize)
    {
        StageInfo leafA = stage(2, fragment(2, LIMIT_A, SOURCE_DISTRIBUTION, SCAN_A.getId()), encodedTasks.get(2), summarize, ImmutableList.of());
        StageInfo leafB = stage(3, fragment(3, PROJECT_B, SOURCE_DISTRIBUTION, SCAN_B.getId()), encodedTasks.get(3), summarize, ImmutableList.of());
        StageInfo intermediate = stage(1, fragment(1, LIMIT_1, FIXED_HASH_DISTRIBUTION), encodedTasks.get(1), summarize, ImmutableList.of(leafA, leafB));
        return stage(0, fragment(0, OUTPUT, SINGLE_DISTRIBUTION), encodedTasks.get(0), summarize, ImmutableList.of(intermediate));
    }

    private static StageInfo stage(int id, PlanFragment fragment, List<byte[]> encodedTasks, boolean summarize, List<StageInfo> subStages)
    {
        StageId stageId = new StageId(QUERY_ID, id);
        StageExecutionId stageExecutionId = new StageExecutionId(stageId, 0);
        long peakUserMemoryReservation = 1_000L * id;
        long peakNodeTotalMemoryReservation = 2_000L * id;

        StageExecutionInfo executionInfo;
        if (summarize) {
            StageTaskStatsAggregator aggregator = new StageTaskStatsAggregator(new RuntimeStats());
            StageExecutionTaskSummary.Builder taskSummary = StageExecutionTaskSummary.builder();
            for (byte[] encodedTask : encodedTasks) {
                TaskInfo taskInfo = TASK_INFO_CODEC.fromJson(encodedTask);
                aggregator.addTask(taskInfo);
                taskSummary.addTask(taskInfo);
            }
            executionInfo = StageExecutionInfo.createWithTaskSummary(
                    StageExecutionState.FINISHED,
                    aggregator.build(stageExecutionId, StageExecutionState.FINISHED, 0, SPLIT_DISTRIBUTION, peakUserMemoryReservation, peakNodeTotalMemoryReservation, 1, 1),
                    taskSummary.build(),
                    Optional.empty());
        }
        else {
            executionInfo = StageExecutionInfo.create(
                    stageExecutionId,
                    StageExecutionState.FINISHED,
                    Optional.empty(),
                    encodedTasks.stream().map(TASK_INFO_CODEC::fromJson).collect(toImmutableList()),
                    0,
                    SPLIT_DISTRIBUTION,
                    new RuntimeStats(),
                    peakUserMemoryReservation,
                    peakNodeTotalMemoryReservation,
                    1,
                    1);
        }
        return new StageInfo(stageId, URI.create("http://example.com/stage/" + stageId), Optional.of(fragment), executionInfo, ImmutableList.of(), subStages, false);
    }

    private static PlanFragment fragment(int id, PlanNode root, PartitioningHandle partitioning, PlanNodeId... tableScans)
    {
        return new PlanFragment(
                new PlanFragmentId(id),
                root,
                ImmutableSet.of(COLUMN),
                partitioning,
                ImmutableList.copyOf(tableScans),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(COLUMN)),
                Optional.empty(),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                Optional.of(StatsAndCosts.empty()),
                Optional.empty());
    }

    private static PartitionedOutputInfo partitionedOutputInfo(Random random)
    {
        return new PartitionedOutputInfo(random.nextInt(1_000_000), random.nextInt(1_000), random.nextInt(1_000_000));
    }

    private static List<StageStatistics> stageStatistics(StageInfo rootStage)
    {
        ImmutableList.Builder<StageStatistics> stageStatistics = ImmutableList.builder();
        QueryMonitor.computeStageStatistics(rootStage, stageStatistics);
        return stageStatistics.build();
    }

    private static void assertStageStatisticsEqual(List<StageStatistics> actual, List<StageStatistics> expected)
    {
        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            StageStatistics actualStage = actual.get(i);
            StageStatistics expectedStage = expected.get(i);
            assertEquals(actualStage.getStageId(), expectedStage.getStageId());
            assertEquals(actualStage.getStageExecutionId(), expectedStage.getStageExecutionId());
            assertEquals(actualStage.getTasks(), expectedStage.getTasks());
            assertEquals(actualStage.getTotalScheduledTime(), expectedStage.getTotalScheduledTime());
            assertEquals(actualStage.getTotalCpuTime(), expectedStage.getTotalCpuTime());
            assertEquals(actualStage.getRetriedCpuTime(), expectedStage.getRetriedCpuTime());
            assertEquals(actualStage.getTotalBlockedTime(), expectedStage.getTotalBlockedTime());
            assertEquals(actualStage.getRawInputDataSize(), expectedStage.getRawInputDataSize());
            assertEquals(actualStage.getProcessedInputDataSize(), expectedStage.getProcessedInputDataSize());
            assertEquals(actualStage.getPhysicalWrittenDataSize(), expectedStage.getPhysicalWrittenDataSize());
            assertEquals(exactJson(actualStage.getGcStatistics()), exactJson(expectedStage.getGcStatistics()));
            assertResourceDistributionEqual(actualStage.getCpuDistribution(), expectedStage.getCpuDistribution());
            assertResourceDistributionEqual(actualStage.getMemoryDistribution(), expectedStage.getMemoryDistribution());
        }
    }

    private static void assertResourceDistributionEqual(ResourceDistribution actual, ResourceDistribution expected)
    {
        assertEquals(actual.getP25(), expected.getP25());
        assertEquals(actual.getP50(), expected.getP50());
        assertEquals(actual.getP75(), expected.getP75());
        assertEquals(actual.getP90(), expected.getP90());
        assertEquals(actual.getP95(), expected.getP95());
        assertEquals(actual.getP99(), expected.getP99());
        assertEquals(actual.getMin(), expected.getMin());
        assertEquals(actual.getMax(), expected.getMax());
        assertEquals(actual.getTotal(), expected.getTotal());
        assertEquals(Double.compare(actual.getAverage(), expected.getAverage()), 0);
    }

    private static void assertSummaryMatchesTasks(StageExecutionTaskSummary summary, List<TaskInfo> tasks)
    {
        double average = tasks.stream().mapToLong(task -> task.getStats().getProcessedInputPositions()).average().orElse(Double.NaN);
        double standardDeviation = Math.sqrt(tasks.stream().mapToDouble(task -> Math.pow(task.getStats().getProcessedInputPositions() - average, 2)).sum() / tasks.size());
        assertEquals(Double.compare(summary.getProcessedInputPositionsAverage(), average), 0);
        assertTrue(abs(summary.getProcessedInputPositionsStdDev() - standardDeviation) <= 1e-9 * standardDeviation, summary.getProcessedInputPositionsStdDev() + " vs " + standardDeviation);

        long firstStartTime = Long.MAX_VALUE;
        long lastStartTime = Long.MIN_VALUE;
        long endTime = Long.MIN_VALUE;
        for (TaskInfo task : tasks) {
            TaskStats taskStats = task.getStats();
            if (taskStats.getFirstStartTimeInMillis() != 0) {
                firstStartTime = Math.min(firstStartTime, taskStats.getFirstStartTimeInMillis());
            }
            if (taskStats.getLastStartTimeInMillis() != 0) {
                lastStartTime = Math.max(lastStartTime, taskStats.getLastStartTimeInMillis());
            }
            if (taskStats.getEndTimeInMillis() != 0) {
                endTime = Math.max(endTime, taskStats.getEndTimeInMillis());
            }
        }
        assertEquals(summary.getMinFirstStartTimeInMillis(), firstStartTime == Long.MAX_VALUE ? 0 : firstStartTime);
        assertEquals(summary.getMaxLastStartTimeInMillis(), lastStartTime == Long.MIN_VALUE ? 0 : lastStartTime);
        assertEquals(summary.getMaxEndTimeInMillis(), endTime == Long.MIN_VALUE ? 0 : endTime);
        assertEquals(summary.getTaskCount(), tasks.size());
        assertEquals(summary.getNodeIds(), tasks.stream().map(TaskInfo::getNodeId).collect(ImmutableSet.toImmutableSet()));
    }

    private static Map<String, PlanNodeStats> byId(Map<PlanNodeId, PlanNodeStats> planNodeStats)
    {
        Map<String, PlanNodeStats> byId = new TreeMap<>();
        planNodeStats.forEach((planNodeId, stats) -> byId.put(planNodeId.toString(), stats));
        return byId;
    }

    private static QueryInfo queryInfo(StageInfo outputStage)
    {
        QueryStats queryStats = QueryStats.immediateFailureQueryStats();
        return new QueryInfo(
                QUERY_ID,
                TEST_SESSION.toSessionRepresentation(),
                FINISHED,
                new MemoryPoolId("memory_pool"),
                true,
                URI.create("http://example.com/query"),
                ImmutableList.of("column"),
                "SELECT 1",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                queryStats,
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                null,
                Optional.of(outputStage),
                null,
                null,
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                true,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty());
    }
}
