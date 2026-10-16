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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.stats.Distribution;
import com.facebook.airlift.stats.Distribution.DistributionSnapshot;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.TestingTaskInfos.OperatorTemplate;
import com.facebook.presto.execution.TestingTaskInfos.PipelineTemplate;
import com.facebook.presto.operator.HashCollisionsInfo;
import com.facebook.presto.operator.OperatorInfo;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.SplitOperatorInfo;
import com.facebook.presto.operator.TableFinishInfo;
import com.facebook.presto.operator.repartition.PartitionedOutputInfo;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.execution.TestingTaskInfos.TASK_INFO_CODEC;
import static com.facebook.presto.execution.TestingTaskInfos.createTaskInfo;
import static com.facebook.presto.execution.TestingTaskInfos.exactJson;
import static com.facebook.presto.execution.TestingTaskInfos.operator;
import static com.facebook.presto.execution.TestingTaskInfos.pipeline;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

public class TestStageTaskStatsAggregator
{
    private static final JsonCodec<StageExecutionStats> STATS_CODEC = JsonCodec.jsonCodec(StageExecutionStats.class);
    private static final DistributionSnapshot SPLIT_DISTRIBUTION = new Distribution().snapshot();
    private static final TaskState[] TASK_STATES = {
            TaskState.FINISHED, TaskState.FINISHED, TaskState.FINISHED, TaskState.FAILED, TaskState.ABORTED, TaskState.CANCELED, TaskState.RUNNING, TaskState.PLANNED};
    private static final StageExecutionState[] STAGE_STATES = {
            StageExecutionState.FINISHED, StageExecutionState.FAILED, StageExecutionState.RUNNING, StageExecutionState.ABORTED};

    @Test
    public void testMatchesCreate()
    {
        for (int seed = 0; seed < 20; seed++) {
            Random random = new Random(seed);
            StageExecutionId stageExecutionId = new StageExecutionId(new StageId("query", random.nextInt(10)), random.nextInt(3));
            List<PipelineTemplate> pipelines = randomPipelines(random);
            int taskCount = random.nextInt(4) == 0 ? random.nextInt(2) : random.nextInt(60);
            ImmutableList.Builder<byte[]> encodedTasks = ImmutableList.builder();
            for (int task = 0; task < taskCount; task++) {
                TaskId taskId = new TaskId(stageExecutionId, task, random.nextInt(2));
                TaskState state = TASK_STATES[random.nextInt(TASK_STATES.length)];
                encodedTasks.add(TASK_INFO_CODEC.toJsonBytes(createTaskInfo(random, taskId, state, "node" + random.nextInt(8), pipelines)));
            }
            StageExecutionState state = STAGE_STATES[random.nextInt(STAGE_STATES.length)];
            RuntimeStats stageRuntimeStats = new RuntimeStats();
            if (random.nextBoolean()) {
                stageRuntimeStats.mergeMetric("stageNanos", new RuntimeMetric("stageNanos", NANO, 10, 2, 7, 3));
            }
            long schedulingCompleteInMillis = random.nextInt(1_000_000);
            long peakUserMemoryReservation = random.nextInt(1_000_000);
            long peakNodeTotalMemoryReservation = random.nextInt(1_000_000);

            StageExecutionInfo expected = StageExecutionInfo.create(
                    stageExecutionId,
                    state,
                    Optional.empty(),
                    encodedTasks.build().stream().map(TASK_INFO_CODEC::fromJson).collect(toImmutableList()),
                    schedulingCompleteInMillis,
                    SPLIT_DISTRIBUTION,
                    stageRuntimeStats,
                    peakUserMemoryReservation,
                    peakNodeTotalMemoryReservation,
                    1,
                    2);

            StageTaskStatsAggregator aggregator = new StageTaskStatsAggregator(stageRuntimeStats);
            for (byte[] encodedTask : encodedTasks.build()) {
                aggregator.addTask(TASK_INFO_CODEC.fromJson(encodedTask));
            }
            StageExecutionStats actual = aggregator.build(
                    stageExecutionId,
                    state,
                    schedulingCompleteInMillis,
                    SPLIT_DISTRIBUTION,
                    peakUserMemoryReservation,
                    peakNodeTotalMemoryReservation,
                    1,
                    2);

            assertEquals(STATS_CODEC.toJson(actual), STATS_CODEC.toJson(expected.getStats()));
            assertEquals(exactJson(actual), exactJson(expected.getStats()));
        }
    }

    @Test
    public void testSingleTaskStageKeepsOperatorStats()
    {
        TableFinishInfo tableFinishInfo = new TableFinishInfo("{}", false, new Duration(28.74, SECONDS), new Duration(1.5, SECONDS));
        TaskInfo taskInfo = createTaskInfo(
                new Random(1),
                new TaskId("query", 0, 0, 0, 0),
                TaskState.FINISHED,
                "node",
                ImmutableList.of(pipeline(
                        1,
                        true,
                        true,
                        operator("1", "ExchangeOperator", random -> null),
                        operator("2", "TableFinishOperator", random -> tableFinishInfo))));

        StageTaskStatsAggregator aggregator = new StageTaskStatsAggregator(new RuntimeStats());
        aggregator.addTask(taskInfo);
        StageExecutionStats stats = build(aggregator, StageExecutionState.FINISHED);

        OperatorStats tableFinish = stats.getOperatorSummaries().stream()
                .filter(operatorStats -> operatorStats.getOperatorType().equals("TableFinishOperator"))
                .collect(onlyElement());
        assertSame(tableFinish, taskInfo.getStats().getPipelines().get(0).getOperatorSummaries().get(1));
        assertSame(tableFinish.getInfo(), tableFinishInfo);
    }

    @Test
    public void testRetriedCpuTimeOnlyForFinishedStage()
    {
        List<PipelineTemplate> pipelines = ImmutableList.of(pipeline(1, true, true, operator("1", "TableScanOperator", random -> null)));
        Random random = new Random(2);
        TaskInfo failed = createTaskInfo(random, new TaskId("query", 1, 0, 0, 0), TaskState.FAILED, "node", pipelines);
        TaskInfo finished = createTaskInfo(random, new TaskId("query", 1, 0, 0, 1), TaskState.FINISHED, "node", pipelines);

        for (StageExecutionState state : STAGE_STATES) {
            StageTaskStatsAggregator aggregator = new StageTaskStatsAggregator(new RuntimeStats());
            aggregator.addTask(failed);
            aggregator.addTask(finished);
            long expectedNanos = state == StageExecutionState.FINISHED ? failed.getStats().getTotalCpuTimeInNanos() : 0;
            assertEquals(build(aggregator, state).getRetriedCpuTime().roundTo(NANOSECONDS), expectedNanos);
        }
    }

    @Test
    public void testAddAfterBuild()
    {
        StageTaskStatsAggregator aggregator = new StageTaskStatsAggregator(new RuntimeStats());
        build(aggregator, StageExecutionState.FINISHED);
        TaskInfo taskInfo = createTaskInfo(new Random(3), new TaskId("query", 1, 0, 0, 0), TaskState.FINISHED, "node", ImmutableList.of());
        expectThrows(IllegalStateException.class, () -> aggregator.addTask(taskInfo));
    }

    private static StageExecutionStats build(StageTaskStatsAggregator aggregator, StageExecutionState state)
    {
        return aggregator.build(new StageExecutionId(new StageId("query", 1), 0), state, 0, SPLIT_DISTRIBUTION, 0, 0, 1, 1);
    }

    private static List<PipelineTemplate> randomPipelines(Random random)
    {
        ImmutableList.Builder<PipelineTemplate> pipelines = ImmutableList.builder();
        int pipelineCount = 1 + random.nextInt(4);
        for (int pipelineId = 0; pipelineId < pipelineCount; pipelineId++) {
            int operatorCount = 1 + random.nextInt(5);
            OperatorTemplate[] operators = new OperatorTemplate[operatorCount];
            for (int operatorId = 0; operatorId < operatorCount; operatorId++) {
                operators[operatorId] = operator(String.valueOf(random.nextInt(6)), "Operator" + operatorId, randomInfo(random));
            }
            pipelines.add(pipeline(random.nextInt(3) == 0 ? 0.5 : 1, pipelineId == 0, pipelineId == pipelineCount - 1, operators));
        }
        return pipelines.build();
    }

    private static Function<Random, OperatorInfo> randomInfo(Random random)
    {
        switch (random.nextInt(6)) {
            case 0:
                return infoRandom -> new PartitionedOutputInfo(infoRandom.nextInt(1_000), infoRandom.nextInt(1_000), infoRandom.nextInt(1_000_000));
            case 1:
                return infoRandom -> new HashCollisionsInfo(infoRandom.nextDouble() * 1e6, infoRandom.nextDouble() * 1e9, infoRandom.nextDouble() * 1e6);
            case 2:
                return infoRandom -> new SplitOperatorInfo("split-" + infoRandom.nextInt(10));
            case 3:
                return infoRandom -> new TableFinishInfo("{}", false, new Duration(infoRandom.nextInt(10_000) / 100.0, SECONDS), new Duration(1, SECONDS));
            case 4:
                return infoRandom -> infoRandom.nextBoolean() ? new PartitionedOutputInfo(1, 2, 3) : new SplitOperatorInfo("mixed");
            default:
                return infoRandom -> null;
        }
    }
}
