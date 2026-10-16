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
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.stats.Distribution;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.RuntimeUnit;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.DynamicFilterStats;
import com.facebook.presto.operator.OperatorInfo;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.common.RuntimeUnit.BYTE;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Builds task infos with randomized stats for tests of stage level aggregation.
 */
public final class TestingTaskInfos
{
    public static final JsonCodec<TaskInfo> TASK_INFO_CODEC = JsonCodec.jsonCodec(TaskInfo.class);

    private static final String[] METRIC_NAMES = {"aNanos", "bBytes", "cCount", "dNanos"};
    private static final RuntimeUnit[] METRIC_UNITS = {NANO, BYTE, NONE, NANO};
    private static final String[] DYNAMIC_FILTER_PRODUCERS = {"df1", "df2", "df3"};
    private static final long BASE_TIME_MILLIS = 1_760_000_000_000L;

    private static final ObjectMapper EXACT_MAPPER = new JsonObjectMapperProvider().get()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .registerModule(new SimpleModule()
                    .addSerializer(Duration.class, new ExactSerializer<>(duration -> duration.getValue() + " " + duration.getUnit()))
                    .addSerializer(DataSize.class, new ExactSerializer<>(dataSize -> dataSize.getValue() + " " + dataSize.getUnit())));

    private TestingTaskInfos() {}

    public static PipelineTemplate pipeline(double probability, boolean inputPipeline, boolean outputPipeline, OperatorTemplate... operators)
    {
        return new PipelineTemplate(probability, inputPipeline, outputPipeline, ImmutableList.copyOf(operators));
    }

    public static OperatorTemplate operator(String planNodeId, String operatorType, Function<Random, OperatorInfo> info)
    {
        return new OperatorTemplate(new PlanNodeId(planNodeId), operatorType, info);
    }

    /**
     * JSON in which every {@link Duration} and {@link DataSize} keeps its exact value and unit and
     * map entries are sorted by key, so that equal strings mean equal values.
     */
    public static String exactJson(Object value)
    {
        try {
            return EXACT_MAPPER.writeValueAsString(value);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static TaskInfo createTaskInfo(Random random, TaskId taskId, TaskState state, String nodeId, List<PipelineTemplate> pipelines)
    {
        ImmutableList.Builder<PipelineStats> pipelineStats = ImmutableList.builder();
        for (int pipelineId = 0; pipelineId < pipelines.size(); pipelineId++) {
            PipelineTemplate pipeline = pipelines.get(pipelineId);
            if (random.nextDouble() >= pipeline.probability) {
                continue;
            }
            ImmutableList.Builder<OperatorStats> operators = ImmutableList.builder();
            for (int operatorId = 0; operatorId < pipeline.operators.size(); operatorId++) {
                OperatorTemplate operator = pipeline.operators.get(operatorId);
                operators.add(createOperatorStats(
                        random,
                        taskId.getStageExecutionId().getStageId().getId(),
                        taskId.getStageExecutionId().getId(),
                        pipelineId,
                        operatorId,
                        operator.planNodeId,
                        operator.operatorType,
                        operator.info.apply(random)));
            }
            pipelineStats.add(createPipelineStats(random, pipelineId, pipeline.inputPipeline, pipeline.outputPipeline, operators.build()));
        }

        boolean done = state.isDone();
        long createTime = BASE_TIME_MILLIS + random.nextInt(100_000);
        TaskStats taskStats = new TaskStats(
                createTime,
                optionalTime(random, createTime + random.nextInt(1_000)),
                optionalTime(random, createTime + 1_000 + random.nextInt(1_000)),
                optionalTime(random, createTime + 2_000 + random.nextInt(1_000)),
                optionalTime(random, createTime + 3_000 + random.nextInt(100_000)),
                randomLong(random, 1L << 40),
                random.nextInt(4) == 0 ? 0 : randomLong(random, 1L << 30),
                random.nextInt(100),
                done ? 0 : random.nextInt(10),
                random.nextInt(10),
                random.nextInt(1_000),
                done ? 0 : random.nextInt(10),
                random.nextInt(10),
                random.nextInt(1_000),
                done ? 0 : random.nextInt(10),
                random.nextInt(100),
                random.nextInt(100),
                random.nextInt(10),
                random.nextInt(10),
                random.nextInt(100),
                random.nextInt(100),
                random.nextInt(10),
                random.nextInt(10),
                random.nextInt(100),
                random.nextDouble() * 1e15,
                random.nextDouble() * 1e16,
                randomLong(random, 1L << 34),
                randomLong(random, 1L << 30),
                randomLong(random, 1L << 34),
                randomLong(random, 1L << 36),
                randomLong(random, 1L << 35),
                randomLong(random, 1L << 37),
                randomLong(random, 1L << 42),
                randomLong(random, 1L << 42),
                random.nextInt(4) == 0 ? 0 : randomLong(random, 1L << 40),
                random.nextBoolean(),
                random.nextBoolean() ? ImmutableSet.of(WAITING_FOR_MEMORY) : ImmutableSet.of(),
                randomLong(random, 1L << 40),
                randomLong(random, 1L << 45),
                randomLong(random, 1L << 45),
                randomLong(random, 1L << 35),
                randomLong(random, 1L << 45),
                randomLong(random, 1L << 35),
                randomLong(random, 1L << 45),
                randomLong(random, 1L << 35),
                randomLong(random, 1L << 40),
                random.nextInt(3),
                random.nextInt(3) == 0 ? 0 : random.nextInt(20_000),
                pipelineStats.build(),
                randomRuntimeStats(random));

        TaskStatus taskStatus = new TaskStatus(
                random.nextLong(),
                random.nextLong(),
                random.nextInt(1_000),
                state,
                URI.create("http://" + nodeId + ".example.com:8080/v1/task/" + taskId),
                ImmutableSet.of(),
                state == TaskState.FAILED ? ImmutableList.of(failure()) : ImmutableList.of(),
                0,
                0,
                0.0,
                false,
                taskStats.getPhysicalWrittenDataSizeInBytes(),
                taskStats.getUserMemoryReservationInBytes(),
                taskStats.getSystemMemoryReservationInBytes(),
                taskStats.getPeakNodeTotalMemoryInBytes(),
                taskStats.getFullGcCount(),
                taskStats.getFullGcTimeInMillis(),
                taskStats.getTotalCpuTimeInNanos(),
                random.nextInt(100_000),
                0,
                0);

        OutputBufferInfo outputBuffers = new OutputBufferInfo(
                "PARTITIONED",
                done ? BufferState.FINISHED : BufferState.OPEN,
                false,
                !done,
                done ? 0 : randomLong(random, 1L << 30),
                random.nextInt(1_000),
                randomLong(random, 1L << 30),
                random.nextInt(1_000),
                ImmutableList.of());

        return new TaskInfo(taskId, taskStatus, createTime, outputBuffers, ImmutableSet.of(), taskStats, false, nodeId);
    }

    public static OperatorStats createOperatorStats(
            Random random,
            int stageId,
            int stageExecutionId,
            int pipelineId,
            int operatorId,
            PlanNodeId planNodeId,
            String operatorType,
            OperatorInfo info)
    {
        long inputPositions = randomLong(random, 1L << 30);
        return new OperatorStats(
                stageId,
                stageExecutionId,
                pipelineId,
                operatorId,
                planNodeId,
                operatorType,

                random.nextInt(100),

                randomLong(random, 1L << 20),
                randomDuration(random),
                randomDuration(random),
                randomLong(random, 1L << 30),

                randomLong(random, 1L << 20),
                randomDuration(random),
                randomDuration(random),
                randomLong(random, 1L << 56),
                randomLong(random, 1L << 56),
                randomLong(random, 1L << 30),
                randomLong(random, 1L << 56),
                inputPositions,
                (double) inputPositions * random.nextInt(1_000),

                randomLong(random, 1L << 20),
                randomDuration(random),
                randomDuration(random),
                randomLong(random, 1L << 56),
                randomLong(random, 1L << 56),
                randomLong(random, 1L << 30),

                randomLong(random, 1L << 56),

                randomDuration(random),
                randomDuration(random),

                randomLong(random, 1L << 20),
                randomDuration(random),
                randomDuration(random),
                randomLong(random, 1L << 30),

                randomLong(random, 1L << 56),
                randomLong(random, 1L << 40),
                randomLong(random, 1L << 56),
                randomLong(random, 1L << 56),
                randomLong(random, 1L << 56),
                randomLong(random, 1L << 56),

                randomLong(random, 1L << 56),

                random.nextInt(3) == 0 ? Optional.of(WAITING_FOR_MEMORY) : Optional.<BlockedReason>empty(),

                info,
                randomRuntimeStats(random),
                randomDynamicFilterStats(random),
                randomLong(random, 1L << 20),
                randomLong(random, 1L << 30),
                randomLong(random, 1L << 20),
                randomLong(random, 1L << 30));
    }

    private static PipelineStats createPipelineStats(Random random, int pipelineId, boolean inputPipeline, boolean outputPipeline, List<OperatorStats> operators)
    {
        return new PipelineStats(
                pipelineId,
                BASE_TIME_MILLIS + random.nextInt(1_000),
                BASE_TIME_MILLIS + 1_000 + random.nextInt(1_000),
                BASE_TIME_MILLIS + 2_000 + random.nextInt(1_000),
                inputPipeline,
                outputPipeline,
                random.nextInt(100),
                random.nextInt(10),
                random.nextInt(10),
                random.nextInt(1_000),
                random.nextInt(10),
                random.nextInt(10),
                random.nextInt(1_000),
                random.nextInt(10),
                random.nextInt(100),
                randomLong(random, 1L << 34),
                randomLong(random, 1L << 30),
                randomLong(random, 1L << 34),
                new Distribution().snapshot(),
                new Distribution().snapshot(),
                randomLong(random, 1L << 40),
                randomLong(random, 1L << 40),
                randomLong(random, 1L << 40),
                random.nextBoolean(),
                ImmutableSet.of(),
                randomLong(random, 1L << 40),
                randomLong(random, 1L << 45),
                randomLong(random, 1L << 35),
                randomLong(random, 1L << 45),
                randomLong(random, 1L << 35),
                randomLong(random, 1L << 45),
                randomLong(random, 1L << 35),
                randomLong(random, 1L << 40),
                operators,
                ImmutableList.of());
    }

    private static long optionalTime(Random random, long time)
    {
        return random.nextInt(5) == 0 ? 0 : time;
    }

    private static long randomLong(Random random, long bound)
    {
        return (random.nextLong() >>> 1) % bound;
    }

    private static Duration randomDuration(Random random)
    {
        switch (random.nextInt(4)) {
            case 0:
                return new Duration(random.nextInt(1_000_000_000), NANOSECONDS);
            case 1:
                return new Duration(random.nextInt(10_000_000) / 100.0, MILLISECONDS);
            case 2:
                return new Duration(random.nextInt(100_000) / 100.0, SECONDS);
            default:
                return new Duration(0, NANOSECONDS);
        }
    }

    private static RuntimeStats randomRuntimeStats(Random random)
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        for (int i = 0; i < METRIC_NAMES.length; i++) {
            if (random.nextBoolean()) {
                long min = randomLong(random, 1L << 20);
                long max = min + randomLong(random, 1L << 20);
                long count = 1 + random.nextInt(10);
                runtimeStats.mergeMetric(METRIC_NAMES[i], new RuntimeMetric(METRIC_NAMES[i], METRIC_UNITS[i], min + max * (count - 1), count, max, min));
            }
        }
        return runtimeStats;
    }

    private static DynamicFilterStats randomDynamicFilterStats(Random random)
    {
        Set<PlanNodeId> producers = new HashSet<>();
        for (String producer : DYNAMIC_FILTER_PRODUCERS) {
            if (random.nextInt(3) == 0) {
                producers.add(new PlanNodeId(producer));
            }
        }
        return new DynamicFilterStats(producers);
    }

    private static ExecutionFailureInfo failure()
    {
        return new ExecutionFailureInfo(
                "com.facebook.presto.spi.PrestoException",
                "Task failed",
                null,
                ImmutableList.of(),
                ImmutableList.of("com.facebook.presto.Example.run(Example.java:1)"),
                null,
                GENERIC_INTERNAL_ERROR.toErrorCode(),
                null,
                null);
    }

    public static class PipelineTemplate
    {
        private final double probability;
        private final boolean inputPipeline;
        private final boolean outputPipeline;
        private final List<OperatorTemplate> operators;

        private PipelineTemplate(double probability, boolean inputPipeline, boolean outputPipeline, List<OperatorTemplate> operators)
        {
            this.probability = probability;
            this.inputPipeline = inputPipeline;
            this.outputPipeline = outputPipeline;
            this.operators = requireNonNull(operators, "operators is null");
        }
    }

    public static class OperatorTemplate
    {
        private final PlanNodeId planNodeId;
        private final String operatorType;
        private final Function<Random, OperatorInfo> info;

        private OperatorTemplate(PlanNodeId planNodeId, String operatorType, Function<Random, OperatorInfo> info)
        {
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.operatorType = requireNonNull(operatorType, "operatorType is null");
            this.info = requireNonNull(info, "info is null");
        }
    }

    private static class ExactSerializer<T>
            extends JsonSerializer<T>
    {
        private final Function<T, String> formatter;

        private ExactSerializer(Function<T, String> formatter)
        {
            this.formatter = requireNonNull(formatter, "formatter is null");
        }

        @Override
        public void serialize(T value, JsonGenerator generator, SerializerProvider serializers)
                throws IOException
        {
            generator.writeString(formatter.apply(value));
        }
    }
}
