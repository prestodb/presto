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
package com.facebook.presto.operator;

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.client.ClientCapabilities;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.PartitionedOutputBuffer;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.operator.OptimizedPartitionedOutputOperator.OptimizedPartitionedOutputFactory;
import com.facebook.presto.operator.PartitionedOutputOperator.PartitionedOutputFactory;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingTaskContext;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.operator.TestingBlockBuilders.createMapType;
import static com.facebook.presto.operator.TestingPageBuilders.buildDictionaryPage;
import static com.facebook.presto.operator.TestingPageBuilders.buildPage;
import static com.facebook.presto.operator.TestingPageBuilders.buildRlePage;
import static com.facebook.presto.operator.TestingPageBuilders.buildTypes;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RowType.withDefaultFieldNames;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Arrays.stream;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPartitionedOutputOperator
{
    @Benchmark
    public void addPage(BenchmarkData data)
    {
        PartitionedOutputOperator operator = data.createPartitionedOutputOperator();
        for (int i = 0; i < data.pageCount; i++) {
            operator.addInput(data.dataPage);
        }
        operator.finish();
    }

    @Benchmark
    public void optimizedAddPage(BenchmarkData data)
    {
        OptimizedPartitionedOutputOperator operator = data.createOptimizedPartitionedOutputOperator();
        for (int i = 0; i < data.pageCount; i++) {
            operator.addInput(data.dataPage);
        }
        operator.finish();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int PARTITION_COUNT = 256;
        private static final int POSITION_COUNT = 8192;
        private static final DataSize MAX_MEMORY = new DataSize(4, GIGABYTE);
        private static final DataSize MAX_PARTITION_BUFFER_SIZE = new DataSize(256, MEGABYTE);
        private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-EXECUTOR-%s"));
        private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("test-%s"));

        @Param({"1", "2"})
        private int channelCount;

        @Param({
                "BIGINT",
                "DICTIONARY(BIGINT)",
                "RLE(BIGINT)",
                "LONG_DECIMAL",
                "INTEGER",
                "SMALLINT",
                "BOOLEAN",
                "VARCHAR",
                "ARRAY(BIGINT)",
                "ARRAY(VARCHAR)",
                "ARRAY(ARRAY(BIGINT))",
                "MAP(BIGINT,BIGINT)",
                "MAP(BIGINT,MAP(BIGINT,BIGINT))",
                "ROW(BIGINT,BIGINT)",
                "ROW(ARRAY(BIGINT),ARRAY(BIGINT))"})
        private String type = "ARRAY(VARCHAR)";

        @Param({"true", "false"})
        private boolean hasNull;

        private List<Type> types;
        private int pageCount;
        private Page dataPage;

        @Setup
        public void setup()
        {
            createPages(type);
        }

        private void createPages(String inputType)
        {
            switch (inputType) {
                case "BIGINT":
                    types = nCopies(channelCount, BIGINT);
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 5000;
                    break;
                case "DICTIONARY(BIGINT)":
                    types = nCopies(channelCount, BIGINT);
                    dataPage = buildDictionaryPage(types, POSITION_COUNT, hasNull);
                    pageCount = 3000;
                    break;
                case "RLE(BIGINT)":
                    types = nCopies(channelCount, BIGINT);
                    dataPage = buildRlePage(types, POSITION_COUNT, hasNull);
                    pageCount = 3000;
                    break;
                case "LONG_DECIMAL":
                    types = nCopies(channelCount, createDecimalType(MAX_SHORT_PRECISION + 1));
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 5000;
                    break;
                case "INTEGER":
                    types = nCopies(channelCount, INTEGER);
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 5000;
                    break;
                case "SMALLINT":
                    types = nCopies(channelCount, SMALLINT);
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 5000;
                    break;
                case "BOOLEAN":
                    types = nCopies(channelCount, BOOLEAN);
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 5000;
                    break;
                case "VARCHAR":
                    types = nCopies(channelCount, VARCHAR);
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 5000;
                    break;
                case "ARRAY(BIGINT)":
                    types = nCopies(channelCount, new ArrayType(BIGINT));
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 1000;
                    break;
                case "ARRAY(VARCHAR)":
                    types = nCopies(channelCount, new ArrayType(VARCHAR));
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 1000;
                    break;
                case "ARRAY(ARRAY(BIGINT))":
                    types = nCopies(channelCount, new ArrayType(new ArrayType(BIGINT)));
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 1000;
                    break;
                case "MAP(BIGINT,BIGINT)":
                    types = nCopies(channelCount, createMapType(BIGINT, BIGINT));
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 1000;
                    break;
                case "MAP(BIGINT,MAP(BIGINT,BIGINT))":
                    types = nCopies(channelCount, createMapType(BIGINT, createMapType(BIGINT, BIGINT)));
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 1000;
                    break;
                case "ROW(BIGINT,BIGINT)":
                    types = nCopies(channelCount, withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT)));
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 1000;
                    break;
                case "ROW(ARRAY(BIGINT),ARRAY(BIGINT))":
                    types = nCopies(channelCount, withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT))));
                    dataPage = buildPage(types, POSITION_COUNT, hasNull);
                    pageCount = 1000;
                    break;

                default:
                    throw new UnsupportedOperationException("Unsupported dataType");
            }
            // We built the dataPage with added pre-computed hash block at channel 0, so types needs to be udpated
            types = buildTypes(types, true, false);
        }

        private PartitionedOutputBuffer createPartitionedOutputBuffer()
        {
            OutputBuffers buffers = createInitialEmptyOutputBuffers(PARTITIONED);
            for (int partition = 0; partition < PARTITION_COUNT; partition++) {
                buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
            }
            PartitionedOutputBuffer buffer = createPartitionedBuffer(
                    buffers.withNoMoreBufferIds(),
                    new DataSize(Long.MAX_VALUE, BYTE)); // don't let output buffer block
            buffer.registerLifespanCompletionCallback(ignore -> {});

            return buffer;
        }

        private OptimizedPartitionedOutputOperator createOptimizedPartitionedOutputOperator()
        {
            PartitionFunction partitionFunction = new LocalPartitionGenerator(new PrecomputedHashGenerator(0), PARTITION_COUNT);
            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new BlockEncodingManager(new TypeRegistry()), false);
            PartitionedOutputBuffer buffer = createPartitionedOutputBuffer();

            OptimizedPartitionedOutputFactory operatorFactory = new OptimizedPartitionedOutputFactory(
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty()),
                    false,
                    OptionalInt.empty(),
                    buffer,
                    MAX_PARTITION_BUFFER_SIZE);

            return (OptimizedPartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), types, Function.identity(), serdeFactory)
                    .createOperator(createDriverContext());
        }

        private PartitionedOutputOperator createPartitionedOutputOperator()
        {
            PartitionFunction partitionFunction = new LocalPartitionGenerator(new PrecomputedHashGenerator(0), PARTITION_COUNT);
            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new BlockEncodingManager(new TypeRegistry()), false);
            PartitionedOutputBuffer buffer = createPartitionedOutputBuffer();

            PartitionedOutputFactory operatorFactory = new PartitionedOutputFactory(
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty()),
                    false,
                    OptionalInt.empty(),
                    buffer,
                    MAX_PARTITION_BUFFER_SIZE);

            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), types, Function.identity(), serdeFactory)
                    .createOperator(createDriverContext());
        }

        private DriverContext createDriverContext()
        {
            Session testSession = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema(TINY_SCHEMA_NAME)
                    .setClientCapabilities(stream(ClientCapabilities.values())
                            .map(ClientCapabilities::toString)
                            .collect(toImmutableSet()))
                    .build();

            return TestingTaskContext.builder(EXECUTOR, SCHEDULER, testSession)
                    .setMemoryPoolSize(MAX_MEMORY)
                    .setQueryMaxTotalMemory(MAX_MEMORY)
                    .build()
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();
        }

        private TestingPartitionedOutputBuffer createPartitionedBuffer(OutputBuffers buffers, DataSize dataSize)
        {
            return new TestingPartitionedOutputBuffer(
                    "task-instance-id",
                    new StateMachine<>("bufferState", SCHEDULER, OPEN, TERMINAL_BUFFER_STATES),
                    buffers,
                    dataSize,
                    () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                    SCHEDULER);
        }

        private static class TestingPartitionedOutputBuffer
                extends PartitionedOutputBuffer
        {
            public TestingPartitionedOutputBuffer(
                    String taskInstanceId,
                    StateMachine<BufferState> state,
                    OutputBuffers outputBuffers,
                    DataSize maxBufferSize,
                    Supplier<LocalMemoryContext> systemMemoryContextSupplier,
                    Executor notificationExecutor)
            {
                super(taskInstanceId, state, outputBuffers, maxBufferSize, systemMemoryContextSupplier, notificationExecutor);
            }

            // Use a dummy enqueue method to avoid OutOfMemory error
            @Override
            public void enqueue(Lifespan lifespan, int partitionNumber, List<SerializedPage> pages)
            {
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkPartitionedOutputOperator().optimizedAddPage(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .jvmArgs("-Xmx10g")
                .include(".*" + BenchmarkPartitionedOutputOperator.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
