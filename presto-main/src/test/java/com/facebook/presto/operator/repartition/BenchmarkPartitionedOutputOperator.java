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
package com.facebook.presto.operator.repartition;

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.PartitionedOutputBuffer;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.operator.BucketPartitionFunction;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.PageAssertions;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.PrecomputedHashGenerator;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.operator.repartition.OptimizedPartitionedOutputOperator.OptimizedPartitionedOutputFactory;
import com.facebook.presto.operator.repartition.PartitionedOutputOperator.PartitionedOutputFactory;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.OutputPartitioning;
import com.facebook.presto.testing.TestingTaskContext;
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
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.block.BlockAssertions.createMapType;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RowType.withDefaultFieldNames;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.operator.PageAssertions.createDictionaryPageWithRandomData;
import static com.facebook.presto.operator.PageAssertions.createRlePageWithRandomData;
import static com.facebook.presto.operator.PageAssertions.updateBlockTypesWithHashBlockAndNullBlock;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.HASH;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
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

    @Test
    public void verifyAddPage()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkPartitionedOutputOperator().addPage(data);
    }

    @Test
    public void verifyOptimizedAddPage()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkPartitionedOutputOperator().optimizedAddPage(data);
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

        @SuppressWarnings("unused")
        @Param({"true", "false"})
        private boolean enableCompression;

        @Param({"1", "2"})
        private int channelCount = 1;

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
                "ROW(ARRAY(BIGINT),ARRAY(BIGINT))"
        })
        private String type = "BIGINT";

        @SuppressWarnings("unused")
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
            float primitiveNullRate = 0.0f;
            float nestedNullRate = 0.0f;

            if (hasNull) {
                primitiveNullRate = 0.2f;
                nestedNullRate = 0.2f;
            }
            switch (inputType) {
                case "BIGINT":
                    types = nCopies(channelCount, BIGINT);
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 5000;
                    break;
                case "DICTIONARY(BIGINT)":
                    types = nCopies(channelCount, BIGINT);
                    dataPage = createDictionaryPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 3000;
                    break;
                case "RLE(BIGINT)":
                    types = nCopies(channelCount, BIGINT);
                    dataPage = createRlePageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 3000;
                    break;
                case "LONG_DECIMAL":
                    types = nCopies(channelCount, createDecimalType(MAX_SHORT_PRECISION + 1));
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 5000;
                    break;
                case "INTEGER":
                    types = nCopies(channelCount, INTEGER);
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 5000;
                    break;
                case "SMALLINT":
                    types = nCopies(channelCount, SMALLINT);
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 5000;
                    break;
                case "BOOLEAN":
                    types = nCopies(channelCount, BOOLEAN);
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 5000;
                    break;
                case "VARCHAR":
                    types = nCopies(channelCount, VARCHAR);
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 5000;
                    break;
                case "ARRAY(BIGINT)":
                    types = nCopies(channelCount, new ArrayType(BIGINT));
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;
                case "ARRAY(VARCHAR)":
                    types = nCopies(channelCount, new ArrayType(VARCHAR));
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;
                case "ARRAY(ARRAY(BIGINT))":
                    types = nCopies(channelCount, new ArrayType(new ArrayType(BIGINT)));
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;
                case "MAP(BIGINT,BIGINT)":
                    types = nCopies(channelCount, createMapType(BIGINT, BIGINT));
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;
                case "MAP(BIGINT,MAP(BIGINT,BIGINT))":
                    types = nCopies(channelCount, createMapType(BIGINT, createMapType(BIGINT, BIGINT)));
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;
                case "ROW(BIGINT,BIGINT)":
                    types = nCopies(channelCount, withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT)));
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;
                case "ROW(ARRAY(BIGINT),ARRAY(BIGINT))":
                    types = nCopies(channelCount, withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT))));
                    dataPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, primitiveNullRate, nestedNullRate);
                    pageCount = 1000;
                    break;

                default:
                    throw new UnsupportedOperationException("Unsupported dataType");
            }
            // We built the dataPage with added pre-computed hash block at channel 0, so types needs to be udpated
            types = updateBlockTypesWithHashBlockAndNullBlock(types, true, false);
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
            PartitionFunction partitionFunction = new BucketPartitionFunction(
                    HASH.createBucketFunction(ImmutableList.of(BIGINT), true, PARTITION_COUNT),
                    IntStream.range(0, PARTITION_COUNT).toArray());
            OutputPartitioning outputPartitioning = createOutputPartitioning(partitionFunction);

            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new BlockEncodingManager(), enableCompression);
            PartitionedOutputBuffer buffer = createPartitionedOutputBuffer();

            OptimizedPartitionedOutputFactory operatorFactory = new OptimizedPartitionedOutputFactory(buffer, MAX_PARTITION_BUFFER_SIZE);

            return (OptimizedPartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), types, Function.identity(), Optional.of(outputPartitioning), serdeFactory)
                    .createOperator(createDriverContext());
        }

        private PartitionedOutputOperator createPartitionedOutputOperator()
        {
            PartitionFunction partitionFunction = new LocalPartitionGenerator(new PrecomputedHashGenerator(0), PARTITION_COUNT);
            OutputPartitioning outputPartitioning = createOutputPartitioning(partitionFunction);

            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new BlockEncodingManager(), enableCompression);
            PartitionedOutputBuffer buffer = createPartitionedOutputBuffer();

            PartitionedOutputFactory operatorFactory = new PartitionedOutputFactory(buffer, MAX_PARTITION_BUFFER_SIZE);

            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), types, Function.identity(), Optional.of(outputPartitioning), serdeFactory)
                    .createOperator(createDriverContext());
        }

        private OutputPartitioning createOutputPartitioning(PartitionFunction partitionFunction)
        {
            return new OutputPartitioning(
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty(), Optional.empty()),
                    false,
                    OptionalInt.empty());
        }

        private DriverContext createDriverContext()
        {
            Session testSession = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema(TINY_SCHEMA_NAME)
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
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .jvmArgs("-Xmx10g")
                .include(".*" + BenchmarkPartitionedOutputOperator.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
