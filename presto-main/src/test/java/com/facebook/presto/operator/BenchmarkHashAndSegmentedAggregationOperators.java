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

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.BenchmarkHashAndStreamingAggregationOperators.Context.ROWS_PER_PAGE;
import static com.facebook.presto.operator.BenchmarkHashAndStreamingAggregationOperators.Context.TOTAL_PAGES;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.testng.Assert.assertEquals;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 5)
@Measurement(iterations = 10, time = 2, timeUnit = SECONDS)
public class BenchmarkHashAndSegmentedAggregationOperators
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = metadata.getFunctionAndTypeManager();

    private static final InternalAggregationFunction LONG_SUM = FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(
            FUNCTION_AND_TYPE_MANAGER.lookupFunction("sum", fromTypes(BIGINT)));
    private static final InternalAggregationFunction COUNT = FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(
            FUNCTION_AND_TYPE_MANAGER.lookupFunction("count", ImmutableList.of()));

    @State(Thread)
    public static class Context
    {
        public static final int TOTAL_PAGES = 100;
        public static final int ROWS_PER_PAGE = 1000;

        @Param({"1", "10", "800", "100000"})
        public int rowsPerSegment;

        @Param({"segmented", "hash"})
        public String operatorType;

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;
        private List<Page> pages;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

            boolean segmentedAggregation = operatorType.equalsIgnoreCase("segmented");

            RowPagesBuilder pagesBuilder = RowPagesBuilder.rowPagesBuilder(true, ImmutableList.of(0, 1), VARCHAR, BIGINT, BIGINT);
            for (int i = 0; i < TOTAL_PAGES; i++) {
                BlockBuilder sortedBlockBuilder = VARCHAR.createBlockBuilder(null, ROWS_PER_PAGE);
                for (int j = 0; j < ROWS_PER_PAGE; j++) {
                    int currentSegment = (i * ROWS_PER_PAGE + j) / rowsPerSegment;
                    VARCHAR.writeString(sortedBlockBuilder, String.valueOf(currentSegment));
                }
                pagesBuilder.addBlocksPage(sortedBlockBuilder, createLongSequenceBlock(0, ROWS_PER_PAGE), createLongSequenceBlock(0, ROWS_PER_PAGE));
            }

            pages = pagesBuilder.build();
            operatorFactory = createHashAggregationOperatorFactory(pagesBuilder.getHashChannel(), segmentedAggregation);
        }

        private OperatorFactory createHashAggregationOperatorFactory(Optional<Integer> hashChannel, boolean segmentedAggregation)
        {
            JoinCompiler joinCompiler = new JoinCompiler(metadata, new FeaturesConfig());
            SpillerFactory spillerFactory = (types, localSpillContext, aggregatedMemoryContext) -> null;

            return new HashAggregationOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    ImmutableList.of(VARCHAR),
                    ImmutableList.of(0),
                    segmentedAggregation ? ImmutableList.of(0) : ImmutableList.of(),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    false,
                    ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                            LONG_SUM.bind(ImmutableList.of(2), Optional.empty())),
                    hashChannel,
                    Optional.empty(),
                    100_000,
                    Optional.of(new DataSize(16, MEGABYTE)),
                    false,
                    succinctBytes(8),
                    succinctBytes(Integer.MAX_VALUE),
                    spillerFactory,
                    joinCompiler,
                    false);
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(2, GIGABYTE));
        }

        public OperatorFactory getOperatorFactory()
        {
            return operatorFactory;
        }

        public List<Page> getPages()
        {
            return pages;
        }
    }

    @Benchmark
    public List<Page> benchmark(Context context)
    {
        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        Operator operator = context.getOperatorFactory().createOperator(driverContext);

        Iterator<Page> input = context.getPages().iterator();
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        boolean finishing = false;
        for (int loops = 0; !operator.isFinished() && loops < 1_000_000; loops++) {
            if (operator.needsInput()) {
                if (input.hasNext()) {
                    Page inputPage = input.next();
                    operator.addInput(inputPage);
                }
                else if (!finishing) {
                    operator.finish();
                    finishing = true;
                }
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        return outputPages.build();
    }

    @Test
    public void verifyHash()
    {
        verify(1, "hash");
        verify(100, "hash");
        verify(800, "hash");
        verify(100000, "hash");
    }

    @Test
    public void verifySegmented()
    {
        verify(1, "segmented");
        verify(100, "segmented");
        verify(800, "segmented");
        verify(100000, "segmented");
    }

    private void verify(int rowsPerSegment, String operatorType)
    {
        Context context = new Context();
        context.operatorType = operatorType;
        context.rowsPerSegment = rowsPerSegment;
        context.setup();

        assertEquals(TOTAL_PAGES, context.getPages().size());
        for (int i = 0; i < TOTAL_PAGES; i++) {
            assertEquals(ROWS_PER_PAGE, context.getPages().get(i).getPositionCount());
        }

        List<Page> outputPages = benchmark(context);
        assertEquals(TOTAL_PAGES * ROWS_PER_PAGE / rowsPerSegment, outputPages.stream().mapToInt(Page::getPositionCount).sum());
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkHashAndSegmentedAggregationOperators.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
