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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.prestosql.RowPagesBuilder;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.SingleStreamSpillerFactory;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.TestingTaskContext;
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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 5)
@Measurement(iterations = 10, time = 2, timeUnit = SECONDS)
public class BenchmarkHashBuildAndJoinOperators
{
    private static final int HASH_BUILD_OPERATOR_ID = 1;
    private static final int HASH_JOIN_OPERATOR_ID = 2;
    private static final PlanNodeId TEST_PLAN_NODE_ID = new PlanNodeId("test");
    private static final LookupJoinOperators LOOKUP_JOIN_OPERATORS = new LookupJoinOperators();

    @State(Thread)
    public static class BuildContext
    {
        protected static final int ROWS_PER_PAGE = 1024;
        protected static final int BUILD_ROWS_NUMBER = 8_000_000;

        @Param({"varchar", "bigint", "all"})
        protected String hashColumns = "bigint";

        @Param({"false", "true"})
        protected boolean buildHashEnabled;

        @Param({"1", "5"})
        protected int buildRowsRepetition = 1;

        protected ExecutorService executor;
        protected ScheduledExecutorService scheduledExecutor;
        protected List<Page> buildPages;
        protected OptionalInt hashChannel;
        protected List<Type> types;
        protected List<Integer> hashChannels;

        @Setup
        public void setup()
        {
            switch (hashColumns) {
                case "varchar":
                    hashChannels = Ints.asList(0);
                    break;
                case "bigint":
                    hashChannels = Ints.asList(1);
                    break;
                case "all":
                    hashChannels = Ints.asList(0, 1, 2);
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unknown hashColumns value [%s]", hashColumns));
            }
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

            initializeBuildPages();
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(2, GIGABYTE));
        }

        public OptionalInt getHashChannel()
        {
            return hashChannel;
        }

        public List<Integer> getHashChannels()
        {
            return hashChannels;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public List<Page> getBuildPages()
        {
            return buildPages;
        }

        protected void initializeBuildPages()
        {
            RowPagesBuilder buildPagesBuilder = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(VARCHAR, BIGINT, BIGINT));

            int maxValue = BUILD_ROWS_NUMBER / buildRowsRepetition + 40;
            int rows = 0;
            while (rows < BUILD_ROWS_NUMBER) {
                int newRows = Math.min(BUILD_ROWS_NUMBER - rows, ROWS_PER_PAGE);
                buildPagesBuilder.addSequencePage(newRows, (rows + 20) % maxValue, (rows + 30) % maxValue, (rows + 40) % maxValue);
                buildPagesBuilder.pageBreak();
                rows += newRows;
            }

            types = buildPagesBuilder.getTypes();
            buildPages = buildPagesBuilder.build();
            hashChannel = buildPagesBuilder.getHashChannel()
                    .map(OptionalInt::of).orElse(OptionalInt.empty());
        }
    }

    @State(Thread)
    public static class JoinContext
            extends BuildContext
    {
        protected static final int PROBE_ROWS_NUMBER = 1_400_000;

        @Param({"0.1", "1", "2"})
        protected double matchRate = 1;

        @Param({"bigint", "all"})
        protected String outputColumns = "bigint";

        protected List<Page> probePages;
        protected List<Integer> outputChannels;

        protected JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory;

        @Override
        @Setup
        public void setup()
        {
            super.setup();

            switch (outputColumns) {
                case "varchar":
                    outputChannels = Ints.asList(0);
                    break;
                case "bigint":
                    outputChannels = Ints.asList(1);
                    break;
                case "all":
                    outputChannels = Ints.asList(0, 1, 2);
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unknown outputColumns value [%s]", hashColumns));
            }

            lookupSourceFactory = new BenchmarkHashBuildAndJoinOperators().benchmarkBuildHash(this, outputChannels);
            initializeProbePages();
        }

        public JoinBridgeManager<PartitionedLookupSourceFactory> getLookupSourceFactory()
        {
            return lookupSourceFactory;
        }

        public List<Page> getProbePages()
        {
            return probePages;
        }

        public List<Integer> getOutputChannels()
        {
            return outputChannels;
        }

        protected void initializeProbePages()
        {
            RowPagesBuilder probePagesBuilder = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(VARCHAR, BIGINT, BIGINT));

            Random random = new Random(42);
            int remainingRows = PROBE_ROWS_NUMBER;
            int rowsInPage = 0;
            while (remainingRows > 0) {
                double roll = random.nextDouble();

                int columnA = 20 + remainingRows;
                int columnB = 30 + remainingRows;
                int columnC = 40 + remainingRows;

                int rowsCount = 1;
                if (matchRate < 1) {
                    // each row has matchRate chance to join
                    if (roll > matchRate) {
                        // generate not matched row
                        columnA *= -1;
                        columnB *= -1;
                        columnC *= -1;
                    }
                }
                else if (matchRate > 1) {
                    // each row has will be repeated between one and 2*matchRate times
                    roll = roll * 2 * matchRate + 1;
                    // example for matchRate == 2:
                    // roll is within [0, 5) range
                    // rowsCount is within [0, 4] range, where each value has same probability
                    // so expected rowsCount is 2
                    rowsCount = (int) Math.floor(roll);
                }

                for (int i = 0; i < rowsCount; i++) {
                    if (rowsInPage >= ROWS_PER_PAGE) {
                        probePagesBuilder.pageBreak();
                        rowsInPage = 0;
                    }
                    probePagesBuilder.row(format("%d", columnA), columnB, columnC);
                    --remainingRows;
                    rowsInPage++;
                }
            }
            probePages = probePagesBuilder.build();
        }
    }

    @Benchmark
    public JoinBridgeManager<PartitionedLookupSourceFactory> benchmarkBuildHash(BuildContext buildContext)
    {
        return benchmarkBuildHash(buildContext, ImmutableList.of(0, 1, 2));
    }

    private JoinBridgeManager<PartitionedLookupSourceFactory> benchmarkBuildHash(BuildContext buildContext, List<Integer> outputChannels)
    {
        DriverContext driverContext = buildContext.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();

        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = JoinBridgeManager.lookupAllAtOnce(new PartitionedLookupSourceFactory(
                buildContext.getTypes(),
                outputChannels.stream()
                        .map(buildContext.getTypes()::get)
                        .collect(toImmutableList()),
                buildContext.getHashChannels().stream()
                        .map(buildContext.getTypes()::get)
                        .collect(toImmutableList()),
                1,
                requireNonNull(ImmutableMap.of(), "layout is null"),
                false));
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                HASH_BUILD_OPERATOR_ID,
                TEST_PLAN_NODE_ID,
                lookupSourceFactoryManager,
                outputChannels,
                buildContext.getHashChannels(),
                buildContext.getHashChannel(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                10_000,
                new PagesIndex.TestingFactory(false),
                false,
                SingleStreamSpillerFactory.unsupportedSingleStreamSpillerFactory());

        Operator operator = hashBuilderOperatorFactory.createOperator(driverContext);
        for (Page page : buildContext.getBuildPages()) {
            operator.addInput(page);
        }
        operator.finish();

        LookupSourceFactory lookupSourceFactory = lookupSourceFactoryManager.getJoinBridge(Lifespan.taskWide());
        ListenableFuture<LookupSourceProvider> lookupSourceProvider = lookupSourceFactory.createLookupSourceProvider();
        if (!lookupSourceProvider.isDone()) {
            throw new AssertionError("Expected lookup source provider to be ready");
        }
        getFutureValue(lookupSourceProvider).close();

        return lookupSourceFactoryManager;
    }

    @Benchmark
    public List<Page> benchmarkJoinHash(JoinContext joinContext)
    {
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.innerJoin(
                HASH_JOIN_OPERATOR_ID,
                TEST_PLAN_NODE_ID,
                joinContext.getLookupSourceFactory(),
                joinContext.getTypes(),
                joinContext.getHashChannels(),
                joinContext.getHashChannel(),
                Optional.of(joinContext.getOutputChannels()),
                OptionalInt.empty(),
                unsupportedPartitioningSpillerFactory());

        DriverContext driverContext = joinContext.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        Operator joinOperator = joinOperatorFactory.createOperator(driverContext);

        Iterator<Page> input = joinContext.getProbePages().iterator();
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        boolean finishing = false;
        for (int loops = 0; !joinOperator.isFinished() && loops < 1_000_000; loops++) {
            if (joinOperator.needsInput()) {
                if (input.hasNext()) {
                    Page inputPage = input.next();
                    joinOperator.addInput(inputPage);
                }
                else if (!finishing) {
                    joinOperator.finish();
                    finishing = true;
                }
            }

            Page outputPage = joinOperator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        return outputPages.build();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkHashBuildAndJoinOperators.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
