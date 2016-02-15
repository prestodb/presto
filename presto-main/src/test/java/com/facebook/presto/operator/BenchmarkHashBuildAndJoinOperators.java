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
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Threads.checkNotSameThreadExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 5)
@Measurement(iterations = 20)
public class BenchmarkHashBuildAndJoinOperators
{
    private static final int HASH_BUILD_OPERATOR_ID = 1;
    private static final int HASH_JOIN_OPERATOR_ID = 2;
    private static final PlanNodeId TEST_PLAN_NODE_ID = new PlanNodeId("test");

    @State(Thread)
    public static class BuildContext
    {
        protected static final int ROWS_PER_PAGE = 1024;
        protected static final int BUILD_ROWS_NUMBER = 700_000;

        @Param({"varchar", "bigint", "all"})
        protected String hashColumns;

        @Param({"false", "true"})
        protected boolean buildHashEnabled;

        protected ExecutorService executor;
        protected List<Page> buildPages;
        protected Optional<Integer> hashChannel;
        protected List<Type> types;
        protected List<Integer> hashChannels;
        protected LookupSourceSupplier lookupSourceSupplier;

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
            executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

            initializeBuildPages();

            lookupSourceSupplier = new BenchmarkHashBuildAndJoinOperators().benchmarkBuildHash(this);
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(
                    checkNotSameThreadExecutor(executor, "executor is null"),
                    TEST_SESSION,
                    new DataSize(2, GIGABYTE));
        }

        public Optional<Integer> getHashChannel()
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

        public LookupSourceSupplier getLookupSourceSupplier()
        {
            return lookupSourceSupplier;
        }

        public List<Page> getBuildPages()
        {
            return buildPages;
        }

        protected void initializeBuildPages()
        {
            RowPagesBuilder buildPagesBuilder = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(VARCHAR, BIGINT, BIGINT));

            int rows = 0;
            while (rows < BUILD_ROWS_NUMBER) {
                int newRows = Math.min(BUILD_ROWS_NUMBER - rows, ROWS_PER_PAGE);
                buildPagesBuilder.addSequencePage(newRows, rows + 20, rows + 30, rows + 40);
                buildPagesBuilder.pageBreak();
                rows += newRows;
            }

            types = buildPagesBuilder.getTypes();
            buildPages = buildPagesBuilder.build();
            hashChannel = buildPagesBuilder.getHashChannel();
        }
    }

    @State(Thread)
    public static class JoinContext
            extends BuildContext
    {
        protected static final int PROBE_ROWS_NUMBER = 700_000;

        @Param({"0.1", "1", "2"})
        protected double matchRate;

        protected List<Page> probePages;

        @Setup
        public void setup()
        {
            super.setup();
            initializeProbePages();
        }

        public List<Page> getProbePages()
        {
            return probePages;
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
    public LookupSourceSupplier benchmarkBuildHash(BuildContext buildContext)
    {
        DriverContext driverContext = buildContext.createTaskContext().addPipelineContext(true, true).addDriverContext();

        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                HASH_BUILD_OPERATOR_ID,
                TEST_PLAN_NODE_ID,
                buildContext.getTypes(),
                ImmutableMap.of(),
                buildContext.getHashChannels(),
                buildContext.getHashChannel(),
                false,
                Optional.empty(),
                10_000);

        Operator operator = hashBuilderOperatorFactory.createOperator(driverContext);
        for (Page page : buildContext.getBuildPages()) {
            operator.addInput(page);
        }
        operator.finish();

        if (!operator.isFinished()) {
            throw new AssertionError("Expected hash build operator to be finished");
        }

        return hashBuilderOperatorFactory.getLookupSourceSupplier();
    }

    @Benchmark
    public List<Page> benchmarkJoinHash(JoinContext joinContext)
    {
        LookupSourceSupplier lookupSourceSupplier = joinContext.getLookupSourceSupplier();

        OperatorFactory joinOperatorFactory = LookupJoinOperators.innerJoin(
                HASH_JOIN_OPERATOR_ID,
                TEST_PLAN_NODE_ID,
                lookupSourceSupplier,
                joinContext.getTypes(),
                joinContext.getHashChannels(),
                joinContext.getHashChannel(),
                false);

        DriverContext driverContext = joinContext.createTaskContext().addPipelineContext(true, true).addDriverContext();
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
