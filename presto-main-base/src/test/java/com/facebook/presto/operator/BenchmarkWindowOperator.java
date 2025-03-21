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
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
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
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.BenchmarkWindowOperator.Context.ROWS_PER_PAGE;
import static com.facebook.presto.operator.BenchmarkWindowOperator.Context.TOTAL_PAGES;
import static com.facebook.presto.operator.TestWindowOperator.ROW_NUMBER;
import static com.facebook.presto.operator.TestWindowOperator.createFactoryUnbounded;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
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
public class BenchmarkWindowOperator
{
    @State(Thread)
    public static class Context
    {
        public static final int NUMBER_OF_GROUP_COLUMNS = 2;
        public static final int TOTAL_PAGES = 140;
        public static final int ROWS_PER_PAGE = 10000;
        private static final List<Integer> PARTITION_CHANNELS = Ints.asList(0, 1);

        @Param({"10", "20", "100"})
        public int rowsPerPartition;

        @Param({"0", "1", "2", "3"})
        public int numberOfPregroupedColumns;

        @Param({"10", "50", "100"})
        public int partitionsPerGroup;

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;

        private List<Page> pages;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

            createOperatorFactoryAndGenerateTestData(numberOfPregroupedColumns);
        }

        private void createOperatorFactoryAndGenerateTestData(int numberOfPreGroupedColumns)
        {
            pages = generateTestData();

            if (numberOfPreGroupedColumns == 0) {
                // Ungrouped
                operatorFactory = createFactoryUnbounded(
                        ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT),
                        Ints.asList(0, 1, 2, 3),
                        ROW_NUMBER,
                        PARTITION_CHANNELS,
                        Ints.asList(),
                        Ints.asList(3),
                        ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                        0,
                        new DummySpillerFactory(),
                        false);
            }
            else if (numberOfPreGroupedColumns < NUMBER_OF_GROUP_COLUMNS) {
                // Partially grouped
                operatorFactory = createFactoryUnbounded(
                        ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT),
                        Ints.asList(0, 1, 2, 3),
                        ROW_NUMBER,
                        PARTITION_CHANNELS,
                        Ints.asList(1),
                        Ints.asList(3),
                        ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                        0,
                        new DummySpillerFactory(),
                        false);
            }
            else {
                // Fully grouped and (potentially) sorted
                operatorFactory = createFactoryUnbounded(
                        ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT),
                        Ints.asList(0, 1, 2, 3),
                        ROW_NUMBER,
                        PARTITION_CHANNELS,
                        Ints.asList(0, 1),
                        Ints.asList(3),
                        ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                        (numberOfPreGroupedColumns - NUMBER_OF_GROUP_COLUMNS),
                        new DummySpillerFactory(),
                        false);
            }
        }

        private List<Page> generateTestData()
        {
            List<Type> typesArray = new ArrayList<>();
            int currentPartitionIdentifier = 1;

            typesArray.add(BIGINT);
            typesArray.add(BIGINT);
            typesArray.add(BIGINT);
            typesArray.add(BIGINT);

            RowPagesBuilder pagesBuilder = buildPages(currentPartitionIdentifier, typesArray);

            return pagesBuilder.build();
        }

        private RowPagesBuilder buildPages(int currentPartitionIdentifier, List<Type> typesArray)
        {
            int groupIdentifier = 100;
            RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(false, ImmutableList.of(0), typesArray);

            for (int i = 0; i < TOTAL_PAGES; i++) {
                BlockBuilder firstColumnBlockBuilder = BIGINT.createBlockBuilder(null, ROWS_PER_PAGE);
                BlockBuilder secondColumnBlockBuilder = BIGINT.createBlockBuilder(null, ROWS_PER_PAGE);
                int currentNumberOfRowsInPartition = 0;
                int numberOfPartitionsInCurrentGroup = 0;
                int currentGroupIdentifier = groupIdentifier++;

                for (int j = 0; j < ROWS_PER_PAGE; j++) {
                    if (currentNumberOfRowsInPartition == rowsPerPartition) {
                        ++currentPartitionIdentifier;
                        ++numberOfPartitionsInCurrentGroup;
                        currentNumberOfRowsInPartition = 0;
                    }

                    if (numberOfPartitionsInCurrentGroup == partitionsPerGroup) {
                        numberOfPartitionsInCurrentGroup = 0;
                        currentGroupIdentifier = groupIdentifier++;
                    }

                    firstColumnBlockBuilder.writeLong(currentGroupIdentifier);
                    secondColumnBlockBuilder.writeLong(currentPartitionIdentifier);
                    ++currentNumberOfRowsInPartition;
                }

                rowPagesBuilder.addBlocksPage(
                        firstColumnBlockBuilder.build(),
                        secondColumnBlockBuilder.build(),
                        createLongSequenceBlock(0, ROWS_PER_PAGE),
                        createLongSequenceBlock(0, ROWS_PER_PAGE));
            }

            return rowPagesBuilder;
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
    public List<Page> benchmark(BenchmarkWindowOperator.Context context)
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
    public void verifyUnGroupedWithMultiplePartitions()
    {
        verify(10, 0, false);
    }

    @Test
    public void verifyUnGroupedWithSinglePartition()
    {
        verify(10, 0, true);
    }

    @Test
    public void verifyPartiallyGroupedWithMultiplePartitions()
    {
        verify(10, 1, false);
    }

    @Test
    public void verifyPartiallyGroupedWithSinglePartition()
    {
        verify(10, 1, true);
    }

    @Test
    public void verifyFullyGroupedWithMultiplePartitions()
    {
        verify(10, 2, false);
    }

    @Test
    public void verifyFullyGroupedWithSinglePartition()
    {
        verify(10, 2, true);
    }

    @Test
    public void verifyFullyGroupedAndFullySortedWithMultiplePartitions()
    {
        verify(10, 3, false);
    }

    @Test
    public void verifyFullyGroupedAndFullySortedWithSinglePartition()
    {
        verify(10, 3, true);
    }

    private void verify(
            int numberOfRowsPerPartition,
            int numberOfPreGroupedColumns,
            boolean useSinglePartition)
    {
        Context context = new Context();

        context.rowsPerPartition = numberOfRowsPerPartition;
        context.numberOfPregroupedColumns = numberOfPreGroupedColumns;

        if (useSinglePartition) {
            context.partitionsPerGroup = 1;
            context.rowsPerPartition = ROWS_PER_PAGE;
        }

        context.setup();

        assertEquals(TOTAL_PAGES, context.getPages().size());
        for (int i = 0; i < TOTAL_PAGES; i++) {
            assertEquals(ROWS_PER_PAGE, context.getPages().get(i).getPositionCount());
        }

        benchmark(context);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkWindowOperator.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
