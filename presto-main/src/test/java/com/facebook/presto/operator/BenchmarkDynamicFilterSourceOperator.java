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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import io.airlift.units.DataSize;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.getDynamicFilteringMaxPerDriverRowCount;
import static com.facebook.presto.SystemSessionProperties.getDynamicFilteringMaxPerDriverSize;
import static com.facebook.presto.SystemSessionProperties.getDynamicFilteringRangeRowLimitPerDriver;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 20, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDynamicFilterSourceOperator
{
    private static final int TOTAL_POSITIONS = 1_000_000;

    @State(Scope.Thread)
    public static class BenchmarkContext
    {
        @Param({"32", "256", "1024"})
        private String positionsPerPage = "32";

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;
        private List<Page> pages;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

            pages = createInputPages(Integer.valueOf(positionsPerPage));

            operatorFactory = new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                    1,
                    new PlanNodeId("joinNodeId"),
                    (tupleDomain -> {}),
                    ImmutableList.of(new DynamicFilterSourceOperator.Channel("0", BIGINT, 0)),
                    getDynamicFilteringMaxPerDriverRowCount(TEST_SESSION),
                    getDynamicFilteringMaxPerDriverSize(TEST_SESSION),
                    getDynamicFilteringRangeRowLimitPerDriver(TEST_SESSION));
        }

        @TearDown
        public void cleanup()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
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

        private static List<Page> createInputPages(int positionsPerPage)
        {
            ImmutableList.Builder<Page> pages = ImmutableList.builder();
            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT));
            LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
            Iterator<LineItem> iterator = lineItemGenerator.iterator();
            for (int i = 0; i < TOTAL_POSITIONS; i++) {
                pageBuilder.declarePosition();

                LineItem lineItem = iterator.next();
                BIGINT.writeLong(pageBuilder.getBlockBuilder(0), lineItem.getOrderKey());

                if (pageBuilder.getPositionCount() == positionsPerPage) {
                    pages.add(pageBuilder.build());
                    pageBuilder.reset();
                }
            }

            if (pageBuilder.getPositionCount() > 0) {
                pages.add(pageBuilder.build());
            }
            return pages.build();
        }
    }

    @Benchmark
    public List<Page> dynamicFilterCollect(BenchmarkContext context)
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
    public void testBenchmark()
    {
        BenchmarkContext context = new BenchmarkContext();
        context.setup();

        List<Page> outputPages = dynamicFilterCollect(context);
        assertEquals(TOTAL_POSITIONS, outputPages.stream().mapToInt(Page::getPositionCount).sum());

        context.cleanup();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkDynamicFilterSourceOperator.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
