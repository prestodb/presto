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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.unnest.UnnestOperator;
import com.facebook.presto.spi.plan.PlanNodeId;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.PageAssertions.createPageWithRandomData;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(3)
@Warmup(iterations = 8, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkUnnestOperator
{
    private static final int TOTAL_POSITIONS = 10000;

    @Benchmark
    public List<Page> unnest(BenchmarkData context)
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

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({
                "bigint",
                "varchar"
        })
        private String replicateType = "bigint";

        @Param({
                "array(varchar)",
                "array(integer)",
                "map(varchar,varchar)",
                "array(row(varchar,varchar,varchar))",
                "array(array(varchar))",
                "array(bigint)|array(bigint)",
                "array(varchar)|array(varchar)"
        })
        private String nestedType = "array(bigint)";

        @SuppressWarnings("unused")
        @Param({"0.0", "0.2"})
        private float primitiveNullsRatio;  // % of nulls in input primitive elements

        @SuppressWarnings("unused")
        @Param({"0.0", "0.05"})
        private float rowNullsRatio;    // % of nulls in row type elements

        @Param("1000")
        private int positionsPerPage = 1000;

        @SuppressWarnings("unused")
        @Param({"false", "true"})
        private boolean withOrdinality;

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;
        private List<Page> pages = new ArrayList<>();

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
            Metadata metadata = createTestMetadataManager();

            ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> replicatedTypesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> unnestTypesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Integer> replicatedChannelsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Integer> unnestChannelsBuilder = ImmutableList.builder();

            String[] replicatedTypes = replicateType.split("\\|");
            for (int i = 0; i < replicatedTypes.length; i++) {
                Type replicateType = getType(metadata, replicatedTypes[i]).get();
                typesBuilder.add(replicateType);
                replicatedTypesBuilder.add(replicateType);
                replicatedChannelsBuilder.add(i);
            }

            String[] unnestTypes = nestedType.split("\\|");
            for (int i = 0; i < unnestTypes.length; i++) {
                Type unnestType = getType(metadata, unnestTypes[i]).get();
                typesBuilder.add(unnestType);
                unnestTypesBuilder.add(unnestType);
                unnestChannelsBuilder.add(i + replicatedTypes.length);
            }

            int pageCount = TOTAL_POSITIONS / positionsPerPage;

            for (int i = 0; i < pageCount; i++) {
                pages.add(createPageWithRandomData(
                        typesBuilder.build(),
                        positionsPerPage,
                        false,
                        false,
                        primitiveNullsRatio,
                        rowNullsRatio,
                        false,
                        ImmutableList.of()));
            }

            operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    replicatedChannelsBuilder.build(),
                    replicatedTypesBuilder.build(),
                    unnestChannelsBuilder.build(),
                    unnestTypesBuilder.build(),
                    withOrdinality);
        }

        public Optional<Type> getType(Metadata metadata, String typeString)
        {
            if (typeString.equals("NONE")) {
                return Optional.empty();
            }
            TypeSignature signature = TypeSignature.parseTypeSignature(typeString);
            return Optional.of(metadata.getType(signature));
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
    }

    public static void main(String[] args)
            throws RunnerException
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkUnnestOperator().unnest(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkUnnestOperator.class.getSimpleName() + ".*")
                .addProfiler(GCProfiler.class)
                .build();

        new Runner(options).run();
    }
}
