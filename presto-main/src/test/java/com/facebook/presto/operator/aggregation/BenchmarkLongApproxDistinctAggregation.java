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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.UpdateMemory;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.aggregation.GenericAccumulatorFactory.generateAccumulatorFactory;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.openjdk.jmh.annotations.Level.Invocation;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLongApproxDistinctAggregation
{
    private static final int ARRAY_SIZE = 10_000_000;

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkLongApproxDistinctAggregation().approxDistinctAggregation(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.BULK)
                .include(".*" + BenchmarkLongApproxDistinctAggregation.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }

    @Benchmark
    @OperationsPerInvocation(ARRAY_SIZE)
    public void approxDistinctAggregation(BenchmarkData data)
    {
        if (data.getName().equals("loop")) {
            data.getAccumulator().addInput(data.getPage());
        }
        else if (data.getName().equals("block")) {
            data.getAccumulator().addBlockInput(data.getPage());
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final long[] ARRAY_DATA = new long[ARRAY_SIZE];
        @Param({"loop", "block"})
        private String name = "loop";

        private String type = "BIGINT";

        private Page page;
        private Accumulator accumulator;

        public BenchmarkData()
        {
            for (int i = 0; i < ARRAY_SIZE; i++) {
                ARRAY_DATA[i] = ThreadLocalRandom.current().nextLong() % 1_000_000;
            }
        }

        private static Block createChannel(int arraySize, Type elementType)
        {
            BlockBuilder blockBuilder;
            if (elementType.getJavaType() == long.class) {
                blockBuilder = elementType.createBlockBuilder(null, arraySize);
                for (int i = 0; i < arraySize; i++) {
                    elementType.writeLong(blockBuilder, ARRAY_DATA[i]);
                }
            }
            else {
                throw new UnsupportedOperationException();
            }
            return blockBuilder.build();
        }

        public String getName()
        {
            return name;
        }

        @Setup(Invocation)
        public void setup()
        {
            FunctionAndTypeManager functionAndTypeManager = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();
            Block block;
            Type elementType;
            switch (type) {
                case "BIGINT":
                    elementType = BIGINT;
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            JavaAggregationFunctionImplementation function = functionAndTypeManager.getJavaAggregateFunctionImplementation(
                    functionAndTypeManager.lookupFunction("approx_distinct", fromTypes(elementType)));
            accumulator = generateAccumulatorFactory(function, ImmutableList.of(0), Optional.empty()).createAccumulator(UpdateMemory.NOOP);

            block = createChannel(ARRAY_SIZE, elementType);
            page = new Page(block);
        }

        public Accumulator getAccumulator()
        {
            return accumulator;
        }

        public Page getPage()
        {
            return page;
        }
    }
}
