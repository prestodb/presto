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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slices;
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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArraySort
{
    private static final int POSITIONS = 100_000;
    private static final int ARRAY_SIZE = 100;
    private static final int NUM_TYPES = 1;
    private static final List<Type> TYPES = ImmutableList.of(VARCHAR);

    static {
        Verify.verify(NUM_TYPES == TYPES.size());
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS * ARRAY_SIZE * NUM_TYPES)
    public List<Optional<Page>> arraySort(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getPageProcessor().process(
                        SESSION.getSqlFunctionProperties(),
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPage()));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"array_sort", "old_array_sort"})
        private String name = "array_sort";

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            MetadataManager metadata = MetadataManager.createTestMetadataManager();
            metadata.registerBuiltInFunctions(extractFunctions(BenchmarkArraySort.class));
            ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
            Block[] blocks = new Block[TYPES.size()];
            for (int i = 0; i < TYPES.size(); i++) {
                Type elementType = TYPES.get(i);
                ArrayType arrayType = new ArrayType(elementType);
                FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupFunction(name, fromTypes(arrayType));
                projectionsBuilder.add(new CallExpression(name, functionHandle, arrayType, ImmutableList.of(field(i, arrayType))));
                blocks[i] = createChannel(POSITIONS, ARRAY_SIZE, arrayType);
            }

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(SESSION.getSqlFunctionProperties(), Optional.empty(), projections).get();
            page = new Page(blocks);
        }

        private static Block createChannel(int positionCount, int arraySize, ArrayType arrayType)
        {
            BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                for (int i = 0; i < arraySize; i++) {
                    if (arrayType.getElementType().getJavaType() == long.class) {
                        arrayType.getElementType().writeLong(entryBuilder, ThreadLocalRandom.current().nextLong());
                    }
                    else if (arrayType.getElementType().equals(VARCHAR)) {
                        arrayType.getElementType().writeSlice(entryBuilder, Slices.utf8Slice("test_string"));
                    }
                    else {
                        throw new UnsupportedOperationException();
                    }
                }
                blockBuilder.closeEntry();
            }
            return blockBuilder.build();
        }

        public PageProcessor getPageProcessor()
        {
            return pageProcessor;
        }

        public Page getPage()
        {
            return page;
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkArraySort().arraySort(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkArraySort.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }

    @ScalarFunction
    @SqlType("array(varchar)")
    public static Block oldArraySort(@SqlType("array(varchar)") Block block)
    {
        List<Integer> positions = Ints.asList(new int[block.getPositionCount()]);
        for (int i = 0; i < block.getPositionCount(); i++) {
            positions.set(i, i);
        }

        positions.sort((p1, p2) -> {
            //TODO: This could be quite slow, it should use parametric equals
            return VARCHAR.compareTo(block, p1, block, p2);
        });

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, block.getPositionCount());

        for (int position : positions) {
            VARCHAR.appendTo(block, position, blockBuilder);
        }

        return blockBuilder.build();
    }
}
