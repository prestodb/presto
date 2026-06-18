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

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArraysOverlap
{
    private static final int POSITIONS = 10_000;
    private static final int SMALL_ARRAY_SIZE = 10;
    private static final int LARGE_ARRAY_SIZE = 1500;

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkArraysOverlap().smallBenchmark(data);
        new BenchmarkArraysOverlap().largeBenchmark(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkArraysOverlap.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS * SMALL_ARRAY_SIZE)
    public List<Optional<Page>> smallBenchmark(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getPageProcessor().process(
                        SESSION.getSqlFunctionProperties(),
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getSmallPage()));
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS * LARGE_ARRAY_SIZE)
    public List<Optional<Page>> largeBenchmark(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getPageProcessor().process(
                        SESSION.getSqlFunctionProperties(),
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getLargePage()));
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkArraysOverlap().smallBenchmark(data);
        new BenchmarkArraysOverlap().largeBenchmark(data);
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final Map<String, Type> TYPE_MAP
                = ImmutableMap.of("BIGINT", BIGINT, "BOOLEAN", BOOLEAN,
                "VARCHAR", VARCHAR, "DOUBLE", DOUBLE);
        private String name = "arrays_overlap";
        private Page smallPage;
        private Page largePage;
        private PageProcessor pageProcessor;

        @Param({"BIGINT", "DOUBLE", "VARCHAR", "BOOLEAN"})
        private String elementType = "BIGINT";

        @Param({"ELEMENT", "ARRAY"})
        private String typeClass = "ELEMENT";

        private static Block createChannel(int positionCount, int arraySize, ArrayType arrayType, TypeClass typeClass)
        {
            BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, positionCount);
            Type basicElementType = (typeClass.equals(TypeClass.ELEMENT)) ? arrayType.getElementType() : ((ArrayType) arrayType.getElementType()).getElementType();
            for (int position = 0; position < positionCount; position++) {
                BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                for (int i = 0; i < arraySize; i++) {
                    if (basicElementType.getJavaType() == long.class) {
                        if (typeClass.equals(TypeClass.ELEMENT)) {
                            basicElementType.writeLong(entryBuilder, ThreadLocalRandom.current().nextLong());
                        }
                        else {
                            arrayType.getElementType().writeObject(entryBuilder,
                                    BlockAssertions.createLongSequenceBlock(0, (ThreadLocalRandom.current().nextInt()) % 10));
                        }
                    }
                    else if (basicElementType.equals(VARCHAR)) {
                        if (typeClass.equals(TypeClass.ELEMENT)) {
                            basicElementType.writeSlice(entryBuilder,
                                    Slices.utf8Slice("test_string " + (ThreadLocalRandom.current().nextInt() % 5)));
                        }
                        else {
                            List<Slice> slices = IntStream.range(0, (ThreadLocalRandom.current().nextInt()) % 10)
                                    .mapToObj(val -> Slices.utf8Slice("test_string " + (ThreadLocalRandom.current().nextInt() % 5)))
                                    .collect(Collectors.toList());
                            arrayType.getElementType().writeObject(entryBuilder, BlockAssertions.createSlicesBlock(slices));
                        }
                    }
                    else if (basicElementType.equals(BOOLEAN)) {
                        if (typeClass.equals(TypeClass.ELEMENT)) {
                            basicElementType.writeBoolean(entryBuilder, ThreadLocalRandom.current().nextBoolean());
                        }
                        else {
                            arrayType.getElementType().writeObject(entryBuilder,
                                    BlockAssertions.createBooleanSequenceBlock(0, (ThreadLocalRandom.current().nextInt()) % 10));
                        }
                    }
                    else if (basicElementType.equals(DOUBLE)) {
                        if (typeClass.equals(TypeClass.ELEMENT)) {
                            basicElementType.writeDouble(entryBuilder, ThreadLocalRandom.current().nextDouble());
                        }
                        else {
                            arrayType.getElementType().writeObject(entryBuilder,
                                    BlockAssertions.createDoubleSequenceBlock(0, (ThreadLocalRandom.current().nextInt()) % 10));
                        }
                    }
                    else {
                        throw new UnsupportedOperationException();
                    }
                }
                blockBuilder.closeEntry();
            }
            return blockBuilder.build();
        }

        @Setup
        public void setup()
        {
            MetadataManager metadata = MetadataManager.createTestMetadataManager();
            FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
            ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();

            ArrayType arrayType = new ArrayType(TYPE_MAP.get(elementType));
            TypeClass typeClassVal = TypeClass.valueOf(typeClass);
            if (typeClassVal.equals(TypeClass.ARRAY)) {
                arrayType = new ArrayType(arrayType);
            }
            FunctionHandle functionHandle = functionAndTypeManager.lookupFunction(name, fromTypes(arrayType, arrayType));

            projectionsBuilder.add(
                    new CallExpression(name, functionHandle, BooleanType.BOOLEAN, ImmutableList.of(
                            field(0, arrayType),
                            field(1, arrayType))));

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(SESSION.getSqlFunctionProperties(), Optional.empty(), projections).get();

            largePage = new Page(createChannel(POSITIONS, LARGE_ARRAY_SIZE, arrayType, typeClassVal),
                    createChannel(POSITIONS, LARGE_ARRAY_SIZE, arrayType, typeClassVal));
            smallPage = new Page(createChannel(POSITIONS, SMALL_ARRAY_SIZE, arrayType, typeClassVal),
                    createChannel(POSITIONS, SMALL_ARRAY_SIZE, arrayType, typeClassVal));
        }

        public PageProcessor getPageProcessor()
        {
            return pageProcessor;
        }

        public Page getSmallPage()
        {
            return smallPage;
        }

        public Page getLargePage()
        {
            return largePage;
        }

        enum TypeClass
        {ELEMENT, ARRAY}
    }
}
