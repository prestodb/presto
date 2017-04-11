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

import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
import com.facebook.presto.type.MapType;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkTransformKey
{
    private static final int POSITIONS = 100_000;
    private static final int NUM_TYPES = 2;

    @Benchmark
    @OperationsPerInvocation(POSITIONS * NUM_TYPES)
    public Object benchmark(BenchmarkData data)
            throws Throwable
    {
        int position = 0;
        List<Page> pages = new ArrayList<>();
        while (position < data.getPage().getPositionCount()) {
            position = data.getPageProcessor().process(SESSION, data.getPage(), position, data.getPage().getPositionCount(), data.getPageBuilder());
            pages.add(data.getPageBuilder().build());
            data.getPageBuilder().reset();
        }
        return pages;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"DOUBLE", "BIGINT"})
        private String type = "DOUBLE";

        private String name = "transform_keys";
        private PageBuilder pageBuilder;
        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            MetadataManager metadata = MetadataManager.createTestMetadataManager();
            ExpressionCompiler compiler = new ExpressionCompiler(metadata);
            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
            Type elementType;
            Object increment;
            switch (type) {
                case "BIGINT":
                    elementType = BIGINT;
                    increment = 1L;
                    break;
                case "DOUBLE":
                    elementType = DOUBLE;
                    increment = 1.0d;
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            MapType mapType = new MapType(elementType, elementType);
            Signature signature = new Signature(
                    name,
                    FunctionKind.SCALAR,
                    mapType.getTypeSignature(),
                    mapType.getTypeSignature(),
                    parseTypeSignature(format("function(%s, %s, %s)", type, type, type)));
            Signature add = new Signature("$operator$" + ADD.name(), FunctionKind.SCALAR, elementType.getTypeSignature(), elementType.getTypeSignature(), elementType.getTypeSignature());
            projectionsBuilder.add(new CallExpression(signature, mapType, ImmutableList.of(
                    new InputReferenceExpression(0, mapType),
                    new LambdaDefinitionExpression(
                            ImmutableList.of(elementType, elementType),
                            ImmutableList.of("x", "y"),
                            new CallExpression(add, elementType, ImmutableList.of(
                                    new VariableReferenceExpression("x", elementType),
                                    new ConstantExpression(increment, elementType)))))));
            Block block = createChannel(POSITIONS, mapType, elementType);

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(new ConstantExpression(true, BOOLEAN), projections).get();
            pageBuilder = new PageBuilder(projections.stream().map(RowExpression::getType).collect(Collectors.toList()));
            page = new Page(block);
        }

        private static Block createChannel(int positionCount, MapType mapType, Type elementType)
        {
            BlockBuilder mapArrayBuilder = mapType.createBlockBuilder(new BlockBuilderStatus(), 1);
            BlockBuilder mapBuilder = new InterleavedBlockBuilder(ImmutableList.of(mapType.getKeyType(), mapType.getValueType()), new BlockBuilderStatus(), positionCount * 2);
            Object value;
            for (int position = 0; position < positionCount; position++) {
                if (elementType.equals(BIGINT)) {
                    value = ThreadLocalRandom.current().nextLong();
                }
                else if (elementType.equals(DOUBLE)) {
                    value = ThreadLocalRandom.current().nextDouble();
                }
                else {
                    throw new UnsupportedOperationException();
                }
                // Use position as the key to avoid collision
                writeNativeValue(elementType, mapBuilder, position);
                writeNativeValue(elementType, mapBuilder, value);
            }
            mapType.writeObject(mapArrayBuilder, mapBuilder.build());
            return mapArrayBuilder.build();
        }

        public PageProcessor getPageProcessor()
        {
            return pageProcessor;
        }

        public Page getPage()
        {
            return page;
        }

        public PageBuilder getPageBuilder()
        {
            return pageBuilder;
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkTransformKey().benchmark(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.BULK)
                .include(".*" + BenchmarkTransformKey.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
