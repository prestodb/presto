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
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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
import java.util.stream.Collectors;

import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayTransform
{
    private static final int POSITIONS = 100_000;
    private static final int ARRAY_SIZE = 4;
    private static final int NUM_TYPES = 1;
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);

    static {
        Verify.verify(NUM_TYPES == TYPES.size());
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS * ARRAY_SIZE * NUM_TYPES)
    public Object benchmark(BenchmarkData data)
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
        private PageBuilder pageBuilder;
        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            MetadataManager metadata = MetadataManager.createTestMetadataManager();
            FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
            ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
            Block[] blocks = new Block[TYPES.size()];
            Type returnType = new ArrayType(BOOLEAN);
            for (int i = 0; i < TYPES.size(); i++) {
                Type elementType = TYPES.get(i);
                ArrayType arrayType = new ArrayType(elementType);
                FunctionHandle functionHandle = functionAndTypeManager.lookupFunction("transform", fromTypeSignatures(arrayType.getTypeSignature(), parseTypeSignature("function(bigint,boolean)")));
                FunctionHandle greaterThan = functionAndTypeManager.resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT));
                projectionsBuilder.add(new CallExpression("transform", functionHandle, returnType, ImmutableList.of(
                        new InputReferenceExpression(0, arrayType),
                        new LambdaDefinitionExpression(
                                ImmutableList.of(BIGINT),
                                ImmutableList.of("x"),
                                new CallExpression(GREATER_THAN.name(), greaterThan, BOOLEAN, ImmutableList.of(new VariableReferenceExpression("x", BIGINT), new ConstantExpression(0L, BIGINT)))))));
                blocks[i] = createChannel(POSITIONS, ARRAY_SIZE, arrayType);
            }

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(SESSION.getSqlFunctionProperties(), Optional.empty(), projections).get();
            pageBuilder = new PageBuilder(projections.stream().map(RowExpression::getType).collect(Collectors.toList()));
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
        new BenchmarkArrayTransform().benchmark(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkArrayTransform.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
