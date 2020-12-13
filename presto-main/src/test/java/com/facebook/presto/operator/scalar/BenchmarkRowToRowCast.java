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
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.block.RowBlock.fromFieldBlocks;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(NANOSECONDS)
@Fork(3)
@BenchmarkMode(AverageTime)
public class BenchmarkRowToRowCast
{
    private static final int POSITION_COUNT = 100_000;

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmark(BenchmarkData data)
    {
        return ImmutableList.copyOf(data.getPageProcessor().process(
                SESSION.getSqlFunctionProperties(),
                new DriverYieldSignal(),
                newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                data.getPage()));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final List<Type> fromFieldTypes = ImmutableList.of(createVarcharType(20), BIGINT);
        private static final List<Type> toFieldTypes = ImmutableList.of(createVarcharType(30), BIGINT);

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            MetadataManager metadata = createTestMetadataManager();
            FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupCast(CAST, RowType.anonymous(fromFieldTypes).getTypeSignature(), RowType.anonymous(toFieldTypes).getTypeSignature());

            List<RowExpression> projections = ImmutableList.of(
                    new CallExpression(CAST.name(), functionHandle, RowType.anonymous(fromFieldTypes), ImmutableList.of(field(0, RowType.anonymous(toFieldTypes)))));

            pageProcessor = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0))
                    .compilePageProcessor(SESSION.getSqlFunctionProperties(), Optional.empty(), projections)
                    .get();

            Block[] fieldBlocks = fromFieldTypes.stream()
                    .map(type -> createBlock(POSITION_COUNT, type))
                    .toArray(Block[]::new);
            Block rowBlock = fromFieldBlocks(POSITION_COUNT, Optional.empty(), fieldBlocks);

            page = new Page(rowBlock);
        }

        private static Block createBlock(int positionCount, Type type)
        {
            BlockBuilder blockBuilder = type.createBlockBuilder(null, positionCount);
            if (type instanceof VarcharType) {
                for (int i = 0; i < positionCount; i++) {
                    type.writeSlice(blockBuilder, utf8Slice(String.valueOf(ThreadLocalRandom.current().nextInt())));
                }
            }
            else if (type == BIGINT) {
                for (int i = 0; i < positionCount; i++) {
                    type.writeLong(blockBuilder, ThreadLocalRandom.current().nextLong());
                }
            }
            else {
                throw new UnsupportedOperationException();
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

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkRowToRowCast().benchmark(data);
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkRowToRowCast().benchmark(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkRowToRowCast.class.getSimpleName() + ".*")
                .warmupMode(WarmupMode.INDI)
                .build();
        new Runner(options).run();
    }
}
