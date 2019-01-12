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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.RowExpression;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayJoin
{
    private static final int POSITIONS = 100_000;
    private static final int ARRAY_SIZE = 10;

    @Benchmark
    @OperationsPerInvocation(POSITIONS * ARRAY_SIZE)
    public List<Optional<Page>> benchmark(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getPageProcessor().process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPage()));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            Signature signature = new Signature("array_join", FunctionKind.SCALAR, VARCHAR.getTypeSignature(), new ArrayType(BIGINT).getTypeSignature(), VARCHAR.getTypeSignature());

            List<RowExpression> projections = ImmutableList.of(
                    new CallExpression(signature, VARCHAR, ImmutableList.of(
                            field(0, new ArrayType(BIGINT)),
                            constant(Slices.wrappedBuffer(",".getBytes(UTF_8)), VARCHAR))));

            MetadataManager metadata = MetadataManager.createTestMetadataManager();
            pageProcessor = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0))
                    .compilePageProcessor(Optional.empty(), projections)
                    .get();

            page = new Page(createChannel(POSITIONS, ARRAY_SIZE));
        }

        private static Block createChannel(int positionCount, int arraySize)
        {
            ArrayType arrayType = new ArrayType(BIGINT);

            BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                for (int i = 0; i < arraySize; i++) {
                    arrayType.getElementType().writeLong(entryBuilder, ThreadLocalRandom.current().nextLong());
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

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkArrayJoin().benchmark(data);
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkArrayJoin().benchmark(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkArrayJoin.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
