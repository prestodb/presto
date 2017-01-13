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
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
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
        private PageBuilder pageBuilder;
        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            Signature signature = new Signature("array_join", FunctionKind.SCALAR, VARCHAR.getTypeSignature(), new ArrayType(BIGINT).getTypeSignature(), VARCHAR.getTypeSignature());

            List<RowExpression> projections = ImmutableList.of(
                    new CallExpression(signature, VARCHAR, ImmutableList.of(
                            new InputReferenceExpression(0, new ArrayType(BIGINT)),
                            new ConstantExpression(Slices.wrappedBuffer(",".getBytes(UTF_8)), VARCHAR))));

            pageProcessor = new ExpressionCompiler(MetadataManager.createTestMetadataManager())
                    .compilePageProcessor(new ConstantExpression(true, BooleanType.BOOLEAN), projections)
                    .get();

            pageBuilder = new PageBuilder(ImmutableList.of(VARCHAR));

            page = new Page(createChannel(POSITIONS, ARRAY_SIZE));
        }

        private static Block createChannel(int positionCount, int arraySize)
        {
            ArrayType arrayType = new ArrayType(BIGINT);

            BlockBuilder blockBuilder = arrayType.createBlockBuilder(new BlockBuilderStatus(), positionCount);
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

        public PageBuilder getPageBuilder()
        {
            return pageBuilder;
        }
    }

    @Test
    public void verify()
            throws Throwable
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
