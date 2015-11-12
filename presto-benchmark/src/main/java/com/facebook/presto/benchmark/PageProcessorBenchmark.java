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
package com.facebook.presto.benchmark;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.NotNull;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static java.util.stream.Collectors.toList;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
public class PageProcessorBenchmark
{
    private static final Metadata METADATA = createTestMetadataManager();
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();

    private PageProcessor processor;
    private List<Page> inputPages;

    private List<? extends Type> types;

    @Param({ "10", "100", "1000", "2000" })
    int pageCount;

    @Param({ "1000", "2000", "4000" })
    int positions;

    @Setup
    public void setup()
    {
        List<RowExpression> projections = getProjections();
        types = projections.stream().map(RowExpression::getType).collect(toList());

        inputPages = createPages(pageCount);
        processor = new ExpressionCompiler(createTestMetadataManager()).compilePageProcessor(getFilter(), projections);
    }

    @Benchmark
    public void rowOriented(Blackhole b)
    {
        for (int i = 0; i < pageCount; i++) {
            PageBuilder pageBuilder = createPageBuilder();
            b.consume(processor.process(null, inputPages.get(i), 0, inputPages.get(i).getPositionCount(), pageBuilder));
        }
    }

    @Benchmark
    public void columnOriented(Blackhole b)
    {
        for (int i = 0; i < pageCount; i++) {
            PageBuilder pageBuilder = createPageBuilder();
            b.consume(processor.processColumnar(null, inputPages.get(i), types));
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + PageProcessorBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    private static RowExpression getFilter()
    {
        return new ConstantExpression(true, BooleanType.BOOLEAN);
    }

    private static List<RowExpression> getProjections()
    {
        return ImmutableList.of(
                call(
                        new Signature(FunctionRegistry.mangleOperatorName("ADD"), SCALAR, StandardTypes.BIGINT, StandardTypes.BIGINT, StandardTypes.BIGINT),
                        BIGINT,
                        field(0, BIGINT),
                        constant(5L, BIGINT)),
                call(
                        new Signature(FunctionRegistry.mangleOperatorName("ADD"), SCALAR, StandardTypes.BIGINT, StandardTypes.BIGINT, StandardTypes.BIGINT),
                        BIGINT,
                        field(1, BIGINT),
                        constant(10L, BIGINT)),
                call(
                        new Signature(FunctionRegistry.mangleOperatorName("ADD"), SCALAR, StandardTypes.BIGINT, StandardTypes.BIGINT, StandardTypes.BIGINT),
                        BIGINT,
                        field(2, BIGINT),
                        constant(11L, BIGINT)));
    }

    @NotNull
    private PageBuilder createPageBuilder()
    {
        return new PageBuilder(types);
    }

    private static Block buildSequenceBlock(long start, int count)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            BIGINT.writeLong(builder, start + i);
        }
        return builder.build();
    }

    @NotNull
    private List<Page> createPages(int count)
    {
        ImmutableList.Builder<Page> builder = ImmutableList.<Page>builder();

        for (int i = 0; i < count; i++) {
            builder.add(new Page(buildSequenceBlock(i, positions), buildSequenceBlock(i, positions), buildSequenceBlock(i, positions)));
        }
        return builder.build();
    }
}
