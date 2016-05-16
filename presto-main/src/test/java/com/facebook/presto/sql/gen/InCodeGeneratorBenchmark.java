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
package com.facebook.presto.sql.gen;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.sql.relational.Signatures.IN;
import static com.google.common.base.Preconditions.checkState;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(10)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(AverageTime)
@SuppressWarnings("FieldMayBeFinal")
public class InCodeGeneratorBenchmark
{
    @Param({"1", "5", "10", "25", "50", "75", "100", "150", "200", "250", "300", "350", "400", "450", "500", "750", "1000", "10000"})
    private int inListCount = 1;

    @Param({StandardTypes.BIGINT, StandardTypes.DOUBLE, StandardTypes.VARCHAR})
    private String type = StandardTypes.BIGINT;

    private Page inputPage;
    private PageProcessor processor;
    private Type prestoType;

    @Setup
    public void setup()
    {
        Random random = new Random();
        RowExpression[] arguments = new RowExpression[1 + inListCount];
        switch (type) {
            case StandardTypes.BIGINT:
                prestoType = BIGINT;
                for (int i = 1; i <= inListCount; i++) {
                    arguments[i] = constant((long) random.nextInt(), BIGINT);
                }
                break;
            case StandardTypes.DOUBLE:
                prestoType = DOUBLE;
                for (int i = 1; i <= inListCount; i++) {
                    arguments[i] = constant(random.nextDouble(), DOUBLE);
                }
                break;
            case StandardTypes.VARCHAR:
                prestoType = VARCHAR;
                for (int i = 1; i <= inListCount; i++) {
                    arguments[i] = constant(Slices.utf8Slice(Long.toString(random.nextLong())), VARCHAR);
                }
                break;
            default:
                throw new IllegalStateException();
        }

        arguments[0] = field(0, prestoType);
        RowExpression project = field(0, prestoType);

        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(prestoType));
        for (int i = 0; i < 10_000; i++) {
            pageBuilder.declarePosition();

            switch (type) {
                case StandardTypes.BIGINT:
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(0), random.nextInt());
                    break;
                case StandardTypes.DOUBLE:
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(0), random.nextDouble());
                    break;
                case StandardTypes.VARCHAR:
                    VARCHAR.writeSlice(pageBuilder.getBlockBuilder(0), Slices.utf8Slice(Long.toString(random.nextLong())));
                    break;
            }
        }
        inputPage = pageBuilder.build();

        RowExpression filter = call(
                new Signature(IN, SCALAR, parseTypeSignature(StandardTypes.BOOLEAN)),
                BOOLEAN,
                arguments);

        processor = new ExpressionCompiler(MetadataManager.createTestMetadataManager()).compilePageProcessor(filter, ImmutableList.of(project)).get();
    }

    @Benchmark
    public Page benchmark()
    {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(prestoType));
        int count = processor.process(null, inputPage, 0, inputPage.getPositionCount(), pageBuilder);
        checkState(count == inputPage.getPositionCount());
        return pageBuilder.build();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + InCodeGeneratorBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
