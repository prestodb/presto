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
package io.prestosql.sql.gen;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.RowExpression;
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

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.sql.relational.Signatures.IN;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(10)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(AverageTime)
@SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal"})
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

        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        processor = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0)).compilePageProcessor(Optional.of(filter), ImmutableList.of(project)).get();
    }

    @Benchmark
    public List<Optional<Page>> benchmark()
    {
        return ImmutableList.copyOf(
                processor.process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        inputPage));
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
