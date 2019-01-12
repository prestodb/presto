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
import io.prestosql.metadata.MetadataManager;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.relational.RowExpression;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.Signature.internalOperator;
import static io.prestosql.metadata.Signature.internalScalarFunction;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.field;

@State(Scope.Thread)
@Fork(3)
@Warmup(iterations = 5, time = 10)
@Measurement(iterations = 5, time = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BenchmarkEqualsOperator
{
    private static final int FIELDS_COUNT = 10;
    private static final int COMPARISONS_COUNT = 100;
    private static final double NULLS_FRACTION = 0.05;

    private static final DriverYieldSignal SIGNAL = new DriverYieldSignal();
    private static final ConnectorSession SESSION = TEST_SESSION.toConnectorSession();

    private PageProcessor compiledProcessor;

    @Setup
    public void setup()
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(
                metadata,
                new PageFunctionCompiler(metadata, 0));
        RowExpression projection = generateComplexComparisonProjection(FIELDS_COUNT, COMPARISONS_COUNT);
        compiledProcessor = expressionCompiler.compilePageProcessor(Optional.empty(), ImmutableList.of(projection)).get();
    }

    private static RowExpression generateComplexComparisonProjection(int fieldsCount, int comparisonsCount)
    {
        checkArgument(fieldsCount > 0, "fieldsCount must be greater than zero");
        checkArgument(comparisonsCount > 0, "comparisonsCount must be greater than zero");

        if (comparisonsCount == 1) {
            return createComparison(0, 0);
        }

        return createConjunction(
                createComparison(0, comparisonsCount % fieldsCount),
                generateComplexComparisonProjection(fieldsCount, comparisonsCount - 1));
    }

    private static RowExpression createConjunction(RowExpression left, RowExpression right)
    {
        return call(
                internalScalarFunction("OR", BOOLEAN.getTypeSignature(), BOOLEAN.getTypeSignature(), BOOLEAN.getTypeSignature()),
                BOOLEAN,
                left,
                right);
    }

    private static RowExpression createComparison(int leftField, int rightField)
    {
        return call(
                internalOperator(OperatorType.EQUAL, BOOLEAN.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature()),
                BOOLEAN,
                field(leftField, BIGINT),
                field(rightField, BIGINT));
    }

    @Benchmark
    public List<Page> processPage(BenchmarkData data)
    {
        List<Page> output = new ArrayList<>();
        Iterator<Optional<Page>> pageProcessorOutput = compiledProcessor.process(
                SESSION,
                SIGNAL,
                newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                data.page);
        while (pageProcessorOutput.hasNext()) {
            pageProcessorOutput.next().ifPresent(output::add);
        }
        return output;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        Page page;

        @Setup
        public void setup()
        {
            List<Type> types = ImmutableList.copyOf(limit(cycle(BIGINT), FIELDS_COUNT));
            ThreadLocalRandom random = ThreadLocalRandom.current();
            PageBuilder pageBuilder = new PageBuilder(types);
            while (!pageBuilder.isFull()) {
                pageBuilder.declarePosition();
                for (int channel = 0; channel < FIELDS_COUNT; channel++) {
                    BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                    if (random.nextDouble() < NULLS_FRACTION) {
                        blockBuilder.appendNull();
                    }
                    else {
                        BIGINT.writeLong(blockBuilder, random.nextLong());
                    }
                }
            }
            page = pageBuilder.build();
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkEqualsOperator.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
