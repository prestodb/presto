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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.google.common.collect.ImmutableList;
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

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;

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
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(
                metadata,
                new PageFunctionCompiler(metadata, 0));
        RowExpression projection = generateComplexComparisonProjection(functionAndTypeManager, FIELDS_COUNT, COMPARISONS_COUNT);
        compiledProcessor = expressionCompiler.compilePageProcessor(SESSION.getSqlFunctionProperties(), Optional.empty(), ImmutableList.of(projection)).get();
    }

    private static RowExpression generateComplexComparisonProjection(FunctionAndTypeManager functionAndTypeManager, int fieldsCount, int comparisonsCount)
    {
        checkArgument(fieldsCount > 0, "fieldsCount must be greater than zero");
        checkArgument(comparisonsCount > 0, "comparisonsCount must be greater than zero");

        if (comparisonsCount == 1) {
            return createComparison(functionAndTypeManager, 0, 0);
        }

        return createConjunction(
                createComparison(functionAndTypeManager, 0, comparisonsCount % fieldsCount),
                generateComplexComparisonProjection(functionAndTypeManager, fieldsCount, comparisonsCount - 1));
    }

    private static RowExpression createConjunction(RowExpression left, RowExpression right)
    {
        return specialForm(OR, BOOLEAN, left, right);
    }

    private static RowExpression createComparison(FunctionAndTypeManager functionAndTypeManager, int leftField, int rightField)
    {
        return call(
                EQUAL.name(),
                functionAndTypeManager.resolveOperator(EQUAL, fromTypes(BIGINT, BIGINT)),
                BOOLEAN,
                field(leftField, BIGINT),
                field(rightField, BIGINT));
    }

    @Benchmark
    public List<Page> processPage(BenchmarkData data)
    {
        List<Page> output = new ArrayList<>();
        Iterator<Optional<Page>> pageProcessorOutput = compiledProcessor.process(
                SESSION.getSqlFunctionProperties(),
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
