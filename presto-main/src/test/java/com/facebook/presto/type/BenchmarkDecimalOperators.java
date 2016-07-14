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
package com.facebook.presto.type;

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Benchmark;
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

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.scalar.FunctionAssertions.createExpression;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.stream.Collectors.toMap;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDecimalOperators
{
    private static final int PAGE_SIZE = 30000;

    private static final DecimalType SHORT_DECIMAL_TYPE = DecimalType.createDecimalType(10, 0);
    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(20, 0);

    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = createTestMetadataManager();

    @State(Thread)
    public static class InequalityBenchmarkState
            extends BaseState
    {
        @Param({"d1 < d2",
                "d1 < d2 AND d1 < d3 AND d1 < d4 AND d2 < d3 AND d2 < d4 AND d3 < d4",
                "s1 < s2",
                "s1 < s2 AND s1 < s3 AND s1 < s4 AND s2 < s3 AND s2 < s4 AND s3 < s4",
                "l1 < l2",
                "l1 < l2 AND l1 < l3 AND l1 < l4 AND l2 < l3 AND l2 < l4 AND l3 < l4"})
        private String expression;

        @Setup
        public void setup()
        {
            addSymbol("d1", DOUBLE);
            addSymbol("d2", DOUBLE);
            addSymbol("d3", DOUBLE);
            addSymbol("d4", DOUBLE);

            addSymbol("s1", SHORT_DECIMAL_TYPE);
            addSymbol("s2", SHORT_DECIMAL_TYPE);
            addSymbol("s3", SHORT_DECIMAL_TYPE);
            addSymbol("s4", SHORT_DECIMAL_TYPE);

            addSymbol("l1", LONG_DECIMAL_TYPE);
            addSymbol("l2", LONG_DECIMAL_TYPE);
            addSymbol("l3", LONG_DECIMAL_TYPE);
            addSymbol("l4", LONG_DECIMAL_TYPE);

            generateInputPage(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
            generateProcessor(expression);
            generatePageBuilder(BOOLEAN);
        }
    }

    @Benchmark
    public Page inequalityBenchmark(InequalityBenchmarkState state)
    {
        return execute(state);
    }

    private Page execute(BaseState state)
    {
        Page inputPage = state.getInputPage();
        PageBuilder pageBuilder = state.getPageBuilder();
        PageProcessor processor = state.getProcessor();

        pageBuilder.reset();
        checkState(processor.process(null, inputPage, 0, inputPage.getPositionCount(), pageBuilder) == PAGE_SIZE);
        return pageBuilder.build();
    }

    private static class BaseState
    {
        private final Map<Symbol, Type> symbolTypes = new HashMap<>();
        private final Map<Symbol, Integer> sourceLayout = new HashMap<>();
        private final List<Type> types = new LinkedList<>();

        private Page inputPage;
        private PageBuilder pageBuilder;
        private PageProcessor processor;

        public Page getInputPage()
        {
            return inputPage;
        }

        public PageBuilder getPageBuilder()
        {
            return pageBuilder;
        }

        public PageProcessor getProcessor()
        {
            return processor;
        }

        protected void addSymbol(String name, Type type)
        {
            Symbol symbol = new Symbol(name);
            symbolTypes.put(symbol, type);
            sourceLayout.put(symbol, types.size());
            types.add(type);
        }

        protected void generateInputPage(int... initialValues)
        {
            RowPagesBuilder buildPagesBuilder = rowPagesBuilder(types);
            buildPagesBuilder.addSequencePage(PAGE_SIZE, initialValues);
            inputPage = getOnlyElement(buildPagesBuilder.build());
        }

        protected void generatePageBuilder(Type type)
        {
            pageBuilder = new PageBuilder(ImmutableList.of(type));
        }

        protected void generateProcessor(String expression)
        {
            processor = new ExpressionCompiler(createTestMetadataManager()).compilePageProcessor(rowExpression("true"), ImmutableList.of(rowExpression(expression))).get();
        }

        private RowExpression rowExpression(String expression)
        {
            SymbolToInputRewriter symbolToInputRewriter = new SymbolToInputRewriter(sourceLayout);
            Expression inputReferenceExpression = ExpressionTreeRewriter.rewriteWith(symbolToInputRewriter, createExpression(expression, METADATA, symbolTypes));

            Map<Integer, Type> types = sourceLayout.entrySet().stream()
                    .collect(toMap(Map.Entry::getValue, entry -> symbolTypes.get(entry.getKey())));

            IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(TEST_SESSION, METADATA, SQL_PARSER, types, inputReferenceExpression);
            return SqlToRowExpressionTranslator.translate(inputReferenceExpression, SCALAR, expressionTypes, METADATA.getFunctionRegistry(), METADATA.getTypeManager(), TEST_SESSION, true);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkDecimalOperators.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
