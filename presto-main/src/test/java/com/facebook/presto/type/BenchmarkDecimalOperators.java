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
import com.facebook.presto.Session;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.ExpressionAnalysis;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.RelationType;
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

import java.math.BigInteger;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.scalar.FunctionAssertions.createExpression;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toMap;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 3, jvmArgs = "-XX:MaxInlineSize=100")
@Warmup(iterations = 20, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDecimalOperators
{
    private static final int PAGE_SIZE = 30000;

    private static final DecimalType SHORT_DECIMAL_TYPE = createDecimalType(10, 0);
    private static final DecimalType LONG_DECIMAL_TYPE = createDecimalType(20, 0);

    private static final SqlParser SQL_PARSER = new SqlParser();

    @State(Thread)
    public static class AdditionBenchmarkState
            extends BaseState
    {
        @Param({"d1 + d2",
                "d1 + d2 + d3 + d4",
                "s1 + s2",
                "s1 + s2 + s3 + s4",
                "l1 + l2",
                "l1 + l2 + l3 + l4",
                "s2 + l3 + l1 + s4"})
        private String expression;

        @Setup
        public void setup()
        {
            addSymbol("d1", DOUBLE);
            addSymbol("d2", DOUBLE);
            addSymbol("d3", DOUBLE);
            addSymbol("d4", DOUBLE);

            addSymbol("s1", createDecimalType(10, 5));
            addSymbol("s2", createDecimalType(7, 2));
            addSymbol("s3", createDecimalType(12, 2));
            addSymbol("s4", createDecimalType(2, 1));

            addSymbol("l1", createDecimalType(35, 10));
            addSymbol("l2", createDecimalType(25, 5));
            addSymbol("l3", createDecimalType(20, 6));
            addSymbol("l4", createDecimalType(25, 8));

            generateRandomInputPage();
            generateProcessor(expression);
            generateResultPageBuilder(expression);
        }

        private void generateRandomInputPage()
        {
            Random random = new Random();
            RowPagesBuilder buildPagesBuilder = rowPagesBuilder(types);

            SqlDecimal decimal = randomDecimal("s1", random);

            for (int i = 0; i < PAGE_SIZE; i++) {
                buildPagesBuilder.row(
                        random.nextDouble() * (2L << 32) - (2L << 31),
                        random.nextDouble() * (2L << 32) - (2L << 31),
                        random.nextDouble() * (2L << 32) - (2L << 31),
                        random.nextDouble() * (2L << 32) - (2L << 31),
                        randomDecimal("s1", random),
                        randomDecimal("s2", random),
                        randomDecimal("s3", random),
                        randomDecimal("s4", random),
                        randomDecimal("l1", random),
                        randomDecimal("l2", random),
                        randomDecimal("l3", random),
                        randomDecimal("l4", random));
            }

            inputPage = getOnlyElement(buildPagesBuilder.build());
        }

        private SqlDecimal randomDecimal(String symbol, Random random)
        {
            DecimalType type = (DecimalType) symbolTypes.get(symbols.get(symbol));
            int maxBits = (int) (Math.log(Math.pow(10, type.getPrecision())) / Math.log(2));
            BigInteger bigInteger = new BigInteger(maxBits, random);

            if (random.nextBoolean()) {
                bigInteger = bigInteger.negate();
            }

            return new SqlDecimal(bigInteger, type.getPrecision(), type.getScale());
        }
    }

    @Benchmark
    public List<Page> additionBenchmark(AdditionBenchmarkState state)
    {
        return execute(state);
    }

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
            generateResultPageBuilder(expression);
        }
    }

    @Benchmark
    public List<Page> inequalityBenchmark(InequalityBenchmarkState state)
    {
        return execute(state);
    }

    @State(Thread)
    public static class DecimalToShortDecimalCastBenchmarkState
            extends BaseState
    {
        @Param({"cast(l_38_30 as decimal(8, 0))",
                "cast(l_26_18 as decimal(8, 0))",
                "cast(l_20_12 as decimal(8, 0))",
                "cast(l_20_8 as decimal(8, 0))",
                "cast(s_17_9 as decimal(8, 0))"})
        private String expression;

        @Setup
        public void setup()
        {
            addSymbol("l_38_30", createDecimalType(38, 30));
            addSymbol("l_26_18", createDecimalType(26, 18));
            addSymbol("l_20_12", createDecimalType(20, 12));
            addSymbol("l_20_8", createDecimalType(20, 8));
            addSymbol("s_17_9", createDecimalType(17, 9));

            generateInputPage(10000, 10000, 10000, 10000, 10000);
            generateProcessor(expression);
            generateResultPageBuilder(expression);
        }
    }

    @Benchmark
    public List<Page> decimalToShortDecimalCastBenchmark(DecimalToShortDecimalCastBenchmarkState state)
    {
        return execute(state);
    }

    @State(Thread)
    public static class DecimalToDoubleCastBenchmarkState
            extends BaseState
    {
        @Param({"cast(l_38_30 as double)",
                "cast(l_38_2 as double)",
                "cast(s_17_9 as double)"})
        private String expression;

        @Setup
        public void setup()
        {
            addSymbol("l_38_30", createDecimalType(38, 30));
            addSymbol("l_38_2", createDecimalType(38, 2));
            addSymbol("s_17_9", createDecimalType(17, 9));

            generateInputPage(10000, 10000, 10000);
            generateProcessor(expression);
            generateResultPageBuilder(expression);
        }
    }

    @Benchmark
    public List<Page> decimalToDoubleCastBenchmark(DecimalToDoubleCastBenchmarkState state)
    {
        return execute(state);
    }

    private List<Page> execute(BaseState state)
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        Page inputPage = state.getInputPage();
        PageBuilder pageBuilder = state.getPageBuilder();
        PageProcessor processor = state.getProcessor();

        int currentPosition = 0;

        while (currentPosition < PAGE_SIZE) {
            pageBuilder.reset();
            currentPosition = processor.process(null, inputPage, currentPosition, inputPage.getPositionCount(), pageBuilder);
            pages.add(pageBuilder.build());
        }

        return pages.build();
    }

    private static class BaseState
    {
        private final MetadataManager metadata = createTestMetadataManager();
        private final Session session = testSessionBuilder().build();

        protected final Map<String, Symbol> symbols = new HashMap<>();
        protected final Map<Symbol, Type> symbolTypes = new HashMap<>();
        private final Map<Symbol, Integer> sourceLayout = new HashMap<>();
        protected final List<Type> types = new LinkedList<>();

        protected Page inputPage;
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
            symbols.put(name, symbol);
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

        protected void generateResultPageBuilder(String expression)
        {
            SqlParser sqlParser = new SqlParser();
            Expression parsedExpression = sqlParser.createExpression(expression);

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (Map.Entry<Symbol, Type> entry : symbolTypes.entrySet()) {
                fields.add(Field.newUnqualified(entry.getKey().getName(), entry.getValue()));
            }
            RelationType tupleDescriptor = new RelationType(fields.build());

            ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpressions(session, metadata, sqlParser, tupleDescriptor, symbolTypes, ImmutableList.of(parsedExpression), emptyList());
            Type resultType = expressionAnalysis.getType(parsedExpression);

            pageBuilder = new PageBuilder(ImmutableList.of(resultType));
        }

        protected void generateProcessor(String expression)
        {
            processor = new ExpressionCompiler(metadata).compilePageProcessor(rowExpression("true"), ImmutableList.of(rowExpression(expression))).get();
        }

        private RowExpression rowExpression(String expression)
        {
            SymbolToInputRewriter symbolToInputRewriter = new SymbolToInputRewriter(sourceLayout);
            Expression inputReferenceExpression = ExpressionTreeRewriter.rewriteWith(symbolToInputRewriter, createExpression(expression, metadata, symbolTypes));

            Map<Integer, Type> types = sourceLayout.entrySet().stream()
                    .collect(toMap(Map.Entry::getValue, entry -> symbolTypes.get(entry.getKey())));

            IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(TEST_SESSION, metadata, SQL_PARSER, types, inputReferenceExpression, emptyList());
            return SqlToRowExpressionTranslator.translate(inputReferenceExpression, SCALAR, expressionTypes, metadata.getFunctionRegistry(), metadata.getTypeManager(), TEST_SESSION, true);
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
