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
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
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
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.scalar.FunctionAssertions.createExpression;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 3)
@Warmup(iterations = 20, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDecimalOperators
{
    private static final int PAGE_SIZE = 30000;

    private static final DecimalType SHORT_DECIMAL_TYPE = createDecimalType(10, 0);
    private static final DecimalType LONG_DECIMAL_TYPE = createDecimalType(20, 0);

    private static final SqlParser SQL_PARSER = new SqlParser();

    @State(Thread)
    public static class CastDoubleToDecimalBenchmarkState
            extends BaseState
    {
        private static final int SCALE = 2;

        @Param({"10", "35", "BIGINT"})
        private String precision = "10";

        @Setup
        public void setup()
        {
            addSymbol("d1", DOUBLE);

            String expression;
            if (precision.equals("BIGINT")) {
                setDoubleMaxValue(Long.MAX_VALUE);
                expression = "CAST(d1 AS BIGINT)";
            }
            else {
                setDoubleMaxValue(Math.pow(9, Integer.valueOf(precision) - SCALE));
                expression = String.format("CAST(d1 AS DECIMAL(%s, %d))", precision, SCALE);
            }
            generateRandomInputPage();
            generateProcessor(expression);
        }
    }

    @Benchmark
    public Object castDoubleToDecimalBenchmark(CastDoubleToDecimalBenchmarkState state)
    {
        return execute(state);
    }

    @Test
    public void testCastDoubleToDecimalBenchmark()
    {
        CastDoubleToDecimalBenchmarkState state = new CastDoubleToDecimalBenchmarkState();
        state.setup();
        castDoubleToDecimalBenchmark(state);
    }

    @State(Thread)
    public static class CastDecimalToDoubleBenchmarkState
            extends BaseState
    {
        private static final int SCALE = 10;

        @Param({"15", "35"})
        private String precision = "15";

        @Setup
        public void setup()
        {
            addSymbol("v1", createDecimalType(Integer.valueOf(precision), SCALE));

            String expression = "CAST(v1 AS DOUBLE)";
            generateRandomInputPage();
            generateProcessor(expression);
        }
    }

    @Benchmark
    public Object castDecimalToDoubleBenchmark(CastDecimalToDoubleBenchmarkState state)
    {
        return execute(state);
    }

    @Test
    public void testCastDecimalToDoubleBenchmark()
    {
        CastDecimalToDoubleBenchmarkState state = new CastDecimalToDoubleBenchmarkState();
        state.setup();
        castDecimalToDoubleBenchmark(state);
    }

    @State(Thread)
    public static class CastDecimalToVarcharBenchmarkState
            extends BaseState
    {
        private static final int SCALE = 10;

        @Param({"15", "35"})
        private String precision = "35";

        @Setup
        public void setup()
        {
            addSymbol("v1", createDecimalType(Integer.valueOf(precision), SCALE));

            String expression = "CAST(v1 AS VARCHAR)";
            generateRandomInputPage();
            generateProcessor(expression);
        }
    }

    @Benchmark
    public Object castDecimalToVarcharBenchmark(CastDecimalToVarcharBenchmarkState state)
    {
        return execute(state);
    }

    @Test
    public void testCastDecimalToVarcharBenchmark()
    {
        CastDecimalToVarcharBenchmarkState state = new CastDecimalToVarcharBenchmarkState();
        state.setup();
        castDecimalToVarcharBenchmark(state);
    }

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
        private String expression = "d1 + d2";

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
        }
    }

    @Benchmark
    public Object additionBenchmark(AdditionBenchmarkState state)
    {
        return execute(state);
    }

    @Test
    public void testAdditionBenchmark()
    {
        AdditionBenchmarkState state = new AdditionBenchmarkState();
        state.setup();
        additionBenchmark(state);
    }

    @State(Thread)
    public static class MultiplyBenchmarkState
            extends BaseState
    {
        @Param({"d1 * d2",
                "d1 * d2 * d3 * d4",
                "i1 * i2",
                // short short -> short
                "s1 * s2",
                "s1 * s2 * s5 * s6",
                // short short -> long
                "s3 * s4",
                // long short -> long
                "l2 * s2",
                "l2 * s2 * s5 * s6",
                // short long -> long
                "s1 * l2",
                // long long -> long
                "l1 * l2"})
        private String expression = "d1 * d2";

        @Setup
        public void setup()
        {
            addSymbol("d1", DOUBLE);
            addSymbol("d2", DOUBLE);
            addSymbol("d3", DOUBLE);
            addSymbol("d4", DOUBLE);

            addSymbol("i1", BIGINT);
            addSymbol("i2", BIGINT);

            addSymbol("s1", createDecimalType(5, 2));
            addSymbol("s2", createDecimalType(3, 1));
            addSymbol("s3", createDecimalType(10, 5));
            addSymbol("s4", createDecimalType(10, 2));
            addSymbol("s5", createDecimalType(3, 2));
            addSymbol("s6", createDecimalType(2, 1));

            addSymbol("l1", createDecimalType(19, 10));
            addSymbol("l2", createDecimalType(19, 5));

            generateRandomInputPage();
            generateProcessor(expression);
        }
    }

    @Benchmark
    public Object multiplyBenchmark(MultiplyBenchmarkState state)
    {
        return execute(state);
    }

    @Test
    public void testMultiplyBenchmark()
    {
        MultiplyBenchmarkState state = new MultiplyBenchmarkState();
        state.setup();
        multiplyBenchmark(state);
    }

    @State(Thread)
    public static class DivisionBenchmarkState
            extends BaseState
    {
        @Param({"d1 / d2",
                "d1 / d2 / d3 / d4",
                "i1 / i2",
                "i1 / i2 / i3 / i4",
                // short short -> short
                "s1 / s2",
                "s1 / s2 / s2 / s2",
                // short short -> long
                "s1 / s3",
                // short long -> short
                "s2 / l1",
                // long short -> long
                "l1 / s2",
                // short long -> long
                "s3 / l1",
                // long long -> long
                "l2 / l3",
                "l2 / l4 / l4 / l4",
                "l2 / s4 / s4 / s4"})
        private String expression = "d1 / d2";

        @Setup
        public void setup()
        {
            addSymbol("d1", DOUBLE);
            addSymbol("d2", DOUBLE);
            addSymbol("d3", DOUBLE);
            addSymbol("d4", DOUBLE);

            addSymbol("i1", BIGINT);
            addSymbol("i2", BIGINT);
            addSymbol("i3", BIGINT);
            addSymbol("i4", BIGINT);

            addSymbol("s1", createDecimalType(8, 3));
            addSymbol("s2", createDecimalType(6, 2));
            addSymbol("s3", createDecimalType(17, 7));
            addSymbol("s4", createDecimalType(3, 2));

            addSymbol("l1", createDecimalType(19, 3));
            addSymbol("l2", createDecimalType(20, 3));
            addSymbol("l3", createDecimalType(21, 10));
            addSymbol("l4", createDecimalType(19, 4));

            generateRandomInputPage();
            generateProcessor(expression);
        }
    }

    @Benchmark
    public Object divisionBenchmark(DivisionBenchmarkState state)
    {
        return execute(state);
    }

    @Test
    public void testDivisionBenchmark()
    {
        DivisionBenchmarkState state = new DivisionBenchmarkState();
        state.setup();
        divisionBenchmark(state);
    }

    @State(Thread)
    public static class ModuloBenchmarkState
            extends BaseState
    {
        @Param({"d1 % d2",
                "d1 % d2 % d3 % d4",
                "i1 % i2",
                "i1 % i2 % i3 % i4",
                // short short -> short
                "s1 % s2",
                "s1 % s2 % s2 % s2",
                // short long -> short
                "s2 % l2",
                // long short -> long
                "l3 % s3",
                // short long -> long
                "s4 % l3",
                // long long -> long
                "l2 % l3",
                "l2 % l3 % l4 % l1"})
        private String expression = "d1 % d2";

        @Setup
        public void setup()
        {
            addSymbol("d1", DOUBLE);
            addSymbol("d2", DOUBLE);
            addSymbol("d3", DOUBLE);
            addSymbol("d4", DOUBLE);

            addSymbol("i1", BIGINT);
            addSymbol("i2", BIGINT);
            addSymbol("i3", BIGINT);
            addSymbol("i4", BIGINT);

            addSymbol("s1", createDecimalType(8, 3));
            addSymbol("s2", createDecimalType(6, 2));
            addSymbol("s3", createDecimalType(9, 0));
            addSymbol("s4", createDecimalType(12, 2));

            addSymbol("l1", createDecimalType(19, 3));
            addSymbol("l2", createDecimalType(20, 3));
            addSymbol("l3", createDecimalType(21, 10));
            addSymbol("l4", createDecimalType(19, 4));

            generateRandomInputPage();
            generateProcessor(expression);
        }
    }

    @Benchmark
    public Object moduloBenchmark(ModuloBenchmarkState state)
    {
        return execute(state);
    }

    @Test
    public void testModuloBenchmark()
    {
        ModuloBenchmarkState state = new ModuloBenchmarkState();
        state.setup();
        moduloBenchmark(state);
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
        private String expression = "d1 < d2";

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
        }
    }

    @Benchmark
    public Object inequalityBenchmark(InequalityBenchmarkState state)
    {
        return execute(state);
    }

    @Test
    public void testInequalityBenchmark()
    {
        InequalityBenchmarkState state = new InequalityBenchmarkState();
        state.setup();
        inequalityBenchmark(state);
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
        private String expression = "cast(l_38_30 as decimal(8, 0))";

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
        }
    }

    @Benchmark
    public Object decimalToShortDecimalCastBenchmark(DecimalToShortDecimalCastBenchmarkState state)
    {
        return execute(state);
    }

    @Test
    public void testDecimalToShortDecimalCastBenchmark()
    {
        DecimalToShortDecimalCastBenchmarkState state = new DecimalToShortDecimalCastBenchmarkState();
        state.setup();
        decimalToShortDecimalCastBenchmark(state);
    }

    private Object execute(BaseState state)
    {
        return ImmutableList.copyOf(state.getProcessor().process(SESSION, new DriverYieldSignal(), state.getInputPage()));
    }

    private static class BaseState
    {
        private final MetadataManager metadata = createTestMetadataManager();
        private final Session session = testSessionBuilder().build();
        private final Random random = new Random();

        protected final Map<String, Symbol> symbols = new HashMap<>();
        protected final Map<Symbol, Type> symbolTypes = new HashMap<>();
        private final Map<Symbol, Integer> sourceLayout = new HashMap<>();
        protected final List<Type> types = new LinkedList<>();

        protected Page inputPage;
        private PageProcessor processor;
        private double doubleMaxValue = 2L << 31;

        public Page getInputPage()
        {
            return inputPage;
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

        protected void generateRandomInputPage()
        {
            RowPagesBuilder buildPagesBuilder = rowPagesBuilder(types);

            for (int i = 0; i < PAGE_SIZE; i++) {
                Object[] values = types.stream()
                        .map(this::generateRandomValue)
                        .collect(toList()).toArray();

                buildPagesBuilder.row(values);
            }

            inputPage = getOnlyElement(buildPagesBuilder.build());
        }

        protected void generateInputPage(int... initialValues)
        {
            RowPagesBuilder buildPagesBuilder = rowPagesBuilder(types);
            buildPagesBuilder.addSequencePage(PAGE_SIZE, initialValues);
            inputPage = getOnlyElement(buildPagesBuilder.build());
        }

        protected void generateProcessor(String expression)
        {
            processor = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0)).compilePageProcessor(Optional.empty(), ImmutableList.of(rowExpression(expression))).get();
        }

        protected void setDoubleMaxValue(double doubleMaxValue)
        {
            this.doubleMaxValue = doubleMaxValue;
        }

        private RowExpression rowExpression(String expression)
        {
            Expression inputReferenceExpression = new SymbolToInputRewriter(sourceLayout).rewrite(createExpression(expression, metadata, TypeProvider.copyOf(symbolTypes)));

            Map<Integer, Type> types = sourceLayout.entrySet().stream()
                    .collect(toMap(Map.Entry::getValue, entry -> symbolTypes.get(entry.getKey())));

            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypesFromInput(TEST_SESSION, metadata, SQL_PARSER, types, inputReferenceExpression, emptyList());
            return SqlToRowExpressionTranslator.translate(inputReferenceExpression, SCALAR, expressionTypes, metadata.getFunctionRegistry(), metadata.getTypeManager(), TEST_SESSION, true);
        }

        private Object generateRandomValue(Type type)
        {
            if (type instanceof DoubleType) {
                return random.nextDouble() * (2L * doubleMaxValue) - doubleMaxValue;
            }
            else if (type instanceof DecimalType) {
                return randomDecimal((DecimalType) type);
            }
            else if (type instanceof BigintType) {
                int randomInt = random.nextInt();
                return randomInt == 0 ? 1 : randomInt;
            }
            throw new UnsupportedOperationException(type.toString());
        }

        private SqlDecimal randomDecimal(DecimalType type)
        {
            int maxBits = (int) (Math.log(Math.pow(10, type.getPrecision())) / Math.log(2));
            BigInteger bigInteger = new BigInteger(maxBits, random);

            if (bigInteger.equals(ZERO)) {
                bigInteger = ONE;
            }

            if (random.nextBoolean()) {
                bigInteger = bigInteger.negate();
            }

            return new SqlDecimal(bigInteger, type.getPrecision(), type.getScale());
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
