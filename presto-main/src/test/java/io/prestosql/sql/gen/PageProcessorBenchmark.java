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
import com.google.common.collect.ImmutableMap;
import io.prestosql.SequencePageBuilder;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.index.PageRecordSet;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolToInputRewriter;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.SqlToRowExpressionTranslator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.testing.TestingSession;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.scalar.FunctionAssertions.createExpression;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;

@SuppressWarnings({"PackageVisibleField", "FieldCanBeLocal"})
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(10)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class PageProcessorBenchmark
{
    private static final Map<String, Type> TYPE_MAP = ImmutableMap.of("bigint", BIGINT, "varchar", VARCHAR);
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = createTestMetadataManager();
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final int POSITIONS = 1024;

    private final DriverYieldSignal yieldSignal = new DriverYieldSignal();
    private final Map<Symbol, Type> symbolTypes = new HashMap<>();
    private final Map<Symbol, Integer> sourceLayout = new HashMap<>();

    private CursorProcessor cursorProcessor;
    private PageProcessor pageProcessor;
    private Page inputPage;
    private RecordSet recordSet;
    private List<Type> types;

    @Param({"2", "4", "8", "16", "32"})
    int columnCount;

    @Param({"varchar", "bigint"})
    String type;

    @Param({"false", "true"})
    boolean dictionaryBlocks;

    @Setup
    public void setup()
    {
        Type type = TYPE_MAP.get(this.type);

        for (int i = 0; i < columnCount; i++) {
            Symbol symbol = new Symbol(type.getDisplayName().toLowerCase(ENGLISH) + i);
            symbolTypes.put(symbol, type);
            sourceLayout.put(symbol, i);
        }

        List<RowExpression> projections = getProjections(type);
        types = projections.stream().map(RowExpression::getType).collect(toList());

        MetadataManager metadata = createTestMetadataManager();
        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(metadata, 0);

        inputPage = createPage(types, dictionaryBlocks);
        pageProcessor = new ExpressionCompiler(metadata, pageFunctionCompiler).compilePageProcessor(Optional.of(getFilter(type)), projections).get();

        recordSet = new PageRecordSet(types, inputPage);
        cursorProcessor = new ExpressionCompiler(metadata, pageFunctionCompiler).compileCursorProcessor(Optional.of(getFilter(type)), projections, "key").get();
    }

    @Benchmark
    public Page rowOriented()
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        cursorProcessor.process(null, yieldSignal, recordSet.cursor(), pageBuilder);
        return pageBuilder.build();
    }

    @Benchmark
    public List<Optional<Page>> columnOriented()
    {
        return ImmutableList.copyOf(
                pageProcessor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        inputPage));
    }

    private RowExpression getFilter(Type type)
    {
        if (type == VARCHAR) {
            return rowExpression("cast(varchar0 as bigint) % 2 = 0", VARCHAR);
        }
        if (type == BIGINT) {
            return rowExpression("bigint0 % 2 = 0", BIGINT);
        }
        throw new IllegalArgumentException("filter not supported for type : " + type);
    }

    private List<RowExpression> getProjections(Type type)
    {
        ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
        if (type == BIGINT) {
            for (int i = 0; i < columnCount; i++) {
                builder.add(rowExpression("bigint" + i + " + 5", type));
            }
        }
        else if (type == VARCHAR) {
            for (int i = 0; i < columnCount; i++) {
                // alternatively use identity expression rowExpression("varchar" + i, type) or
                // rowExpression("substr(varchar" + i + ", 1, 1)", type)
                builder.add(rowExpression("concat(varchar" + i + ", 'foo')", type));
            }
        }
        return builder.build();
    }

    private RowExpression rowExpression(String expression, Type type)
    {
        SymbolToInputRewriter symbolToInputRewriter = new SymbolToInputRewriter(sourceLayout);
        Expression inputReferenceExpression = symbolToInputRewriter.rewrite(createExpression(expression, METADATA, TypeProvider.copyOf(symbolTypes)));

        ImmutableMap.Builder<Integer, Type> builder = ImmutableMap.builder();
        for (int i = 0; i < columnCount; i++) {
            builder.put(i, type);
        }
        Map<Integer, Type> types = builder.build();

        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypesFromInput(TEST_SESSION, METADATA, SQL_PARSER, types, inputReferenceExpression, emptyList(), WarningCollector.NOOP);
        return SqlToRowExpressionTranslator.translate(inputReferenceExpression, SCALAR, expressionTypes, METADATA.getFunctionRegistry(), METADATA.getTypeManager(), TEST_SESSION, true);
    }

    private static Page createPage(List<? extends Type> types, boolean dictionary)
    {
        if (dictionary) {
            return SequencePageBuilder.createSequencePageWithDictionaryBlocks(types, POSITIONS);
        }
        else {
            return SequencePageBuilder.createSequencePage(types, POSITIONS);
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
}
