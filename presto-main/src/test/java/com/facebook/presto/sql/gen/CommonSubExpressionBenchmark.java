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

import com.facebook.presto.SequencePageBuilder;
import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.index.PageRecordSet;
import com.facebook.presto.operator.project.CursorProcessor;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.scalar.FunctionAssertions.createExpression;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class CommonSubExpressionBenchmark
{
    private static final Map<String, Type> TYPE_MAP = ImmutableMap.of("bigint", BIGINT, "varchar", VARCHAR, "json", VARCHAR);
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = createTestMetadataManager();
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final int POSITIONS = 1024;

    private PageProcessor pageProcessor;
    private CursorProcessor cursorProcessor;
    private Page inputPage;
    private Map<String, Type> symbolTypes;
    private Map<VariableReferenceExpression, Integer> sourceLayout;
    private List<Type> projectionTypes;

    @Param({"json", "bigint", "varchar"})
    String functionType;

    @Param({"true", "false"})
    boolean optimizeCommonSubExpression;

    @Param({"true", "false"})
    boolean dictionaryBlocks;

    @Setup
    public void setup()
    {
        Type type = TYPE_MAP.get(this.functionType);

        VariableReferenceExpression variable = new VariableReferenceExpression(type.getDisplayName().toLowerCase(ENGLISH) + "0", type);
        symbolTypes = ImmutableMap.of(variable.getName(), type);
        sourceLayout = ImmutableMap.of(variable, 0);
        inputPage = createPage(functionType, dictionaryBlocks);

        List<RowExpression> projections = getProjections(this.functionType);

        projectionTypes = projections.stream().map(RowExpression::getType).collect(toList());

        MetadataManager metadata = createTestMetadataManager();
        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(metadata, 0);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(metadata, pageFunctionCompiler);
        pageProcessor = expressionCompiler.compilePageProcessor(TEST_SESSION.getSqlFunctionProperties(), Optional.of(getFilter(functionType)), projections, optimizeCommonSubExpression, Optional.empty()).get();
        cursorProcessor = expressionCompiler.compileCursorProcessor(TEST_SESSION.getSqlFunctionProperties(), Optional.of(getFilter(functionType)), projections, "key", optimizeCommonSubExpression).get();
    }

    @Benchmark
    public List<Optional<Page>> computePage()
    {
        return ImmutableList.copyOf(
                pageProcessor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        inputPage));
    }

    @Benchmark
    public Optional<Page> ComputeRecordSet()
    {
        List<Type> types = ImmutableList.of(TYPE_MAP.get(this.functionType));
        PageBuilder pageBuilder = new PageBuilder(projectionTypes);
        RecordSet recordSet = new PageRecordSet(types, inputPage);

        cursorProcessor.process(
                null,
                new DriverYieldSignal(),
                recordSet.cursor(),
                pageBuilder);

        return Optional.of(pageBuilder.build());
    }

    private RowExpression getFilter(String functionType)
    {
        if (functionType.equals("varchar")) {
            return rowExpression("cast(varchar0 as bigint) % 2 = 0");
        }
        if (functionType.equals("bigint")) {
            return rowExpression("bigint0 % 2 = 0");
        }
        if (functionType.equals("json")) {
            return rowExpression("rand() < 0.5");
        }
        throw new IllegalArgumentException("filter not supported for type : " + functionType);
    }

    private List<RowExpression> getProjections(String functionType)
    {
        ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
        if (functionType.equals("bigint")) {
            return ImmutableList.of(rowExpression("bigint0 + bigint0"), rowExpression("bigint0 + bigint0 + 5"));
        }
        else if (functionType.equals("varchar")) {
            return ImmutableList.of(rowExpression("concat(varchar0, varchar0)"), rowExpression("concat(concat(varchar0, varchar0), 'foo')"));
        }
        else if (functionType.equals("json")) {
            return ImmutableList.of(rowExpression("json_extract(json_parse(varchar0), '$.a')"), rowExpression("json_extract(json_parse(varchar0), '$.b')"));
        }
        throw new IllegalArgumentException();
    }

    private RowExpression rowExpression(String value)
    {
        Expression expression = createExpression(value, METADATA, TypeProvider.copyOf(symbolTypes));

        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(TEST_SESSION, METADATA, SQL_PARSER, TypeProvider.copyOf(symbolTypes), expression, emptyList(), WarningCollector.NOOP);
        RowExpression rowExpression = SqlToRowExpressionTranslator.translate(expression, expressionTypes, sourceLayout, METADATA.getFunctionAndTypeManager(), TEST_SESSION);
        RowExpressionOptimizer optimizer = new RowExpressionOptimizer(METADATA);
        return optimizer.optimize(rowExpression, OPTIMIZED, TEST_SESSION.toConnectorSession());
    }

    private static Page createPage(String functionType, boolean dictionary)
    {
        List<Type> types = ImmutableList.of(TYPE_MAP.get(functionType));
        switch (functionType) {
            case "bigint":
            case "varchar":
                if (dictionary) {
                    return SequencePageBuilder.createSequencePageWithDictionaryBlocks(types, POSITIONS);
                }
                else {
                    return SequencePageBuilder.createSequencePage(types, POSITIONS);
                }
            case "json":
                if (dictionary) {
                    return createDictionaryStringJsonPage();
                }
                else {
                    return createStringJsonPage();
                }
            default:
                throw new IllegalArgumentException();
        }
    }

    private static Page createStringJsonPage()
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, POSITIONS);

        for (int i = 0; i < POSITIONS; i++) {
            VARCHAR.writeString(builder, "{\"a\": 1, \"b\": 2}");
        }
        return new Page(builder.build());
    }

    private static Page createDictionaryStringJsonPage()
    {
        int dictionarySize = POSITIONS / 5;
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, dictionarySize);
        for (int i = 0; i < dictionarySize; i++) {
            VARCHAR.writeString(builder, "{\"a\": 1, \"b\": 2}");
        }
        int[] ids = new int[POSITIONS];
        for (int i = 0; i < POSITIONS; i++) {
            ids[i] = i % dictionarySize;
        }
        return new Page(new DictionaryBlock(builder.build(), ids));
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + CommonSubExpressionBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
