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

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.scalar.FunctionAssertions.createExpression;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkJsonExtract
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = createTestMetadataManager();
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    public static final ConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of());

    private static final int POSITION_COUNT = 100_000;
    private static final int ARRAY_SIZE = 20;
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private PageProcessor pageProcessor;
    private Page inputPage;
    private Map<String, Type> symbolTypes;
    private Map<VariableReferenceExpression, Integer> sourceLayout;

    @Param({"true", "false"})
    boolean isCanonicalizedJsonExtract;

    @Setup
    public void setup()
    {
        VariableReferenceExpression variable = new VariableReferenceExpression(Optional.empty(), VARCHAR.getDisplayName().toLowerCase(ENGLISH) + "0", VARCHAR);
        symbolTypes = ImmutableMap.of(variable.getName(), VARCHAR);
        sourceLayout = ImmutableMap.of(variable, 0);
        inputPage = new Page(createChannel());
        List<RowExpression> projections = ImmutableList.of(rowExpression("json_extract(varchar0, '$.key1')"), rowExpression("json_extract(varchar0, '$.key2')"));
        MetadataManager metadata = createTestMetadataManager();
        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(metadata, 0);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(metadata, pageFunctionCompiler);
        pageProcessor = expressionCompiler.compilePageProcessor(TEST_SESSION.getSqlFunctionProperties(), Optional.empty(), projections).get();
    }

    @Benchmark
    public List<Optional<Page>> computePage()
    {
        SqlFunctionProperties sqlFunctionProperties = SqlFunctionProperties.builder()
                .setTimeZoneKey(UTC_KEY)
                .setLegacyTimestamp(true)
                .setSessionStartTime(0)
                .setSessionLocale(ENGLISH).setSessionUser("user")
                .setCanonicalizedJsonExtract(isCanonicalizedJsonExtract)
                .build();

        return ImmutableList.copyOf(
                pageProcessor.process(
                        sqlFunctionProperties,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        inputPage));
    }

    private RowExpression rowExpression(String value)
    {
        Expression expression = createExpression(TEST_SESSION, value, METADATA, TypeProvider.copyOf(symbolTypes));
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(TEST_SESSION, METADATA, SQL_PARSER, TypeProvider.copyOf(symbolTypes), expression, emptyMap(), WarningCollector.NOOP);
        RowExpression rowExpression = SqlToRowExpressionTranslator.translate(expression, expressionTypes, sourceLayout, METADATA.getFunctionAndTypeManager(), TEST_SESSION);
        RowExpressionOptimizer optimizer = new RowExpressionOptimizer(METADATA);
        return optimizer.optimize(rowExpression, OPTIMIZED, TEST_SESSION.toConnectorSession());
    }

    private static Block createChannel()
    {
        BlockBuilder blockBuilder = JSON.createBlockBuilder(null, BenchmarkJsonExtract.POSITION_COUNT);
        for (int position = 0; position < BenchmarkJsonExtract.POSITION_COUNT; position++) {
            try (SliceOutput jsonSlice = new DynamicSliceOutput(20 * BenchmarkJsonExtract.ARRAY_SIZE)) {
                jsonSlice.appendByte('{');
                int k1Index = ThreadLocalRandom.current().nextInt(ARRAY_SIZE);
                int k2Index = ThreadLocalRandom.current().nextInt(ARRAY_SIZE);
                while (k2Index == k1Index) {
                    k2Index = ThreadLocalRandom.current().nextInt(ARRAY_SIZE);
                }

                for (int i = 0; i < ARRAY_SIZE; i++) {
                    String key;
                    if (i == k1Index) {
                        key = "key1";
                    }
                    else if (i == k2Index) {
                        key = "key2";
                    }
                    else {
                        key = generateRandomKey(ThreadLocalRandom.current().nextInt(5) + 1);
                    }
                    jsonSlice.appendBytes("\"".getBytes());
                    jsonSlice.appendBytes(key.getBytes());
                    jsonSlice.appendBytes("\"".getBytes());
                    jsonSlice.appendByte(':');
                    String value;
                    if (key.equals("key1") || key.equals("key2") || (ThreadLocalRandom.current().nextInt(10) & 1) == 0) {
                        value = generateNestedJsonValue();
                    }
                    else {
                        value = generateRandomJsonValue();
                    }
                    jsonSlice.appendBytes(value.getBytes());
                    if (i < ARRAY_SIZE - 1) {
                        jsonSlice.appendByte(','); // Add a comma between JSON objects
                    }
                }

                jsonSlice.appendByte('}');
                JSON.writeSlice(blockBuilder, jsonSlice.slice());
            }
            catch (Exception ignore) {
                // Ignore...
            }
        }
        return blockBuilder.build();
    }

    private static String generateRandomJsonValue()
    {
        int length = ThreadLocalRandom.current().nextInt(10) + 1;
        StringBuilder builder = new StringBuilder(length + 2);
        builder.append('"');
        for (int i = 0; i < length; i++) {
            char c = CHARACTERS.charAt(ThreadLocalRandom.current().nextInt(CHARACTERS.length()));
            if (c == '"') {
                builder.append('\\'); // escape double quote
            }
            builder.append(c);
        }
        builder.append('"');
        return builder.toString();
    }

    private static String generateNestedJsonValue()
    {
        int size = ThreadLocalRandom.current().nextInt(5) + 1;
        StringBuilder builder = new StringBuilder(size * 10);
        builder.append('{');
        for (int i = 0; i < size; i++) {
            String key = generateRandomKey(ThreadLocalRandom.current().nextInt(5) + 2);
            builder.append("\"").append(key).append("\":");
            builder.append(generateRandomJsonValue());
            if (i < size - 1) {
                builder.append(",");
            }
        }
        builder.append('}');
        return builder.toString();
    }

    private static String generateRandomKey(int len)
    {
        StringBuilder builder = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            builder.append(CHARACTERS.charAt(ThreadLocalRandom.current().nextInt(CHARACTERS.length())));
        }
        return builder.toString();
    }

    @Test
    public void verify()
    {
        BenchmarkJsonToArrayCast.BenchmarkData data = new BenchmarkJsonToArrayCast.BenchmarkData();
        data.setup();
        new BenchmarkJsonToArrayCast().benchmark(data);
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkJsonToArrayCast.BenchmarkData data = new BenchmarkJsonToArrayCast.BenchmarkData();
        data.setup();
        new BenchmarkJsonToArrayCast().benchmark(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkJsonExtract.class.getSimpleName() + ".*")
                .warmupMode(WarmupMode.BULK_INDI)
                .build();
        new Runner(options).run();
    }
}
