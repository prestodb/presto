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
package com.facebook.presto.sql;

import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.block.MethodHandleUtil;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.relation.column.ColumnExpression;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.TestMapType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.ExpressionEquivalence;
import com.facebook.presto.sql.relational.SqlToColumnExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.relational.ColumnExpressionToSqlTranslator.translate;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertTrue;

public class TestColumnExpressionToSqlTranslator
{
    private final Metadata metadata = MetadataManager.createTestMetadataManager();
    private final LiteralEncoder literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());

    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();
    private static final ExpressionEquivalence EQUIVALENCE = new ExpressionEquivalence(METADATA, SQL_PARSER);
    private static final RowType ROW_TYPE = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("x"), INTEGER)));
    private static final MapType MAP_TYPE = new MapType(
            createVarcharType(42),
            BIGINT,
            MethodHandleUtil.methodHandle(TestMapType.class, "throwUnsupportedOperation"),
            MethodHandleUtil.methodHandle(TestMapType.class, "throwUnsupportedOperation"),
            MethodHandleUtil.methodHandle(TestMapType.class, "throwUnsupportedOperation"),
            MethodHandleUtil.methodHandle(TestMapType.class, "throwUnsupportedOperation"));
    private static final Map<String, ColumnHandle> COLUMNS = new ImmutableMap.Builder<String, ColumnHandle>()
            .put("c1", new TestColumnHandle("c1", BIGINT))
            .put("c2", new TestColumnHandle("c2", BIGINT))
            .put("c3", new TestColumnHandle("c3", VARCHAR))
            .put("c4", new TestColumnHandle("c4", ROW_TYPE))
            .put("c5", new TestColumnHandle("c5", new ArrayType(INTEGER)))
            .put("c6", new TestColumnHandle("c6", MAP_TYPE))
            .build();
    private static final Map<Symbol, Type> TYPES = COLUMNS.values()
            .stream()
            .map(TestColumnHandle.class::cast)
            .collect(toMap(column -> new Symbol(column.getColumnName()), column -> column.getType()));
    private static final Map<ColumnHandle, String> columnReverse = COLUMNS.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getValue, Map.Entry::getKey));

    @Test
    public void testConstantExpressionRoundTrip()
    {
        assertExpressionConversion("NULL");
        assertExpressionConversion("TRUE");
        assertExpressionConversion("FALSE");
        assertExpressionConversion("1");
        assertExpressionConversion("1.1");
        assertExpressionConversion("DECIMAL '10.3'");
        assertExpressionConversion("BIGINT '1'");
        assertExpressionConversion("'string'");
        assertExpressionConversion("DATE '2001-01-02'");
        assertExpressionConversion("ARRAY [1, 2, 3]");
        assertExpressionConversion("MAP(ARRAY [1, 2, 3], ARRAY ['a', 'b', 'c'])");
        assertExpressionConversion("ROW(1, 2)");
        assertExpressionConversion("TIMESTAMP '2001-08-22 03:04:05.321'");
        assertExpressionConversion("x'01 0f'");
        // TODO following evaluates to magical literal thus cannot compare
//        assertExpressionConversion("TIMESTAMP '2001-08-22 03:04:05.321 America/Los_Angeles'");
//        assertExpressionConversion("INTERVAL '2' DAY");
    }

    @Test
    public void testCast()
    {
        assertExpressionConversion("CAST(ROW(1, 2) AS ROW(x BIGINT, y INTEGER))");
        assertExpressionConversion("CAST(NULL AS VARCHAR)");
        assertExpressionConversion("CAST('TRUE' AS BOOLEAN)");
        assertExpressionConversion("CAST('1' AS BIGINT)");
        assertExpressionConversion("CAST('1' AS INTEGER)");
        assertExpressionConversion("CAST('1' AS SMALLINT)");
//        assertExpressionConversion("cast(TIME '03:04:05.321' as varchar)");
//        assertExpressionConversion("cast(TIMESTAMP '2017-06-06 10:00:00.000 Asia/Kathmandu' as time with time zone)");
        assertExpressionConversion("CAST(infinity() AS JSON)");
    }

    @Test
    public void testSimpleExpressionRoundTrip()
    {
        assertExpressionConversion("c1 + c2");
        assertExpressionConversion("c1 + BIGINT '1'");
        assertExpressionConversion("c1 - c2");
        assertExpressionConversion("c1 - BIGINT '1'");
        assertExpressionConversion("c1 * c2");
        assertExpressionConversion("c1 * BIGINT '1'");
        assertExpressionConversion("c1 / c2");
        assertExpressionConversion("c1 / BIGINT '1'");
        assertExpressionConversion("c1 % c2");
        assertExpressionConversion("c1 % BIGINT '1'");

        assertExpressionConversion("c1 < c2");
        assertExpressionConversion("c1 <=  c2");
        assertExpressionConversion("c1 >  c2");
        assertExpressionConversion("c1 >=  c2");
        assertExpressionConversion("c1 =  c2");
        assertExpressionConversion("c1 <> c2");
        assertExpressionConversion("NOT c1 > 1");
        assertExpressionConversion("TRUE AND TRUE AND FALSE");
        assertExpressionConversion("TRUE OR TRUE OR FALSE");
        assertExpressionConversion("c1 IN (1, 2, 3)");

        assertExpressionConversion("IF(c1 > 0, 1)");
        assertExpressionConversion("c4 IS NULL");
        assertExpressionConversion("c4 IS NOT NULL");
        assertExpressionConversion("COALESCE(CAST(NULL AS INTEGER), 1, 2)");
        assertExpressionConversion("c3 LIKE '%c3%'");
        assertExpressionConversion("'abc' LIKE 'a\\_c' ESCAPE '\\'");

        assertExpressionConversion("TRY_CAST(c1 AS VARCHAR)");
        assertExpressionConversion("TRY(c1 + 1)");
        assertExpressionConversion("TRY(CAST(c1 AS VARCHAR))");
        assertExpressionConversion("typeof(c1)");

        assertExpressionConversion("TRANSFORM(ARRAY [1, 2,  3], x -> x + 1)");
        assertExpressionConversion("REDUCE(ARRAY [5, 20, 50], 0, (s, x) -> s + x, s -> s)");

        assertExpressionConversion("c4.x");
        assertExpressionConversion("c5[1]");
        assertExpressionConversion("c6['a']");
    }

    @Test
    public void testComplexExpressionRoundTrip()
    {
        assertExpressionConversion("c1 > BIGINT '100' AND (c1 + c2) > BIGINT '100' AND hamming_distance(c3, 'test_str') > BIGINT '10' " +
                "AND cardinality(filter(ARRAY[5, -6, CAST(NULL AS INTEGER), 7], x -> x > 0)) > BIGINT '0'");
    }

    private static Expression expression(String sql)
    {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql, new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE)));
    }

    private void assertExpressionConversion(@Language("SQL") String sql)
    {
        // must explicitly cast the type since we won't add coercing here.
        // we don't want simplify or optimization to mess up the equivalence comparison.
        Expression expression = expression(sql);

        ColumnExpression columnExpression = translateAndOptimize(expression, SCALAR, TYPES, COLUMNS);
        Optional<Expression> resultExpression = translate(columnExpression, ImmutableList.of(), columnReverse, literalEncoder, metadata.getFunctionRegistry());
        assertTrue(resultExpression.isPresent());
        assertEquivalent(expression, resultExpression.get(), TYPES);
    }

    private ColumnExpression translateAndOptimize(Expression expression, FunctionKind kind, Map<Symbol, Type> symbolTypeMap, Map<String, ColumnHandle> columns)
    {
        return SqlToColumnExpressionTranslator.translate(expression, kind, getExpressionTypes(expression, symbolTypeMap), columns, metadata.getFunctionRegistry(), metadata.getTypeManager(), TEST_SESSION, false);
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Expression expression, Map<Symbol, Type> symbolTypes)
    {
        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata.getFunctionRegistry(),
                metadata.getTypeManager(),
                TEST_SESSION,
                TypeProvider.copyOf(symbolTypes),
                emptyList(),
                node -> new IllegalStateException("Unexpected node: %s" + node),
                WarningCollector.NOOP,
                false);
        expressionAnalyzer.analyze(expression, Scope.create());
        return expressionAnalyzer.getExpressionTypes();
    }

    private static void assertEquivalent(Expression leftExpression, Expression rightExpression, Map<Symbol, Type> typeMap)
    {
        TypeProvider types = TypeProvider.copyOf(typeMap);

        assertTrue(
                EQUIVALENCE.areExpressionsEquivalent(TEST_SESSION, leftExpression, rightExpression, types),
                String.format("Expected (%s) and (%s) to be equivalent", leftExpression.toString(), rightExpression.toString()));
        assertTrue(
                EQUIVALENCE.areExpressionsEquivalent(TEST_SESSION, rightExpression, leftExpression, types),
                String.format("Expected (%s) and (%s) to be equivalent", leftExpression.toString(), rightExpression.toString()));
    }

    private static class TestColumnHandle
            implements ColumnHandle
    {
        private final String columnName;
        private final Type type;

        public TestColumnHandle(String columnName, Type type)
        {
            this.columnName = columnName;
            this.type = type;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public Type getType()
        {
            return type;
        }
    }
}
