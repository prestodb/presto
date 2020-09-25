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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.collectCSEByLevel;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.getExpressionsPartitionedByCSE;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.rewriteExpressionWithCSE;
import static org.testng.Assert.assertEquals;

public class TestCommonSubExpressionRewritter
{
    private static final Session SESSION = TEST_SESSION;
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final TypeProvider TYPES = TypeProvider.viewOf(
            ImmutableMap.<String, Type>builder()
                    .put("x", BIGINT)
                    .put("y", BIGINT)
                    .put("z", BIGINT)
                    .put("add$cse", BIGINT)
                    .put("multiply$cse", BIGINT)
                    .put("add$cse_0", BIGINT)
                    .put("expr$cse", BIGINT).build());

    @Test
    void testGetExpressionsWithCSE()
    {
        List<RowExpression> expressions = ImmutableList.of(rowExpression("x + y"), rowExpression("(x + y) * 2"), rowExpression("x + 2"), rowExpression("y * (x + 2)"), rowExpression("x * y"));
        Map<List<RowExpression>, Boolean> expressionsWithCSE = getExpressionsPartitionedByCSE(expressions, 3);
        assertEquals(
                expressionsWithCSE,
                ImmutableMap.of(
                        ImmutableList.of(rowExpression("x + y"), rowExpression("(x + y) * 2")), true,
                        ImmutableList.of(rowExpression("x + 2"), rowExpression("y * (x + 2)")), true,
                        ImmutableList.of(rowExpression("x * y")), false));
        expressions = ImmutableList.of(rowExpression("x + y"), rowExpression("x * 2"), rowExpression("x + y + x * 2"), rowExpression("y * 2"), rowExpression("x + y * 2"));
        expressionsWithCSE = getExpressionsPartitionedByCSE(expressions, 3);
        assertEquals(
                expressionsWithCSE,
                ImmutableMap.of(
                        ImmutableList.of(rowExpression("x + y"), rowExpression("x + y + x * 2"), rowExpression("x * 2")), true,
                        ImmutableList.of(rowExpression("y * 2"), rowExpression("x + y * 2")), true));

        expressionsWithCSE = getExpressionsPartitionedByCSE(expressions, 2);
        assertEquals(
                expressionsWithCSE,
                ImmutableMap.of(
                        ImmutableList.of(rowExpression("x + y"), rowExpression("x + y + x * 2")), true,
                        ImmutableList.of(rowExpression("y * 2"), rowExpression("x + y * 2")), true,
                        ImmutableList.of(rowExpression("x * 2")), true));
    }

    @Test
    void testCollectCSEByLevel()
    {
        List<RowExpression> expressions = ImmutableList.of(rowExpression("x * 2 + y + z"), rowExpression("(x * 2 + y + 1) * 2"), rowExpression("(x * 2)  + (x * 2 + y + z)"));
        Map<Integer, Map<RowExpression, VariableReferenceExpression>> cseByLevel = collectCSEByLevel(expressions);
        assertEquals(cseByLevel, ImmutableMap.of(
                3, ImmutableMap.of(rowExpression("\"add$cse\" + z"), rowExpression("\"add$cse_0\"")),
                2, ImmutableMap.of(rowExpression("\"multiply$cse\" + y"), rowExpression("\"add$cse\"")),
                1, ImmutableMap.of(rowExpression("x * 2"), rowExpression("\"multiply$cse\""))));
    }

    @Test
    void testCollectCSEByLevelCaseStatement()
    {
        List<RowExpression> expressions = ImmutableList.of(rowExpression("1 + CASE WHEN x = 1 THEN y + z WHEN x = 2 THEN z * 2 END"), rowExpression("2 + CASE WHEN x = 1 THEN y + z WHEN x = 2 THEN z * 2 END"));
        Map<Integer, Map<RowExpression, VariableReferenceExpression>> cseByLevel = collectCSEByLevel(expressions);
        assertEquals(cseByLevel, ImmutableMap.of(3, ImmutableMap.of(rowExpression("CASE WHEN x = 1 THEN y + z WHEN x = 2 THEN z * 2 END"), rowExpression("\"expr$cse\""))));
    }

    @Test
    void testNoRedundantCSE()
    {
        List<RowExpression> expressions = ImmutableList.of(rowExpression("x * 2 + y + z"), rowExpression("(x * 2 + y + z) * 2"), rowExpression("x * 2"));
        Map<Integer, Map<RowExpression, VariableReferenceExpression>> cseByLevel = collectCSEByLevel(expressions);
        // x * 2 + y is redundant thus should not appear in results
        assertEquals(cseByLevel, ImmutableMap.of(
                3, ImmutableMap.of(rowExpression("\"multiply$cse\" + y + z"), rowExpression("\"add$cse\"")),
                1, ImmutableMap.of(rowExpression("x * 2"), rowExpression("\"multiply$cse\""))));
    }

    @Test
    void testRewriteExpressionWithCSE()
    {
        assertEquals(
                rewriteExpressionWithCSE(
                        rowExpression("(x * y + z) * (y + z) + (x * y)"),
                        ImmutableMap.of(
                                rowExpression("x * y"), variable("multiply$cse"),
                                rowExpression("y + z"), variable("add$cse"),
                                rowExpression("\"multiply$cse\" + z"), variable("add$cse_0"))),
                rowExpression("\"add$cse_0\" * \"add$cse\" + \"multiply$cse\""));
    }

    private VariableReferenceExpression variable(String variable)
    {
        return new VariableReferenceExpression(variable, TYPES.allTypes().get(variable));
    }

    private RowExpression rowExpression(String sql)
    {
        Expression expression = rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql));
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                SESSION,
                METADATA,
                new SqlParser(),
                TYPES,
                expression,
                ImmutableList.of(),
                WarningCollector.NOOP);
        return SqlToRowExpressionTranslator.translate(
                expression,
                expressionTypes,
                ImmutableMap.of(),
                METADATA.getFunctionAndTypeManager(),
                SESSION);
    }
}
