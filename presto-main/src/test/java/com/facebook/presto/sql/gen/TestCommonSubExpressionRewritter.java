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
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.CommonSubExpressionRewriter.SubExpressionInfo;
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
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.collectCSEByLevel;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.getExpressionsWithCSE;
import static org.testng.Assert.assertEquals;

public class TestCommonSubExpressionRewritter
{
    private static final Session SESSION = TEST_SESSION;
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final TypeProvider TYPES = TypeProvider.viewOf(ImmutableMap.of("x", BIGINT, "y", BIGINT, "z", BIGINT, "add$cse", BIGINT, "multiply$cse", BIGINT));

    @Test
    void testGetExpressionsWithCSE()
    {
        List<RowExpression> expressions = ImmutableList.of(rowExpression("x + y"), rowExpression("(x + y) * 2"), rowExpression("x + 2"));
        List<RowExpression> expressionsWithCSE = getExpressionsWithCSE(expressions);
        assertEquals(expressionsWithCSE, ImmutableList.of(rowExpression("x + y"), rowExpression("(x + y) * 2")));
    }

    @Test
    void testCseDedupe()
    {
        List<RowExpression> expressions = ImmutableList.of(rowExpression("x * 2 + y + z"), rowExpression("(x * 2 + y + z) * 2"), rowExpression("x * 2"));
        Map<Integer, Map<RowExpression, CommonSubExpressionRewriter.SubExpressionInfo>> cseByLevel = collectCSEByLevel(expressions);
        // x * 2 + y is redundant thus should not appear in results
        assertEquals(cseByLevel, ImmutableMap.of(
                3, ImmutableMap.of(rowExpression("\"multiply$cse\" + y + z"), subExpressionInfo("\"add$cse\"", 2)),
                1, ImmutableMap.of(rowExpression("x * 2"), subExpressionInfo("\"multiply$cse\"", 2))));

        expressions = ImmutableList.of(rowExpression("x * 2 + y + z"), rowExpression("(x * 2 + z + 1) * 2"), rowExpression("(x * 2)  + (x * 2 + y + z)"));
        cseByLevel = collectCSEByLevel(expressions);
        // x * 2 + y is redundant thus should not appear in results
        assertEquals(cseByLevel, ImmutableMap.of(
                3, ImmutableMap.of(rowExpression("\"multiply$cse\" + y + z"), subExpressionInfo("\"add$cse\"", 2)),
                1, ImmutableMap.of(rowExpression("x * 2"), subExpressionInfo("\"multiply$cse\"", 3))));
    }

    private SubExpressionInfo subExpressionInfo(String variable, int occurence)
    {
        return new SubExpressionInfo((VariableReferenceExpression) rowExpression(variable), occurence);
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
                METADATA.getFunctionManager(),
                METADATA.getTypeManager(),
                SESSION);
    }
}
