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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.SubqueryExpression;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;

public class TestExpressionTreeCopier
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @DataProvider(name = "expressions")
    public Object[][] expressions()
    {
        return new Object[][] {
                {"a"},
                {"\"Quoted\""},
                {"t.a"},
                {"1"},
                {"1.5"},
                {"1.5e0"},
                {"'text'"},
                {"true"},
                {"null"},
                {"X'0A1B'"},
                {"DATE '2020-01-01'"},
                {"TIME '01:02:03'"},
                {"TIMESTAMP '2020-01-01 01:02:03'"},
                {"INTERVAL '3' DAY"},
                {"INTERVAL '1-2' YEAR TO MONTH"},
                {"CHAR 'abc'"},
                {"?"},
                {"current_user"},
                {"current_time"},
                {"current_timestamp(3)"},
                {"count(*)"},
                {"sum(a)"},
                {"sum(DISTINCT a)"},
                {"array_agg(a ORDER BY b DESC)"},
                {"count(*) FILTER (WHERE a > 1)"},
                {"row_number() OVER ()"},
                {"row_number() OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"},
                {"grouping(a, t.b)"},
                {"a + b * -c"},
                {"a AND NOT b OR c"},
                {"a = b"},
                {"a BETWEEN 1 AND 2"},
                {"a IS NULL"},
                {"a IS NOT NULL"},
                {"a IN (1, 2, 3)"},
                {"a IN (SELECT x FROM t)"},
                {"a LIKE 'x%' ESCAPE '\\'"},
                {"nullif(a, b)"},
                {"if(a, b, c)"},
                {"if(a, b)"},
                {"coalesce(a, b, c)"},
                {"CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END"},
                {"CASE a WHEN 1 THEN 'one' ELSE 'other' END"},
                {"CASE a WHEN 1 THEN 'one' END"},
                {"CAST(a AS varchar(25))"},
                {"TRY_CAST(a AS bigint)"},
                {"TRY(a / b)"},
                {"ARRAY[1, 2, 3]"},
                {"ARRAY[]"},
                {"a[1]"},
                {"ROW(1, 'a')"},
                {"ROW(1 AS x, 'a' AS y)"},
                {"r.field"},
                {"EXTRACT(YEAR FROM a)"},
                {"a AT TIME ZONE 'UTC'"},
                {"transform(a, x -> x + 1)"},
                {"reduce(a, 0, (s, x) -> s + x, s -> s)"},
                {"\"$internal$bind\"(a, (x, y) -> x + y)"},
                {"EXISTS (SELECT 1 FROM t)"},
                {"(SELECT max(x) FROM t)"},
                {"a > ALL (SELECT x FROM t)"},
                {"a = ANY (SELECT x FROM t)"},
                {"CASE WHEN COALESCE(category, CAST(is_cpu AS varchar)) IS NULL THEN 'total' WHEN is_cpu THEN 'cpu_total' ELSE category END"},
        };
    }

    @Test(dataProvider = "expressions")
    public void testCopyIsEqualAndSharesNoNodes(@Language("SQL") String sql)
    {
        Expression original = SQL_PARSER.createExpression(sql, new ParsingOptions(AS_DECIMAL));
        Expression copy = ExpressionTreeCopier.copy(original);

        assertNotSame(copy, original);
        assertEquals(copy, original);
        assertEquals(copy.hashCode(), original.hashCode());
        assertEquals(copy.toString(), original.toString());

        Set<Node> originalNodes = Collections.newSetFromMap(new IdentityHashMap<>());
        collectNodes(original, originalNodes);
        Set<Node> copiedNodes = Collections.newSetFromMap(new IdentityHashMap<>());
        collectNodes(copy, copiedNodes);
        assertEquals(copiedNodes.size(), originalNodes.size());
        for (Node node : copiedNodes) {
            assertFalse(originalNodes.contains(node), "node is shared between the original and the copy: " + node);
        }
    }

    private static void collectNodes(Node node, Set<Node> nodes)
    {
        nodes.add(node);
        if (node instanceof SubqueryExpression) {
            // the nested query is intentionally shared
            return;
        }
        for (Node child : node.getChildren()) {
            collectNodes(child, nodes);
        }
    }
}
