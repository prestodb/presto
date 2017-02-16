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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

public class TestExpressionVerifier
{
    private final SqlParser parser = new SqlParser();

    @Test
    public void test()
    {
        Expression actual = expression("NOT(orderkey = 3 AND custkey = 3 AND orderkey < 10)");

        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("X", new SymbolReference("orderkey"))
                .put("Y", new SymbolReference("custkey"))
                .build();

        NodeVerifier verifier = new NodeVerifier(symbolAliases);

        assertTrue(verifier.process(actual, expression("NOT(X = 3 AND Y = 3 AND X < 10)")));
        assertThrows(() -> verifier.process(actual, expression("NOT(X = 3 AND Y = 3 AND Z < 10)")));
        assertFalse(verifier.process(actual, expression("NOT(X = 3 AND X = 3 AND X < 10)")));
    }

    @Test
    public void testCast()
            throws Exception
    {
        SymbolAliases aliases = SymbolAliases.builder()
                .put("X", new SymbolReference("orderkey"))
                .build();

        NodeVerifier verifier = new NodeVerifier(aliases);
        assertTrue(verifier.process(expression("CAST('2' AS varchar)"), expression("CAST('2' AS varchar)")));
        assertFalse(verifier.process(expression("CAST('2' AS varchar)"), expression("CAST('2' AS bigint)")));
        assertTrue(verifier.process(expression("CAST(orderkey AS varchar)"), expression("CAST(X AS varchar)")));
    }

    private Expression expression(String sql)
    {
        return rewriteIdentifiersToSymbolReferences(parser.createExpression(sql));
    }

    private static void assertThrows(Runnable runnable)
    {
        try {
            runnable.run();
            throw new AssertionError("Method didn't throw exception as expected");
        }
        catch (Exception expected) {
        }
    }
}
