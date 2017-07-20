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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestCanonicalizeExpressionRewriter
{
    private static final CanonicalizeExpressionRewriter rewriter = new CanonicalizeExpressionRewriter();

    @Test
    public void testRewriteIsNotNullPredicate()
    {
        assertRewritten("x is NOT NULL", "NOT (x IS NULL)");
    }

    @Test
    public void testRewriteIfExpression()
    {
        assertRewritten("IF(x = 0, 0, 1)", "CASE WHEN x = 0 THEN 0 ELSE 1 END");
    }

    @Test
    public void testRewriteCurrentTime()
    {
        assertRewritten("CURRENT_TIME", "\"current_time\"()");
    }

    @Test
    public void testRewriteYearExtract()
            throws Exception
    {
        assertRewritten("EXTRACT(YEAR FROM '2017-07-20')", "year('2017-07-20')");
    }

    private static void assertRewritten(String from, String to)
    {
        assertEquals(rewrite(PlanBuilder.expression(from)), PlanBuilder.expression(to));
    }

    private static Expression rewrite(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(rewriter, expression);
    }
}
