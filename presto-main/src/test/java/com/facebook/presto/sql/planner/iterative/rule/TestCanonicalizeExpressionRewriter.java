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
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static org.testng.Assert.assertEquals;

public class TestCanonicalizeExpressionRewriter
{
    @Test
    public void testRewriteIsNotNullPredicate()
    {
        assertRewritten("x is NOT NULL", "NOT (x IS NULL)");
    }

    @Test
    public void testRewriteCurrentTime()
    {
        assertRewritten("CURRENT_TIME", "\"current_time\"()");
    }

    @Test
    public void testRewriteYearExtract()
    {
        assertRewritten("EXTRACT(YEAR FROM '2017-07-20')", "year('2017-07-20')");
    }

    private static void assertRewritten(String from, String to)
    {
        assertEquals(canonicalizeExpression(PlanBuilder.expression(from)), PlanBuilder.expression(to));
    }
}
