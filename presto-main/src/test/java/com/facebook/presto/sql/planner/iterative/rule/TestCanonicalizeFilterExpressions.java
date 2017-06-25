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

import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;

public class TestCanonicalizeFilterExpressions
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireForExpressionsInCanonicalForm()
    {
        tester().assertThat(new CanonicalizeFilterExpressions())
                .on(p -> p.filter(FALSE_LITERAL, p.values()))
                .doesNotFire();
    }

    @Test
    public void testCanonicalizesExpressions()
    {
        tester().assertThat(new CanonicalizeFilterExpressions())
                .on(p -> p.filter(
                        p.expression("x IS NOT NULL"),
                        p.values(p.symbol("x"))))
                .matches(filter("NOT (x IS NULL)", values("x")));
    }
}
