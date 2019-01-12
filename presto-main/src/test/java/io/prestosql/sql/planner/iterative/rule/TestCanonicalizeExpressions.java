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
package io.prestosql.sql.planner.iterative.rule;

import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.tree.BooleanLiteral.FALSE_LITERAL;

public class TestCanonicalizeExpressions
        extends BaseRuleTest
{
    private static final CanonicalizeExpressions canonicalizeExpressions = new CanonicalizeExpressions();

    @Test
    public void testDoesNotFireForExpressionsInCanonicalForm()
    {
        tester().assertThat(canonicalizeExpressions.filterExpressionRewrite())
                .on(p -> p.filter(FALSE_LITERAL, p.values()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForUnfilteredJoin()
    {
        tester().assertThat(canonicalizeExpressions.joinExpressionRewrite())
                .on(p -> p.join(INNER, p.values(), p.values()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForCanonicalExpressions()
    {
        tester().assertThat(canonicalizeExpressions.joinExpressionRewrite())
                .on(p -> p.join(INNER, p.values(), p.values(), FALSE_LITERAL))
                .doesNotFire();
    }
}
