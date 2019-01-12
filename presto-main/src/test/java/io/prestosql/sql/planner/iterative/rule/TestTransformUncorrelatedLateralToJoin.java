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

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.JoinNode;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static java.util.Collections.emptyList;

public class TestTransformUncorrelatedLateralToJoin
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester()
                .assertThat(new TransformUncorrelatedLateralToJoin())
                .on(p -> p.lateral(emptyList(), p.values(), p.values()))
                .matches(join(JoinNode.Type.INNER, emptyList(), values(), values()));
    }

    @Test
    public void testDoesNotFire()
    {
        Symbol symbol = new Symbol("x");
        tester()
                .assertThat(new TransformUncorrelatedLateralToJoin())
                .on(p -> p.lateral(ImmutableList.of(symbol), p.values(symbol), p.values()))
                .doesNotFire();
    }
}
