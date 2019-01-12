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
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.PlanNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneSemiJoinFilteringSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllColumnsReferenced()
    {
        tester().assertThat(new PruneSemiJoinFilteringSourceColumns())
                .on(p -> buildSemiJoin(p, symbol -> true))
                .matches(
                        semiJoin("leftKey", "rightKey", "match",
                                values("leftKey"),
                                strictProject(
                                        ImmutableMap.of(
                                                "rightKey", expression("rightKey"),
                                                "rightKeyHash", expression("rightKeyHash")),
                                        values("rightKey", "rightKeyHash", "rightValue"))));
    }

    @Test
    public void testAllColumnsNeeded()
    {
        tester().assertThat(new PruneSemiJoinFilteringSourceColumns())
                .on(p -> buildSemiJoin(p, symbol -> !symbol.getName().equals("rightValue")))
                .doesNotFire();
    }

    private static PlanNode buildSemiJoin(PlanBuilder p, Predicate<Symbol> filteringSourceSymbolFilter)
    {
        Symbol match = p.symbol("match");
        Symbol leftKey = p.symbol("leftKey");
        Symbol rightKey = p.symbol("rightKey");
        Symbol rightKeyHash = p.symbol("rightKeyHash");
        Symbol rightValue = p.symbol("rightValue");
        List<Symbol> filteringSourceSymbols = ImmutableList.of(rightKey, rightKeyHash, rightValue);
        return p.semiJoin(
                leftKey,
                rightKey,
                match,
                Optional.empty(),
                Optional.of(rightKeyHash),
                p.values(leftKey),
                p.values(
                        filteringSourceSymbols.stream()
                                .filter(filteringSourceSymbolFilter)
                                .collect(toImmutableList()),
                        ImmutableList.of()));
    }
}
