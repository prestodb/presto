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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableWriter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.union;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushTableWriteThroughUnion
        extends BaseRuleTest
{
    @Test
    public void testPushThroughUnion()
    {
        tester().assertThat(new PushTableWriteThroughUnion())
                .on(p ->
                        p.tableWriter(
                                ImmutableList.of(p.symbol("A", BIGINT), p.symbol("B", BIGINT)), ImmutableList.of("a", "b"),
                                p.union(
                                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                                .putAll(p.symbol("A", BIGINT), p.symbol("A1", BIGINT), p.symbol("B2", BIGINT))
                                                .putAll(p.symbol("B", BIGINT), p.symbol("B1", BIGINT), p.symbol("A2", BIGINT))
                                                .build(),
                                        ImmutableList.of(
                                                p.values(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                                p.values(p.symbol("A2", BIGINT), p.symbol("B2", BIGINT))))))
                .matches(union(
                        tableWriter(ImmutableList.of("A1", "B1"), ImmutableList.of("a", "b"), values(ImmutableMap.of("A1", 0, "B1", 1))),
                        tableWriter(ImmutableList.of("B2", "A2"), ImmutableList.of("a", "b"), values(ImmutableMap.of("A2", 0, "B2", 1)))));
    }
}
