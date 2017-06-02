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

import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableWriter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.union;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushTableWriteThroughUnion
{
    @Test
    public void testPushThroughUnion()
    {
        new RuleTester().assertThat(new PushTableWriteThroughUnion())
                .on(p ->
                        p.tableWriter(
                                p.union(ImmutableList.of(
                                        p.values(p.symbol("A", BIGINT), p.symbol("B", BIGINT)),
                                        p.values(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                        ImmutableListMultimap.of(
                                                p.symbol("A2", BIGINT), p.symbol("A", BIGINT),
                                                p.symbol("A2", BIGINT), p.symbol("B1", BIGINT),
                                                p.symbol("B2", BIGINT), p.symbol("B", BIGINT),
                                                p.symbol("B2", BIGINT), p.symbol("A1", BIGINT))),
                                ImmutableList.of(p.symbol("A2", BIGINT), p.symbol("B2", BIGINT)),
                                ImmutableList.of("a", "b")))
                .matches(union(
                        tableWriter(
                                values(ImmutableMap.of("A", 0, "B", 1)),
                                ImmutableList.of("A", "B"),
                                ImmutableList.of("a", "b")
                        ),
                        tableWriter(
                                values(ImmutableMap.of("A1", 0, "B1", 1)),
                                ImmutableList.of("B1", "A1"),
                                ImmutableList.of("a", "b")
                        )));
    }
}
