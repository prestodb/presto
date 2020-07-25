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
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;

public class TestRemoveTrivialFilters
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveTrivialFilters())
                .on(p -> p.filter(p.expression("1 = 1"), p.values()))
                .doesNotFire();
    }

    @Test
    public void testRemovesTrueFilter()
    {
        tester().assertThat(new RemoveTrivialFilters())
                .on(p -> p.filter(p.expression("TRUE"), p.values()))
                .matches(values());
    }

    @Test
    public void testRemovesFalseFilter()
    {
        tester().assertThat(new RemoveTrivialFilters())
                .on(p -> p.filter(
                        p.expression("FALSE"),
                        p.values(
                                ImmutableList.of(p.variable("a")),
                                ImmutableList.of(constantExpressions(BIGINT, 1L)))))
                .matches(values("a"));
    }
}
