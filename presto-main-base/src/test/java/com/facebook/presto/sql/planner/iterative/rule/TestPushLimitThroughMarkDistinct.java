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

import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;

public class TestPushLimitThroughMarkDistinct
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new PushLimitThroughMarkDistinct())
                .on(p ->
                        p.limit(
                                1,
                                p.markDistinct(
                                        p.variable("foo"), ImmutableList.of(p.variable("bar")), p.values())))
                .matches(
                        node(MarkDistinctNode.class,
                                node(LimitNode.class,
                                        node(ValuesNode.class))));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new PushLimitThroughMarkDistinct())
                .on(p ->
                        p.markDistinct(
                                p.variable("foo"),
                                ImmutableList.of(p.variable("bar")),
                                p.limit(
                                        1,
                                        p.values())))
                .doesNotFire();
    }
}
