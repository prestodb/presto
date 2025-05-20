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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantJoin;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.plan.JoinType.FULL;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestRemoveRedundantJoin
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(10, p.variable("a")),
                                p.values(10, p.variable("b"))))
                .doesNotFire();

        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(10, p.variable("a")),
                                p.values(10, p.variable("b"))))
                .doesNotFire();
    }

    @Test
    public void testInnerJoinRemoval()
    {
        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(10, p.variable("a")),
                                p.values(0)))
                .matches(values("a"));

        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(0),
                                p.values(10, p.variable("b"))))
                .matches(values("b"));
    }

    @Test
    public void testLeftJoinRemoval()
    {
        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(0),
                                p.values(10, p.variable("b"))))
                .matches(values("a"));
    }

    @Test
    public void testRightJoinRemoval()
    {
        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(10, p.variable("a")),
                                p.values(0)))
                .matches(values("a"));
    }

    @Test
    public void testFullJoinRemoval()
    {
        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(0, p.variable("a")),
                                p.values(0, p.variable("b"))))
                .matches(values("a", "b"));
    }
}
