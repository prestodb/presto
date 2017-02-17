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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.sql.planner.iterative.rule.RemoveFullSample;
import com.facebook.presto.sql.planner.plan.SampleNode.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expressions;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestRemoveFullSample
{
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test
    public void testDoesNotFire()
            throws Exception
    {
        tester.assertThat(new RemoveFullSample())
                .on(p ->
                        p.sample(
                                0.15,
                                Type.BERNOULLI,
                                p.values(p.symbol("a", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void test()
            throws Exception
    {
        tester.assertThat(new RemoveFullSample())
                .on(p ->
                        p.sample(
                                1.0,
                                Type.BERNOULLI,
                                p.filter(
                                        expression("b > 5"),
                                        p.values(
                                                ImmutableList.of(p.symbol("a", BIGINT), p.symbol("b", BIGINT)),
                                                ImmutableList.of(
                                                        expressions("1", "10"),
                                                        expressions("2", "11"))))))
                // TODO: verify contents
                .matches(filter("b > 5", values(ImmutableMap.of("a", 0, "b", 1))));
    }
}
