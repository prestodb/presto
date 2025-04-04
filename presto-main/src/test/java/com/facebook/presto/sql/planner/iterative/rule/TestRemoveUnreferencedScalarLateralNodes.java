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

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static java.util.Collections.emptyList;

public class TestRemoveUnreferencedScalarLateralNodes
        extends BaseRuleTest
{
    @Test
    public void testRemoveUnreferencedInput()
    {
        tester().assertThat(new RemoveUnreferencedScalarLateralNodes())
                .on(p -> p.lateral(
                        emptyList(),
                        p.values(p.variable("x", BigintType.BIGINT)),
                        p.values(emptyList(), ImmutableList.of(emptyList()))))
                .matches(values("x"));
    }

    @Test
    public void testRemoveUnreferencedSubquery()
    {
        tester().assertThat(new RemoveUnreferencedScalarLateralNodes())
                .on(p -> p.lateral(
                        emptyList(),
                        p.values(emptyList(), ImmutableList.of(emptyList())),
                        p.values(p.variable("x", BigintType.BIGINT))))
                .matches(values("x"));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveUnreferencedScalarLateralNodes())
                .on(p -> p.lateral(
                        emptyList(),
                        p.values(),
                        p.values()))
                .doesNotFire();
    }
}
