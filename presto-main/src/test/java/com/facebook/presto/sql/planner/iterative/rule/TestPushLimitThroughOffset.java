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
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.OFFSET_CLAUSE_ENABLED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.limit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.offset;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushLimitThroughOffset
        extends BaseRuleTest
{
    @Test
    public void testPushdownLimitThroughOffset()
    {
        tester().assertThat(new PushLimitThroughOffset())
                .setSystemProperty(OFFSET_CLAUSE_ENABLED, "true")
                .on(p -> p.limit(
                        2,
                        p.offset(5, p.values())))
                .matches(
                        offset(
                                5,
                                limit(7, values())));
    }

    @Test
    public void doNotPushdownWhenRowCountOverflowsLong()
    {
        tester().assertThat(new PushLimitThroughOffset())
                .setSystemProperty(OFFSET_CLAUSE_ENABLED, "true")
                .on(p -> {
                    return p.limit(
                            Long.MAX_VALUE,
                            p.offset(Long.MAX_VALUE, p.values()));
                })
                .doesNotFire();
    }
}
