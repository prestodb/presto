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

import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestMergeFilters
        extends BaseRuleTest
{
    private TestingRowExpressionTranslator sqlToRowExpressionTranslator;

    public TestMergeFilters()
    {
    }

    @BeforeClass
    public void setupTranslator()
    {
        this.sqlToRowExpressionTranslator = new TestingRowExpressionTranslator(tester().getMetadata());
    }

    @Test
    public void test()
    {
        tester().assertThat(new MergeFilters(getFunctionManager()))
                .on(p ->
                        p.filter(sqlToRowExpression("b > 44"),
                                p.filter(sqlToRowExpression("a < 42"),
                                        p.values(p.variable("a"), p.variable("b")))))
                .matches(filter("(a < 42) AND (b > 44)", values(ImmutableMap.of("a", 0, "b", 1))));
    }

    private RowExpression sqlToRowExpression(String sql)
    {
        return sqlToRowExpressionTranslator.translate(PlanBuilder.expression(sql), TypeProvider.copyOf(ImmutableMap.of("a", BIGINT, "b", BIGINT)));
    }
}
