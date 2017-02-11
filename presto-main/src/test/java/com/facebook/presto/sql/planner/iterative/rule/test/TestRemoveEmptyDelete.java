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

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.RemoveEmptyDelete;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class TestRemoveEmptyDelete
{
    private final RuleTester tester = new RuleTester();

    @Test
    public void testDoesNotFire()
            throws Exception
    {
        tester.assertThat(new RemoveEmptyDelete())
                .on(p -> p.tableDelete(
                        SchemaTableName.valueOf("sch.tab"),
                        p.tableScan(ImmutableList.of(), ImmutableMap.of()),
                        p.symbol("a", BigintType.BIGINT))
                )
                .doesNotFire();
    }

    @Test
    public void test()
    {
        tester.assertThat(new RemoveEmptyDelete())
                .on(p -> p.tableDelete(
                        SchemaTableName.valueOf("sch.tab"),
                        p.values(),
                        p.symbol("a", BigintType.BIGINT)
                        )
                )
                .matches(
                        PlanMatchPattern.values(ImmutableMap.of("a", 0))
                );
    }
}
