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

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.iterative.rule.test.RuleTester.CONNECTOR_ID;

public class TestRemoveEmptyDelete
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveEmptyDelete())
                .on(p -> p.tableDelete(
                        new SchemaTableName("sch", "tab"),
                        p.tableScan(
                                new TableHandle(
                                        CONNECTOR_ID,
                                        new TpchTableHandle("nation", 1.0),
                                        TestingTransactionHandle.create(),
                                        Optional.empty()),
                                ImmutableList.of(),
                                ImmutableMap.of()),
                        p.variable("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void test()
    {
        tester().assertThat(new RemoveEmptyDelete())
                .on(p -> p.tableDelete(
                        new SchemaTableName("sch", "tab"),
                        p.values(),
                        p.variable("a", BIGINT)))
                .matches(
                        PlanMatchPattern.values(ImmutableMap.of("a", 0)));
    }
}
