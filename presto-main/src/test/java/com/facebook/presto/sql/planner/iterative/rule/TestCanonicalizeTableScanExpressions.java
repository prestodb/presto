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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class TestCanonicalizeTableScanExpressions
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireForUnfilteredTableScan()
    {
        tester().assertThat(new CanonicalizeTableScanExpressions())
                .on(p -> p.tableScan(emptyList(), emptyMap()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForFilterInCanonicalForm()
    {
        tester().assertThat(new CanonicalizeTableScanExpressions())
                .on(p -> p.tableScan(emptyList(), emptyMap(), FALSE_LITERAL))
                .doesNotFire();
    }

    @Test
    public void testCanonicalizesFilter()
    {
        tester().assertThat(new CanonicalizeTableScanExpressions())
                .on(p -> p.tableScan(
                        new TableHandle(
                                new ConnectorId("local"),
                                new TpchTableHandle("local", "nation", TINY_SCALE_FACTOR)),
                        ImmutableList.of(p.symbol("nationkey")),
                        ImmutableMap.of(p.symbol("nationkey"), new TpchColumnHandle("nationkey", BIGINT)),
                        p.expression("nationkey IS NOT NULL")))
                .matches(tableScan("nation", "NOT (nationkey IS NULL)"));
    }
}
