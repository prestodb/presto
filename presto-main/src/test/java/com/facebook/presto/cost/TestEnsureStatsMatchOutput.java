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

package com.facebook.presto.cost;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.cost.SymbolStatsAssertion.assertThat;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;

public class TestEnsureStatsMatchOutput
{
    @Test
    public void test()
    {
        Symbol a = new Symbol("a");
        Symbol b = new Symbol("b");
        Symbol c = new Symbol("c");

        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10)
                .addSymbolStatistics(a, SymbolStatsEstimate.builder().setDistinctValuesCount(20).build())
                .addSymbolStatistics(b, SymbolStatsEstimate.builder().setDistinctValuesCount(20).build())
                .build();

        ComposableStatsCalculator.Normalizer normalizer = new EnsureStatsMatchOutput();
        PlanNode node = new ValuesNode(new PlanNodeId(""), ImmutableList.of(a, c), emptyList());
        PlanNodeStatsEstimate normalized = normalizer.normalize(node, estimate, null);

        assertEquals(normalized.getSymbolsWithKnownStatistics(), ImmutableList.of(a, c));
        assertThat(normalized.getSymbolStatistics(a)).distinctValuesCount(20);
        assertThat(normalized.getSymbolStatistics(c)).distinctValuesCountUnknown();
    }
}
