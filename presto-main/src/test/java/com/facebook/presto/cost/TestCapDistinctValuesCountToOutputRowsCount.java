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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.cost.SymbolStatsAssertion.assertThat;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class TestCapDistinctValuesCountToOutputRowsCount
{
    private static final ValuesNode NODE = new ValuesNode(new PlanNodeId("1"), emptyList(), emptyList());
    private static final Map<Symbol, Type> TYPES = emptyMap();
    private static final Symbol A = new Symbol("a");
    private static final Symbol B = new Symbol("b");
    private static final Symbol C = new Symbol("c");

    @Test
    public void tesOutputRowCountIsKnown()
    {
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10)
                .addSymbolStatistics(A, SymbolStatsEstimate.builder().setDistinctValuesCount(20).build())
                .addSymbolStatistics(B, SymbolStatsEstimate.builder().setDistinctValuesCount(5).build())
                .addSymbolStatistics(C, SymbolStatsEstimate.builder().build())
                .build();

        assertThat(normalize(estimate).getSymbolStatistics(A)).distinctValuesCount(10);
        assertThat(normalize(estimate).getSymbolStatistics(B)).distinctValuesCount(5);
        assertThat(normalize(estimate).getSymbolStatistics(C)).distinctValuesCountUnknown();
    }

    @Test
    public void testOutputRowCountIsNotKnown()
    {
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(A, SymbolStatsEstimate.builder().setDistinctValuesCount(20).build())
                .build();

        assertThat(normalize(estimate).getSymbolStatistics(A)).distinctValuesCount(20);
    }

    private PlanNodeStatsEstimate normalize(PlanNodeStatsEstimate estimate)
    {
        CapDistinctValuesCountToOutputRowsCount normalizer = new CapDistinctValuesCountToOutputRowsCount();
        return normalizer.normalize(NODE, estimate, TYPES);
    }
}
