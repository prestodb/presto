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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.plan.TableScanNode.GeneratedPartitions;
import static com.google.common.base.Predicates.in;
import static org.testng.Assert.assertEquals;

public class TestSymbolExtractor
{
    @SuppressWarnings("resource")
    private final SymbolAllocator symbolAllocator = new SymbolAllocator();

    @Test
    public void testSampleSymbols()
    {
        PlanNode tableScanNode = createTableScanNode("test");
        SampleNode sampleNode = createSampleNode("sample", tableScanNode);
        PlanNode root = createOutputNode("outout", sampleNode);

        Set<Symbol> dependencies = SymbolExtractor.extract(root);
        Map<Symbol, Type> filteredSymbols = Maps.filterKeys(symbolAllocator.getTypes(), in(dependencies));

        assertEquals(filteredSymbols.get(new Symbol("$sampleweight")), BIGINT);
        assertEquals(filteredSymbols.get(new Symbol("column")), VARCHAR);
    }

    private PlanNode createOutputNode(String planId, SampleNode source)
    {
        PlanNodeId outputNodeId = new PlanNodeId(planId);

        ImmutableList.Builder<String> names = ImmutableList.builder();
        names.add("column");

        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        outputs.add(new Symbol("column"));

        return new OutputNode(outputNodeId, source, names.build(), outputs.build());
    }

    private SampleNode createSampleNode(String planId, PlanNode source)
    {
        PlanNodeId sampleNodeId = new PlanNodeId(planId);

        double ratio = 0.5;
        Symbol sampleWeightSymbol = symbolAllocator.newSymbol("$sampleweight", BIGINT);

        return new SampleNode(sampleNodeId, source, ratio, SampleNode.Type.POISSONIZED, true, Optional.fromNullable(sampleWeightSymbol));
    }

    private PlanNode createTableScanNode(String planId)
    {
        Symbol symbol = symbolAllocator.newSymbol("column", VARCHAR);

        PlanNodeId tableScanNodeId = new PlanNodeId(planId);
        PlanNode planNode = new TableScanNode(
                tableScanNodeId,
                new TableHandle("test", new TestingTableHandle()),
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new ColumnHandle("test", new TestingColumnHandle("column"))),
                null,
                Optional.<GeneratedPartitions>absent());
        return planNode;
    }
}
