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

package com.facebook.presto.sql.planner.plan.util;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class ValueNodesSymbolsPrunner
{
    private ValueNodesSymbolsPrunner()
    {
    }

    public static PlanNode pruneValuesNode(ValuesNode node, Predicate<Symbol> predicate)
    {
        ImmutableList.Builder<Symbol> rewrittenOutputSymbolsBuilder = ImmutableList.builder();
        ImmutableList.Builder<ImmutableList.Builder<Expression>> rowBuildersBuilder = ImmutableList.builder();
        // Initialize builder for each row
        for (int i = 0; i < node.getRows().size(); i++) {
            rowBuildersBuilder.add(ImmutableList.builder());
        }
        ImmutableList<ImmutableList.Builder<Expression>> rowBuilders = rowBuildersBuilder.build();
        for (int i = 0; i < node.getOutputSymbols().size(); i++) {
            Symbol outputSymbol = node.getOutputSymbols().get(i);
            // If output symbol is used
            if (predicate.test(outputSymbol)) {
                rewrittenOutputSymbolsBuilder.add(outputSymbol);
                // Add the value of the output symbol for each row
                for (int j = 0; j < node.getRows().size(); j++) {
                    rowBuilders.get(j).add(node.getRows().get(j).get(i));
                }
            }
        }
        List<List<Expression>> rewrittenRows = rowBuilders.stream()
                .map((rowBuilder) -> rowBuilder.build())
                .collect(toImmutableList());
        return new ValuesNode(node.getId(), rewrittenOutputSymbolsBuilder.build(), rewrittenRows);
    }
}
