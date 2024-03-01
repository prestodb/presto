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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SetOperationNodeUtils
{
    private SetOperationNodeUtils() {}

    public static Map<VariableReferenceExpression, List<VariableReferenceExpression>> fromListMultimap(ListMultimap<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs)
    {
        Map<VariableReferenceExpression, List<VariableReferenceExpression>> mapping = new LinkedHashMap<>();
        for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : outputsToInputs.entries()) {
            if (!mapping.containsKey(entry.getKey())) {
                List<VariableReferenceExpression> values = new ArrayList<>();
                values.add(entry.getValue());
                mapping.put(entry.getKey(), values);
            }
            else {
                mapping.get(entry.getKey()).add(entry.getValue());
            }
        }

        return mapping;
    }

    /**
     * Returns the input to output symbol mapping for the given source channel.
     * A single input symbol can map to multiple output symbols, thus requiring a Multimap.
     */
    public static Multimap<VariableReferenceExpression, VariableReferenceExpression> outputMap(UnionNode node, int sourceIndex)
    {
        return FluentIterable.from(node.getOutputVariables())
                .toMap(output -> node.getVariableMapping().get(output).get(sourceIndex))
                .asMultimap()
                .inverse();
    }
}
