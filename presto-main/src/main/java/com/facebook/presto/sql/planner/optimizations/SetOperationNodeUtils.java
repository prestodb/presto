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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.SetOperationNode;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class SetOperationNodeUtils
{
    private SetOperationNodeUtils() {}

    public static List<VariableReferenceExpression> sourceOutputLayout(SetOperationNode node, int sourceIndex)
    {
        // Make sure the sourceOutputSymbolLayout symbols are listed in the same order as the corresponding output symbols
        return unmodifiableList(node.getOutputVariables().stream()
                .map(variable -> node.getVariableMapping().get(variable).get(sourceIndex))
                .collect(toList()));
    }

    /**
     * Returns the output to input variable mapping for the given source channel
     */
    public static Map<VariableReferenceExpression, VariableReferenceExpression> sourceVariableMap(SetOperationNode node, int sourceIndex)
    {
        return unmodifiableMap(node.getVariableMapping().entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().get(sourceIndex))));
    }

    /**
     * Returns the input to output symbol mapping for the given source channel.
     * A single input symbol can map to multiple output symbols, thus requiring a Multimap.
     */
    public static Multimap<VariableReferenceExpression, VariableReferenceExpression> outputMap(SetOperationNode node, int sourceIndex)
    {
        return FluentIterable.from(node.getOutputVariables())
                .toMap(output -> node.getVariableMapping().get(output).get(sourceIndex))
                .asMultimap()
                .inverse();
    }
}
