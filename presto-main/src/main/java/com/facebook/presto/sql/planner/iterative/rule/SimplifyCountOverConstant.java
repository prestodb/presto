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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public class SimplifyCountOverConstant
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof AggregationNode)) {
            return Optional.empty();
        }

        AggregationNode parent = (AggregationNode) node;

        PlanNode input = lookup.resolve(parent.getSource());
        if (!(input instanceof ProjectNode)) {
            return Optional.empty();
        }

        ProjectNode child = (ProjectNode) input;

        boolean changed = false;
        Map<Symbol, AggregationNode.Aggregation> assignments = new LinkedHashMap<>(parent.getAssignments());

        for (Entry<Symbol, AggregationNode.Aggregation> entry : parent.getAssignments().entrySet()) {
            Symbol symbol = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();

            if (isCountOverConstant(aggregation, child.getAssignments())) {
                changed = true;
                assignments.put(symbol, new AggregationNode.Aggregation(
                        new FunctionCall(QualifiedName.of("count"), ImmutableList.of()),
                        new Signature("count", AGGREGATE, parseTypeSignature(StandardTypes.BIGINT))));
            }
        }

        if (!changed) {
            return Optional.empty();
        }

        return Optional.of(new AggregationNode(
                node.getId(),
                child,
                assignments,
                parent.getGroupingSets(),
                parent.getStep(),
                parent.getHashSymbol(),
                parent.getGroupIdSymbol()));
    }

    private static boolean isCountOverConstant(AggregationNode.Aggregation aggregation, Assignments inputs)
    {
        Signature signature = aggregation.getSignature();
        if (!signature.getName().equals("count") || signature.getArgumentTypes().size() != 1) {
            return false;
        }

        Expression argument = aggregation.getCall().getArguments().get(0);
        if (argument instanceof SymbolReference) {
            argument = inputs.get(Symbol.from(argument));
        }

        return argument instanceof Literal && !(argument instanceof NullLiteral);
    }
}
